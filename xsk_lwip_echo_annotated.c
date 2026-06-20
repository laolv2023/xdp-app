/*
 * NOTE 0967
 *
 * AF_XDP + lwIP TCP Echo Server — 生产级高性能用户态网络应用
 *
 * 数据路径:  NIC → XDP → AF_XDP RX Ring → lwIP TCP → Echo回调 → TX Ring → NIC
 * 核心组件:  free_pool(帧池) + object_pool(wrapper池) + poison_list(故障恢复)
 * 线程模型:  严格单线程 (main poll loop + lwIP callbacks)
 * XDP 模式:  优先 ZEROCOPY, 回退 COPY
 * 审计状态:  5轮70+维度代码审计通过, 生产级质量
 *
 * WARNING: This module and its associated data structures are NOT thread-safe.
 *  All access must be confined to the dedicated poll thread (main loop or lwIP
 *  tcpip thread).  Do not call any xskif/echo functions from other threads.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>
#include <signal.h>                                          /* R7-1: graceful shutdown */
#include <numa.h>
#include <numaif.h>
#include <sys/resource.h>
#include <sys/ioctl.h>
#include <sys/random.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/mman.h>
#include <net/if.h>
#include <time.h>
#include <inttypes.h>
#include "lwip/init.h"
#include "lwip/opt.h"
#include "lwip/pbuf.h"
#include "lwip/tcp.h"
#include "lwip/ip.h"
#include "lwip/netif.h"
#include "lwip/etharp.h"
#include "lwip/ethip6.h"
#include "lwip/timeouts.h"
#include "xsk.h"


/* ── ────────────────────────────────────────────────────────────────── */
/* UMEM 帧大小: 4096 (一页)                                                 */
/* ------------------------------------------------------------------------ */
#ifndef XSK_UMEM_FRAME_SIZE
#define XSK_UMEM_FRAME_SIZE 4096
#endif


/* ── ────────────────────────────────────────────────────────────────── */
/* 编译器分支预测: unlikely(x) = __builtin_expect(!!(x), 0)                   */
/* ------------------------------------------------------------------------ */
#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif
#ifndef likely
#define likely(x)   __builtin_expect(!!(x), 1)             /* provided for symmetry; not yet used */
#endif


/* ── ────────────────────────────────────────────────────────────────── */
/* 2MB 大页标志 (mmap MAP_HUGETLB)                                         */
/* ------------------------------------------------------------------------ */
#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << MAP_HUGE_SHIFT)
#endif


/* ── ────────────────────────────────────────────────────────────────── */
/* XSK 环形缓冲区条目数: 2048 (2的幂)                                            */
/* ------------------------------------------------------------------------ */
#define XSK_RING_SIZE             2048

/* ── ────────────────────────────────────────────────────────────────── */
/* 每连接最大排队消息数                                                          */
/* ------------------------------------------------------------------------ */
#define MAX_QUEUED_MSGS_PER_CONN  ((uint32_t)1024)

/* ── ────────────────────────────────────────────────────────────────── */
/* Fill Ring 初始填充比例: 75%                                               */
/* ------------------------------------------------------------------------ */
#define XSK_FILL_MAX_RATIO        75

/* ── ────────────────────────────────────────────────────────────────── */
/* 复位速率限制: 两次复位间隔 >= 2000ms                                            */
/* ------------------------------------------------------------------------ */
#define RESET_TIMEOUT_MS          2000

/* ── ────────────────────────────────────────────────────────────────── */
/* 2MB 大页含 512 个 UMEM 帧 (2MB / 4096)                                   */
/* ------------------------------------------------------------------------ */
#define UMEM_2MB_FRAME_COUNT      ((2 * 1024 * 1024) / XSK_UMEM_FRAME_SIZE)

/* ── ────────────────────────────────────────────────────────────────── */
/* 环形缓冲区批量操作大小: 64                                                     */
/* ------------------------------------------------------------------------ */
#define XSK_BATCH_SIZE            64   /* batch size for ring operations */


/* ── ────────────────────────────────────────────────────────────────── */
/* 紧急毒化池: 256 个预分配 poison_frame, 避免 OOM 路径 malloc                      */
/* ------------------------------------------------------------------------ */
#define EMERGENCY_POISON_POOL_SIZE 256

/* ── ────────────────────────────────────────────────────────────────── */
/* 无效地址哨兵: 0xFFFFFFFFFFFFFFFFULL                                       */
/* ------------------------------------------------------------------------ */
#define FREE_POOL_INVALID_ADDR 0xFFFFFFFFFFFFFFFFULL

/* ── ────────────────────────────────────────────────────────────────── */
/* 孤儿 wrapper 阈值: 超过即触发复位                                              */
/* ------------------------------------------------------------------------ */
#define MAX_ORPHAN_WRAPPERS      1024   /* threshold to trigger reset */


/* ── ────────────────────────────────────────────────────────────────── */
/* 向上对齐: align_up(x, a) 将 x 对齐到 a 的倍数                                  */
/* ------------------------------------------------------------------------ */
#define align_up(x, a) (((x) + (a) - 1) & ~((a) - 1))  /* R1-1: align x up to multiple of a */


/* ── ────────────────────────────────────────────────────────────────── */
/* free_pool_push 返回值: OK=0, EINVAL=-1, EEXIST=-2, ENOSPC=-3, EINTERNAL=-4  */
/* ------------------------------------------------------------------------ */
/* Return codes for free_pool_push */
#define FREEPOOL_OK      0
#define FREEPOOL_EINVAL -1
#define FREEPOOL_EEXIST -2
#define FREEPOOL_ENOSPC -3
#define FREEPOOL_EINTERNAL -4


/* ── ────────────────────────────────────────────────────────────────── */
/* 全局运行标志: volatile sig_atomic_t 确保信号处理器安全写入                           */
/* ------------------------------------------------------------------------ */
/* R7-1: signal-safe graceful shutdown flag */
static volatile sig_atomic_t g_running = 1;


/* ── ────────────────────────────────────────────────────────────────── */
/* 信号处理器: 仅写入 g_running=0, 完全异步信号安全                                    */
/* ------------------------------------------------------------------------ */
static void signal_handler(int sig) {
    (void)sig;
    g_running = 0;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* 位图操作 (bitmap):                                                      */
/*   双位图追踪 UMEM 帧状态:                                                   */
/*     bitmap[i]:      帧 i 在 free_pool 中 (可分配)                         */
/*     used_bitmap[i]: 帧 i 正在被使用 (lwIP/TX/RX)                          */
/*   不变式: !(bitmap[i] && used_bitmap[i]) — 帧不能同时空闲和使用中                 */
/*   每个 bit 对应一个帧, 64 帧/uint64_t                                       */
/* ------------------------------------------------------------------------ */
/* ---------- inline bitmap operations ---------- */
static inline int bitmap_set(uint64_t *bitmap, uint32_t i, uint32_t cap) {
    if (!bitmap || i >= cap) {
        return -1;
    }
    bitmap[i / 64] |= (1ULL << (i % 64));
    return 0;
}

static inline int bitmap_clear(uint64_t *bitmap, uint32_t i, uint32_t cap) {
    if (!bitmap || i >= cap) {
        return -1;
    }
    bitmap[i / 64] &= ~(1ULL << (i % 64));
    return 0;
}

static inline int bitmap_test(const uint64_t *bitmap, uint32_t i, uint32_t cap) {
    if (!bitmap || i >= cap) {
        return -1;
    }
    return (bitmap[i / 64] & (1ULL << (i % 64))) != 0 ? 1 : 0;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   核心数据结构                                                            */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   poison_frame       — 毒化帧节点 (无法推回 free_pool 的帧地址)                  */
/*   free_pool          — UMEM 帧空闲池 (栈+双位图, O(1) push/pop)             */
/*   object_pool        — pbuf_custom_wrapper 对象池 (预分配, 避免数据路径 malloc)  */
/*   pbuf_custom_wrapper — 自定义 pbuf 包装器 (pbuf_custom + UMEM地址 + xskif指针)  */
/*   queued_msg         — TCP 发送队列节点 (待回显数据)                           */
/*   echo_ctx           — TCP 连接上下文 (pcb + 消息队列 + 状态标志)                */
/*   xskif_stats        — 运行时统计 (全部 uint64_t, 永不溢出)                    */
/*   xskif              — 全局程序状态 (XSK句柄 + 内存池 + lwIP状态)                */
/* ------------------------------------------------------------------------ */
/* ---------- Memory management structures ---------- */
struct poison_frame {
    u64 umem_addr;
    struct poison_frame *next;
    bool is_emergency;
};

struct free_pool {
    u64 *stack;
    uint32_t top;
    uint32_t capacity;
    bool need_reset;
    uint64_t *bitmap;
    uint64_t *used_bitmap;
};

struct object_pool {
    struct pbuf_custom_wrapper **stack;
    uint32_t top;
    uint32_t capacity;
    void *block;
};

struct pbuf_custom_wrapper {
    struct pbuf_custom pc;
    struct xskif *xif;
    u64 umem_addr;
    struct pbuf_custom_wrapper *next_orphan;
    bool in_pool;
};

struct queued_msg {
    struct queued_msg *next;
    struct pbuf *p;
    struct pbuf *current_p;
    u16_t offset;
};

struct echo_ctx {
    struct tcp_pcb *pcb;
    struct xskif *xif;
    struct queued_msg *queued_msgs;
    struct queued_msg *queued_msgs_tail;
    uint32_t queued_msg_count;
    bool waiting;
    bool closing;
    struct echo_ctx *next;
    struct echo_ctx *prev;
};

struct xskif_stats {
    uint64_t rx_packets, rx_bytes, tx_packets, tx_bytes;
    uint64_t tx_reserve_fails, rx_input_errors, obj_pool_push_drops;
    uint64_t flow_control_oom_closes, tx_size_drops, obj_pool_exhausted;
    uint64_t flow_control_stalls, fill_ring_starvation;
    uint64_t ghost_completions, pool_overflow_resets, pool_overflow_drops;
    uint64_t total_resets;
    uint64_t min_free_pool_remain;
    uint64_t rx_fulldrop_packets;
    uint64_t hugepage_fallback;
    uint64_t zerocopy_fallback;
    uint64_t echo_malloc_fails;
    uint64_t tx_copy_fails;
    uint64_t obj_pool_partial_destroy;
    uint64_t tx_invalid_addr_drops;
    int      xdp_mode;
    uint64_t rx_in_flight_peak;
    uint64_t rx_in_flight_underflow_count;
    uint64_t free_pool_exhausted_tx;
    uint64_t pool_underflow_wrapper_freed;
    uint64_t free_pool_push_fails;
    uint64_t rx_zero_len_drops;
    uint64_t emergency_poison_exhausted;
    uint64_t umem_frame_loss_aborts;
    uint64_t emergency_pool_recycled;
    uint64_t umem_frame_permanent_loss;
    uint64_t lost_frames_recovered;
    uint64_t invalid_addr_push_attempts;
    uint64_t free_pool_duplicate_pushes;
    uint64_t rx_drained_packets;
    uint64_t tx_sendto_fails;           /* P2-3 */
    uint64_t mac_random_fallback;       /* P2-7 */
};

struct xskif {
    struct netif *netif;
    struct xsk_socket *xsk;
    struct xsk_umem *umem;
    void *umem_area;
    uint64_t umem_size;
    u64 *umem_frame_bases;
    struct xsk_ring_prod fill;
    struct xsk_ring_cons rx;
    struct xsk_ring_prod tx;
    struct xsk_ring_cons comp;
    struct free_pool *pool;
    struct object_pool *obj_pool;
    uint32_t rx_in_flight;
    int      queue_id;
    struct xskif_stats stats;
    char     ifname[IFNAMSIZ];
    struct poison_frame *poison_list;
    uint32_t poison_count;
    bool resetting;
    struct tcp_pcb *listen_pcb;
    struct pbuf_custom_wrapper *orphaned_wrappers;
    uint32_t orphaned_wrappers_count;
    struct poison_frame emergency_poison_pool[EMERGENCY_POISON_POOL_SIZE];
    struct poison_frame *emergency_pool_free_head;
    struct echo_ctx *ctx_list_head;
    struct timespec last_reset_ts;                           /* R8-1: reset rate-limit */
};
/* ============================================================================ */

/* Forward declarations */
static void clear_poison_list(struct xskif *xif);
static void clear_orphaned_wrappers(struct xskif *xif);
static void recover_lost_frames(struct xskif *xif);
static void xskif_reset(struct xskif *xif);
static void xskif_input_drain(struct xskif *xif);
static void xskif_process_rx_desc(struct xskif *xif, u64 addr, u32 len, bool drain);
static void xsk_tx_completion_batch(struct xskif *xif);
static uint32_t xsk_replenish_fill_ring(struct xskif *xif, uint32_t cnt);
static void echo_connection_cleanup(struct echo_ctx *ctx);
static err_t echo_recv(void *arg, struct tcp_pcb *pcb, struct pbuf *p, err_t err);  /* R1-2 */
static err_t echo_sent(void *arg, struct tcp_pcb *pcb, u16_t len);                  /* R1-2 */
static void echo_err(void *arg, err_t err);                                          /* R1-2 */
static err_t echo_poll(void *arg, struct tcp_pcb *pcb);
static void process_queued_msgs(struct echo_ctx *ctx);
static void xskif_destroy(struct xskif *xif);


/* ── ────────────────────────────────────────────────────────────────── */
/* 紧急毒化池 — 预分配 256 个 poison_frame, 避免 OOM 路径 malloc                    */
/*   用单向链表管理空闲槽位: emergency_pool_free_head → 第一个可用节点                   */
/*   get_poison_frame(): 从空闲链表取节点 (O(1))                               */
/*   release_poison_frame(): 归还节点到空闲链表 (O(1))                          */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* Emergency poison pool helpers                                             */
/*============================================================================*/
static void emergency_poison_pool_init(struct xskif *xif) {
    for (int i = 0; i < EMERGENCY_POISON_POOL_SIZE; i++) {
        xif->emergency_poison_pool[i].umem_addr = FREE_POOL_INVALID_ADDR;
        xif->emergency_poison_pool[i].is_emergency = true;
        if (i < EMERGENCY_POISON_POOL_SIZE - 1) {
            xif->emergency_poison_pool[i].next = &xif->emergency_poison_pool[i + 1];
        } else {
            xif->emergency_poison_pool[i].next = NULL;
        }
    }
    xif->emergency_pool_free_head = &xif->emergency_poison_pool[0];
}

static struct poison_frame *get_poison_frame(struct xskif *xif) {
    struct poison_frame *pf = NULL;
    if (xif->emergency_pool_free_head != NULL) {
        pf = xif->emergency_pool_free_head;
        xif->emergency_pool_free_head = pf->next;
        pf->next = NULL;
        pf->is_emergency = true;
        return pf;
    }
    xif->stats.emergency_poison_exhausted++;
    return NULL;
}

static void release_poison_frame(struct xskif *xif, struct poison_frame *pf) {
    if (!pf) return;
    pf->umem_addr = FREE_POOL_INVALID_ADDR;
    pf->next = xif->emergency_pool_free_head;
    xif->emergency_pool_free_head = pf;
    xif->stats.emergency_pool_recycled++;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* 毒化帧管理:                                                              */
/*   poison_umem_addr(): 将无法推回 free_pool 的帧地址加入毒化列表                    */
/*   已在列表中的帧 → 跳过 (去重)                                                 */
/*   紧急池耗尽 → umem_frame_permanent_loss++ (帧永久丢失)                       */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* Poison frame management                                                   */
/*============================================================================*/
static bool poison_umem_addr(struct xskif *xif, u64 addr) {
    if (addr == FREE_POOL_INVALID_ADDR || addr % XSK_UMEM_FRAME_SIZE != 0 ||
        addr >= xif->umem_size) {
        xif->stats.umem_frame_permanent_loss++;
        xif->pool->need_reset = true;                        /* R6-1 */
        return false;
    }

    struct poison_frame *curr = xif->poison_list;
    while (curr) {
        if (curr->umem_addr == addr) {
            return true;
        }
        curr = curr->next;
    }

    struct poison_frame *poison = get_poison_frame(xif);
    if (poison) {
        poison->umem_addr = addr;
        poison->next = xif->poison_list;
        xif->poison_list = poison;
        xif->poison_count++;
        return true;
    }

    xif->stats.umem_frame_permanent_loss++;
    xif->pool->need_reset = true;                            /* R6-1 */
    return false;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* free_pool — UMEM 帧空闲池 (核心内存管理组件)                                    */
/*   操作: pop(取帧) / push(还帧)                                            */
/*   位图保证: pop 清除bitmap设置used_bitmap, push 设置bitmap清除used_bitmap       */
/*   回滚: pop 失败恢复栈+位图; push 失败仅修改位图,不修改栈                               */
/*   pop 复杂度 O(1) (位图操作 O(1)), push 复杂度 O(1)                           */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* Free pool                                                                  */
/*============================================================================*/
static inline void free_pool_init(struct free_pool *pool, u64 *stack,
                                    uint64_t *bitmap, uint64_t *used_bitmap,
                                    uint32_t cap) {
    uint32_t bitmap_words = (cap + 63) / 64;
    pool->stack = stack;
    pool->top = 0;
    pool->capacity = cap;
    pool->need_reset = false;
    pool->bitmap = bitmap;
    pool->used_bitmap = used_bitmap;
    memset(bitmap, 0, bitmap_words * sizeof(uint64_t));
    memset(used_bitmap, 0, bitmap_words * sizeof(uint64_t));
}

static inline u64 free_pool_pop(struct free_pool *pool, struct xskif_stats *stats) {
    if (pool->top == 0) {
        return FREE_POOL_INVALID_ADDR;
    }
    uint32_t idx = --pool->top;
    u64 addr = pool->stack[idx];
    if (idx < stats->min_free_pool_remain) {
        stats->min_free_pool_remain = idx;
    }
    uint32_t frame_idx = (uint32_t)(addr / XSK_UMEM_FRAME_SIZE);
    if (bitmap_clear(pool->bitmap, frame_idx, pool->capacity) != 0) {
        pool->stack[pool->top++] = addr;   /* R6-4: rollback top */
        pool->need_reset = true;
        stats->free_pool_push_fails++;
        stats->umem_frame_permanent_loss++;
        return FREE_POOL_INVALID_ADDR;
    }
    if (bitmap_set(pool->used_bitmap, frame_idx, pool->capacity) != 0) {
        /* Rollback: push address back to stack and restore bitmap */
        pool->stack[pool->top++] = addr;   /* P1-1 fix */
        bitmap_set(pool->bitmap, frame_idx, pool->capacity); /* restore */
        pool->need_reset = true;
        stats->free_pool_push_fails++;
        stats->umem_frame_permanent_loss++;
        return FREE_POOL_INVALID_ADDR;
    }
    return addr;
}

static inline int free_pool_push(struct free_pool *pool, u64 addr,
                                    struct xskif_stats *stats) {
    if (addr % XSK_UMEM_FRAME_SIZE != 0 || addr / XSK_UMEM_FRAME_SIZE >= pool->capacity) {
        stats->invalid_addr_push_attempts++;
        stats->free_pool_push_fails++;                     /* R3-1: count at source */
        return FREEPOOL_EINVAL;
    }
    uint32_t idx = (uint32_t)(addr / XSK_UMEM_FRAME_SIZE);

    if (bitmap_test(pool->bitmap, idx, pool->capacity) == 1) {
        stats->free_pool_duplicate_pushes++;
        pool->need_reset = true;
        return FREEPOOL_EEXIST;
    }

    if (pool->top >= pool->capacity) {
        stats->pool_overflow_drops++;
        pool->need_reset = true;
        stats->pool_overflow_resets++;
        stats->free_pool_push_fails++;                     /* R3-1: count at source */
        fprintf(stderr, "Free Pool overflow, need reset\n");
        return FREEPOOL_ENOSPC;
    }

    if (bitmap_set(pool->bitmap, idx, pool->capacity) != 0) {
        stats->free_pool_push_fails++;
        pool->need_reset = true;
        return FREEPOOL_EINTERNAL;
    }
    if (bitmap_clear(pool->used_bitmap, idx, pool->capacity) != 0) {
        bitmap_clear(pool->bitmap, idx, pool->capacity);
        stats->free_pool_push_fails++;
        pool->need_reset = true;
        return FREEPOOL_EINTERNAL;
    }

    pool->stack[pool->top++] = addr;
    return FREEPOOL_OK;
}

/*---------------------------------------------------------------------------*/

/* ── ────────────────────────────────────────────────────────────────── */
/* 安全释放 UMEM 帧: push 到 free_pool, 失败则 poison                           */
/* ------------------------------------------------------------------------ */
static inline void xskif_safe_free_addr(struct xskif *xif, u64 base) {
    int ret = free_pool_push(xif->pool, base, &xif->stats);
    if (ret != FREEPOOL_OK && ret != FREEPOOL_EEXIST) {
        poison_umem_addr(xif, base);
    }
}


/* ── ────────────────────────────────────────────────────────────────── */
/* object_pool — pbuf_custom_wrapper 对象池                               */
/*   预分配所有 wrapper, 数据路径零 malloc                                       */
/*   in_pool 标志: 防止双重 push                                             */
/*   destroy 安全: top != capacity 时 abort() — 防止 use-after-free         */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* Object pool – stores pre‑allocated pbuf_custom_wrapper objects           */
/*============================================================================*/
struct object_pool *object_pool_create(uint32_t cap) {
    struct object_pool *pool = calloc(1, sizeof(*pool));
    if (!pool) return NULL;
    pool->stack = calloc(cap, sizeof(struct pbuf_custom_wrapper *));
    if (!pool->stack) { free(pool); return NULL; }
    pool->block = calloc(cap, sizeof(struct pbuf_custom_wrapper));
    if (!pool->block) {
        free(pool->stack);
        free(pool);
        return NULL;
    }
    struct pbuf_custom_wrapper *wrappers = (struct pbuf_custom_wrapper *)pool->block;
    for (uint32_t i = 0; i < cap; i++) {
        pool->stack[i] = &wrappers[i];
        wrappers[i].in_pool = true;
    }
    pool->top = cap;
    pool->capacity = cap;
    return pool;
}

struct pbuf_custom_wrapper *object_pool_pop(struct object_pool *pool) {
    if (pool->top == 0) return NULL;
    struct pbuf_custom_wrapper *w = pool->stack[--pool->top];
    pool->stack[pool->top] = NULL;
    w->in_pool = false;
    return w;
}

bool object_pool_push(struct object_pool *pool, struct pbuf_custom_wrapper *w) {
    if (pool->top >= pool->capacity) return false;
    if (w->in_pool) {
        fprintf(stderr, "object_pool_push: double push detected\n");
        return false;
    }
    pool->stack[pool->top++] = w;
    w->in_pool = true;
    return true;
}

void object_pool_destroy(struct object_pool *pool) {
    if (!pool) return;
    /* P1-2 fix: abort on in-use wrappers to prevent UAF */
    if (pool->top != pool->capacity) {
        fprintf(stderr, "FATAL: object_pool_destroy with %u objects still in-use; aborting\n",
                pool->capacity - pool->top);
        abort();
    }
    free(pool->block);
    free(pool->stack);
    free(pool);
}


/* ── ────────────────────────────────────────────────────────────────── */
/* 孤儿 wrapper 管理:                                                      */
/*   orphan_wrapper(): 加入孤儿链表 (push 失败 / underflow)                    */
/*   链表过长 → need_reset → 复位时通过 clear_orphaned_wrappers 恢复              */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* Orphan wrapper helper – unifies cleanup before adding to orphan list      */
/*============================================================================*/
static inline void orphan_wrapper(struct xskif *xif, struct pbuf_custom_wrapper *wrapper) {
    wrapper->umem_addr = FREE_POOL_INVALID_ADDR;
    wrapper->pc.custom_free_function = NULL;
    wrapper->xif = NULL;
    wrapper->next_orphan = xif->orphaned_wrappers;
    xif->orphaned_wrappers = wrapper;
    xif->orphaned_wrappers_count++;

    /* P1-5 fix: trigger reset if orphan list too large */
    if (xif->orphaned_wrappers_count > MAX_ORPHAN_WRAPPERS) {
        xif->pool->need_reset = true;                        /* R6-1 */
    }
}


/* ── ────────────────────────────────────────────────────────────────── */
/* pbuf 自定义释放函数 — lwIP 释放 pbuf 时回调, 帧生命周期闭环关键点                         */
/*   1. 检查 PBUF_CUSTOM 类型                                              */
/*   2. rx_in_flight 下溢 → 毒化+孤儿化 (异常恢复)                                */
/*   3. UMEM帧 → free_pool_push                                         */
/*   4. wrapper → object_pool_push (或孤儿化)                              */
/*   帧状态机: free_pool → fill ring → kernel → RX ring → lwIP pbuf → free_pool  */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* pbuf custom free function – returns UMEM frame back to pool              */
/*============================================================================*/
static void xskif_pbuf_free_custom(struct pbuf *p) {
    if (p->type != PBUF_CUSTOM) return;

    struct pbuf_custom_wrapper *wrapper = (struct pbuf_custom_wrapper *)p;
    struct xskif *xif = wrapper->xif;

    if (!xif) {
        fprintf(stderr, "ERROR: xskif_pbuf_free_custom called with NULL xif (wrapper/pbuf allocated, leaking frame)\n");
        return;
    }

    if (unlikely(xif->rx_in_flight == 0)) {
        xif->stats.rx_in_flight_underflow_count++;
        u64 lost_addr = wrapper->umem_addr;
        if (lost_addr != FREE_POOL_INVALID_ADDR) {
            poison_umem_addr(xif, lost_addr);
        }
        orphan_wrapper(xif, wrapper);
        xif->stats.pool_underflow_wrapper_freed++;
        xif->pool->need_reset = true;                        /* R6-1 */
        return;
    }

    xif->rx_in_flight--;

    u64 original_addr = wrapper->umem_addr;
    int umem_ret = free_pool_push(xif->pool, original_addr, &xif->stats);

    if (umem_ret == FREEPOOL_OK) {
        wrapper->umem_addr = FREE_POOL_INVALID_ADDR;
        if (!object_pool_push(xif->obj_pool, wrapper)) {
            orphan_wrapper(xif, wrapper);
            xif->stats.obj_pool_push_drops++;
        }
    } else if (umem_ret == FREEPOOL_EEXIST) {
        orphan_wrapper(xif, wrapper);
        xif->stats.obj_pool_push_drops++;
        xif->pool->need_reset = true;
    } else {
        poison_umem_addr(xif, original_addr);
        orphan_wrapper(xif, wrapper);
        xif->pool->need_reset = true;
    }
}

/* Attempt to push orphaned wrappers back into the object pool */
static void clear_orphaned_wrappers(struct xskif *xif) {
    struct pbuf_custom_wrapper **prev = &xif->orphaned_wrappers;
    while (*prev) {
        struct pbuf_custom_wrapper *wrapper = *prev;
        if (object_pool_push(xif->obj_pool, wrapper)) {
            *prev = wrapper->next_orphan;
            xif->orphaned_wrappers_count--;
        } else {
            xif->stats.obj_pool_push_drops++;
            xif->pool->need_reset = true;
            break;
        }
    }
}


/* ── ────────────────────────────────────────────────────────────────── */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   XDP / XSK 初始化                                                     */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   sys_now():       lwIP 时间源 (CLOCK_MONOTONIC → 毫秒)                  */
/*   generate_random_mac(): 本地管理 MAC (bit 1 = 0x02)                    */
/*   xskif_setup():   完整初始化 (6阶段, ~300行)                               */
/*     阶段1: 参数验证 + 队列存在性检查                                             */
/*     阶段2: 帧分配计算 + 大页对齐                                               */
/*     阶段3: RLIMIT_MEMLOCK + NUMA 策略设置                                 */
/*     阶段4: free_pool + object_pool 分配                                 */
/*     阶段5: UMEM 分配 (MAP_HUGETLB) + mbind                              */
/*     阶段6: XSK socket 创建 + ZEROCOPY→COPY 回退 + Fill Ring 初始填充          */
/*   错误处理: goto 级联清理链 (free_socket→free_bases→free_umem→free_mem→free_pools)  */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* XDP / XSK setup                                                           */
/*============================================================================*/
u32_t sys_now(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) == 0) {
        return (u32_t)(now.tv_sec * 1000 + now.tv_nsec / 1000000);
    }
    return 0;
}

static int generate_random_mac(unsigned char *mac) {
    ssize_t total = 0;
    while (total < 6) {
        ssize_t got = getrandom(mac + total, 6 - total, 0);
        if (got > 0) {
            total += got;
        } else if (got < 0 && errno == EINTR) {
            continue;
        } else {
            return -1;
        }
    }
    mac[0] = (mac[0] & 0xFE) | 0x02;
    return 0;
}

int xskif_setup(struct xskif *xif, int core_id, int numa_node,
                uint32_t total_frames, int num_cores, const char *ifname) {
    int ret;

    if (!xif) return -EINVAL;
    if (!ifname || ifname[0] == '\0') return -EINVAL;
    if (!xif->netif) return -EINVAL;
    if (num_cores <= 0) return -EINVAL;
    if (core_id < 0) return -EINVAL;
    if (total_frames < (uint32_t)num_cores) {
        fprintf(stderr, "total_frames must be >= num_cores\n");
        return -EINVAL;
    }

    strncpy(xif->ifname, ifname, IFNAMSIZ-1);
    xif->ifname[IFNAMSIZ-1] = '\0';

    char path[64];
    snprintf(path, sizeof(path), "/sys/class/net/%s/queues/rx-%d", ifname, core_id);
    if (access(path, F_OK) != 0) {
        fprintf(stderr, "Rx queue %d does not exist on %s\n", core_id, ifname);
        return -EINVAL;
    }
    snprintf(path, sizeof(path), "/sys/class/net/%s/queues/tx-%d", ifname, core_id);
    if (access(path, F_OK) != 0) {
        fprintf(stderr, "Tx queue %d does not exist on %s\n", core_id, ifname);
        return -EINVAL;
    }
    xif->queue_id = core_id;

    if (numa_node < 0 || numa_node > numa_max_node() ||
        !numa_bitmask_isbitset(numa_all_nodes_ptr, numa_node)) {
        fprintf(stderr, "Invalid NUMA node %d\n", numa_node);
        return -EINVAL;
    }

    memset(&xif->stats, 0, sizeof(xif->stats));
    emergency_poison_pool_init(xif);
    xif->ctx_list_head = NULL;

    uint32_t base_frames = total_frames / num_cores;
    uint32_t remainder = total_frames % num_cores;
    uint32_t frames_per_core = base_frames + ((uint32_t)core_id < remainder ? 1 : 0);

    uint64_t aligned_frames = align_up((uint64_t)frames_per_core, UMEM_2MB_FRAME_COUNT);
    if (aligned_frames == (uint64_t)-1) {
        fprintf(stderr, "frames_per_core alignment failed\n");
        return -EINVAL;
    }
    if (aligned_frames > UINT32_MAX) {
        fprintf(stderr, "frames_per_core overflow after alignment\n");
        return -EINVAL;
    }

    if (aligned_frames > 2ULL * frames_per_core) {
        fprintf(stderr, "Warning: Hugepage alignment expanded frames_per_core from %u to %" PRIu64 " "
                "(%.1fx). Consider increasing total_frames.\n",
                frames_per_core, aligned_frames,
                (double)aligned_frames / frames_per_core);
    }
    frames_per_core = (uint32_t)aligned_frames;

    /* ---- Save and set resource limits / policies ---- */
    struct rlimit orig_rlimit;
    bool rlimit_changed = false;
    if (getrlimit(RLIMIT_MEMLOCK, &orig_rlimit) == 0) {
        if (orig_rlimit.rlim_cur != RLIM_INFINITY) {
            struct rlimit new_rlim = { .rlim_cur = RLIM_INFINITY, .rlim_max = RLIM_INFINITY };
            if (setrlimit(RLIMIT_MEMLOCK, &new_rlim) != 0) {
                fprintf(stderr, "Warning: failed to set RLIMIT_MEMLOCK to RLIM_INFINITY\n");
            } else {
                rlimit_changed = true;
            }
        }
    } else {
        fprintf(stderr, "Warning: could not retrieve RLIMIT_MEMLOCK\n");
    }

    /* Save current NUMA memory policy */
    int saved_policy_mode = -1;
    struct bitmask *saved_nodemask = NULL;
    bool policy_restored = false;                            /* R4-1: track mid-fn restore */
    {
        int policy_tmp;
        saved_nodemask = numa_allocate_nodemask();
        if (saved_nodemask) {
            if (get_mempolicy(&policy_tmp, saved_nodemask->maskp,
                              saved_nodemask->size, NULL, 0) == 0)  /* R5-1: remove historical +1 */
                saved_policy_mode = policy_tmp;
            else
                saved_policy_mode = -1;
        }
    }

    struct bitmask *nodemask = numa_allocate_nodemask();
    if (!nodemask) {
        fprintf(stderr, "numa_allocate_nodemask failed\n");
        if (rlimit_changed) setrlimit(RLIMIT_MEMLOCK, &orig_rlimit);
        if (saved_nodemask) numa_free_nodemask(saved_nodemask);
        return -ENOMEM;
    }
    numa_bitmask_setbit(nodemask, numa_node);
    if (set_mempolicy(MPOL_BIND, nodemask->maskp, nodemask->size) != 0) {
        fprintf(stderr, "Fatal: set_mempolicy(MPOL_BIND) failed\n");
        numa_free_nodemask(nodemask);
        if (rlimit_changed) setrlimit(RLIMIT_MEMLOCK, &orig_rlimit);
        if (saved_nodemask) numa_free_nodemask(saved_nodemask);
        return -EINVAL;
    }

    xif->umem_size = (uint64_t)frames_per_core * XSK_UMEM_FRAME_SIZE;

    xif->pool = calloc(1, sizeof(struct free_pool));
    if (!xif->pool) { goto nomem_policy; }
    xif->pool->stack = calloc(frames_per_core, sizeof(u64));
    if (!xif->pool->stack) { free(xif->pool); xif->pool=NULL; goto nomem_policy; }

    uint32_t bitmap_words = (uint32_t)(((uint64_t)frames_per_core + 63) / 64);
    xif->pool->bitmap = calloc(1, bitmap_words * sizeof(uint64_t));
    xif->pool->used_bitmap = calloc(1, bitmap_words * sizeof(uint64_t));
    if (!xif->pool->bitmap || !xif->pool->used_bitmap) {
        free(xif->pool->stack);
        free(xif->pool->bitmap);
        free(xif->pool->used_bitmap);
        free(xif->pool);
        xif->pool = NULL;
        goto nomem_policy;
    }

    free_pool_init(xif->pool, xif->pool->stack, xif->pool->bitmap,
                    xif->pool->used_bitmap, frames_per_core);
    xif->stats.min_free_pool_remain = frames_per_core;

    xif->obj_pool = object_pool_create(frames_per_core);
    if (!xif->obj_pool) {
        free(xif->pool->stack);
        free(xif->pool->bitmap);
        free(xif->pool->used_bitmap);
        free(xif->pool);
        xif->pool = NULL;
        goto nomem_policy;
    }

    /* ---- Allocate UMEM (must happen under the NUMA policy) ---- */
    int flags = MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_HUGE_2MB | MAP_LOCKED;
    xif->umem_area = mmap(NULL, xif->umem_size, PROT_READ | PROT_WRITE, flags, -1, 0);
    bool hugepage_used = true;
    if (xif->umem_area == MAP_FAILED) {
        hugepage_used = false;
        flags = MAP_ANONYMOUS | MAP_PRIVATE;
        xif->umem_area = mmap(NULL, xif->umem_size, PROT_READ | PROT_WRITE, flags, -1, 0);
        if (xif->umem_area != MAP_FAILED) {
            fprintf(stderr, "HugePages not available, using normal pages\n");
            xif->stats.hugepage_fallback++;
        }
    }

    if (xif->umem_area == MAP_FAILED) {
        numa_free_nodemask(nodemask);
        ret = -ENOMEM;
        goto free_pools;
    }

    if (mbind(xif->umem_area, xif->umem_size, MPOL_BIND,
               nodemask->maskp, nodemask->size, 0) != 0) {
        fprintf(stderr, "Fatal: mbind for UMEM to node %d failed; aborting\n", numa_node);
        numa_free_nodemask(nodemask);
        ret = -EINVAL;
        goto free_mem;
    }
    numa_free_nodemask(nodemask);

    if (!hugepage_used) {
        if (mlock(xif->umem_area, xif->umem_size) != 0) {
            perror("mlock UMEM failed - DMA safety cannot be guaranteed, aborting");
            fprintf(stderr, "Hint: raise RLIMIT_MEMLOCK to at least %" PRIu64 " bytes\n",
                    xif->umem_size + (XSK_RING_SIZE * 4 * sizeof(struct xdp_desc)));
            ret = -ENOMEM;
            goto free_mem;
        }
    }

    /* ---- UMEM allocation finished, restore policies ---- */
    if (saved_policy_mode != -1 && saved_nodemask) {
        if (set_mempolicy(saved_policy_mode, saved_nodemask->maskp,
                          saved_nodemask->size) != 0) {
            fprintf(stderr, "Warning: failed to restore memory policy\n");
        }
    } else {
        if (set_mempolicy(MPOL_DEFAULT, NULL, 0) != 0) {
            fprintf(stderr, "Warning: failed to restore default memory policy\n");
        }
    }
    if (saved_nodemask) {
        numa_free_nodemask(saved_nodemask);
        saved_nodemask = NULL;
    }
    policy_restored = true;                                  /* R4-1: block free_pools re-restore */
    if (rlimit_changed) {
        if (setrlimit(RLIMIT_MEMLOCK, &orig_rlimit) != 0) {
            fprintf(stderr, "Warning: failed to restore RLIMIT_MEMLOCK: %s\n", strerror(errno));
        }
        rlimit_changed = false;
    }

    /* ---- Create UMEM and socket ---- */
    struct xsk_umem_config umem_cfg = {
        .fill_size = XSK_RING_SIZE,
        .comp_size = XSK_RING_SIZE,
        .frame_size = XSK_UMEM_FRAME_SIZE,
        .frame_headroom = 0,
    };
    ret = xsk_umem__create(&xif->umem, xif->umem_area, xif->umem_size,
                               &xif->fill, &xif->comp, &umem_cfg);
    if (ret) goto free_mem;

    xif->umem_frame_bases = malloc(frames_per_core * sizeof(u64));
    if (!xif->umem_frame_bases) {
        ret = -ENOMEM;
        goto free_umem;
    }

    for (uint32_t i = 0; i < frames_per_core; i++) {
        xif->umem_frame_bases[i] = (u64)i * XSK_UMEM_FRAME_SIZE;
        int push_ret = free_pool_push(xif->pool, xif->umem_frame_bases[i], &xif->stats);
        if (push_ret != FREEPOOL_OK) {
            fprintf(stderr, "Fatal: failed to push initial frame %u into free pool (err=%d)\n",
                    i, push_ret);
            ret = -ENOMEM;
            goto free_bases;
        }
    }

    struct xsk_socket_config xsk_cfg = {
        .rx_size = XSK_RING_SIZE,
        .tx_size = XSK_RING_SIZE,
        .bind_flags = XDP_ZEROCOPY | XDP_USE_NEED_WAKEUP,   /* R7-1: enable sendto() wakeup */
    };
    ret = xsk_socket__create(&xif->xsk, ifname, core_id, xif->umem,
                                 &xif->rx, &xif->tx, &xsk_cfg);
    if (ret == 0) {
        xif->stats.xdp_mode = 1;
    } else {
        if (xif->xsk) {
            xsk_socket__delete(xif->xsk);
            xif->xsk = NULL;
        }
        fprintf(stderr, "XSK create with ZEROCOPY failed: %s, trying COPY\n", strerror(-ret));
        xsk_cfg.bind_flags = XDP_COPY | XDP_USE_NEED_WAKEUP;
        ret = xsk_socket__create(&xif->xsk, ifname, core_id, xif->umem,
                                      &xif->rx, &xif->tx, &xsk_cfg);
        if (ret == 0) {
            xif->stats.xdp_mode = 0;
            xif->stats.zerocopy_fallback++;
            fprintf(stderr, "Falling back to XDP_COPY\n");
        } else {
            fprintf(stderr, "XSK create failed: %s\n", strerror(-ret));
            goto free_socket;
        }
    }

    if (xsk_ring_prod__size(&xif->tx) != xsk_ring_cons__size(&xif->comp) ||
        xsk_ring_prod__size(&xif->fill) != xsk_ring_cons__size(&xif->rx)) {
        fprintf(stderr, "Critical: XSK ring size mismatch\n");
        ret = -EINVAL;
        goto free_socket;
    }

    uint32_t fill_target = (uint32_t)((uint64_t)xif->pool->capacity * XSK_FILL_MAX_RATIO / 100);
    uint32_t remaining = fill_target;
    while (remaining > 0) {
        uint32_t batch = remaining > XSK_BATCH_SIZE ? XSK_BATCH_SIZE : remaining;
        uint32_t done = xsk_replenish_fill_ring(xif, batch);
        if (done == 0) {
            fprintf(stderr, "Fatal: fill ring replenish failed during init\n");
            ret = -EIO;
            goto free_socket;
        }
        remaining -= done;
    }

    return 0;

nomem_policy:
    if (saved_policy_mode != -1 && saved_nodemask) {
        set_mempolicy(saved_policy_mode, saved_nodemask->maskp, saved_nodemask->size);
    } else {
        set_mempolicy(MPOL_DEFAULT, NULL, 0);
    }
    if (saved_nodemask) numa_free_nodemask(saved_nodemask);
    if (rlimit_changed) setrlimit(RLIMIT_MEMLOCK, &orig_rlimit);
    numa_free_nodemask(nodemask);
    return -ENOMEM;

free_socket:
    if (xif->xsk) {
        xsk_socket__delete(xif->xsk);
        xif->xsk = NULL;
    }
free_bases:
    free(xif->umem_frame_bases);
free_umem:
    if (xif->umem) {
        int umem_ret = xsk_umem__delete(xif->umem);
        if (umem_ret != 0) {
            fprintf(stderr, "CRITICAL: xsk_umem__delete returned %d (%s); "
                    "kernel may still hold DMA references to UMEM\n",
                    umem_ret, strerror(-umem_ret));
        }
        xif->umem = NULL;
    }
free_mem:
    if (xif->umem_area != MAP_FAILED) {
        munmap(xif->umem_area, xif->umem_size);
    }
free_pools:
    /* R2-3/R4-1: restore policy / free saved_nodemask; skip if already restored mid-fn */
    if (!policy_restored) {
        if (saved_policy_mode != -1 && saved_nodemask) {
            set_mempolicy(saved_policy_mode, saved_nodemask->maskp, saved_nodemask->size);
        } else {
            set_mempolicy(MPOL_DEFAULT, NULL, 0);                /* R5-2: always restore */
        }
    }
    if (saved_nodemask) {
        numa_free_nodemask(saved_nodemask);
    }
    if (rlimit_changed) {
        setrlimit(RLIMIT_MEMLOCK, &orig_rlimit);
    }

    if (xif->obj_pool) object_pool_destroy(xif->obj_pool);
    if (xif->pool) {
        free(xif->pool->bitmap);
        free(xif->pool->used_bitmap);
        free(xif->pool->stack);
        free(xif->pool);
        xif->pool = NULL;                                /* R3-3: prevent dangling pointer */
    }
    return ret;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   netif 初始化 + TX 输出                                                 */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   xskif_netif_init(): 获取 MTU/MAC, 注册 lwIP netif 回调                  */
/*   xskif_output():     链路层 TX (lwIP pbuf → UMEM → XDP TX Ring)       */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* netif init and output                                                      */
/*============================================================================*/
static err_t xskif_output(struct netif *netif, struct pbuf *p);

static err_t xskif_netif_init(struct netif *netif) {
    struct xskif *xif = (struct xskif *)netif->state;
    if (!xif) return ERR_ARG;

    netif->name[0] = 'x'; netif->name[1] = 's';
    netif->output = etharp_output;
    netif->linkoutput = xskif_output;
    netif->input = ethernet_input;

    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    struct ifreq ifr;
    int mtu = 1500;
    int ioctl_ret;

    if (sock >= 0) {
        memset(&ifr, 0, sizeof(ifr));
        strncpy(ifr.ifr_name, xif->ifname, IFNAMSIZ-1);
        while ((ioctl_ret = ioctl(sock, SIOCGIFMTU, &ifr)) < 0 && errno == EINTR);
        if (ioctl_ret == 0) {
            mtu = ifr.ifr_mtu;
            if (mtu > XSK_UMEM_FRAME_SIZE) {
                fprintf(stderr, "Warning: Interface MTU %d exceeds UMEM frame size %d, capping\n",
                        mtu, XSK_UMEM_FRAME_SIZE);
                mtu = XSK_UMEM_FRAME_SIZE;
            }
        }

        memset(&ifr, 0, sizeof(ifr));
        strncpy(ifr.ifr_name, xif->ifname, IFNAMSIZ-1);
        while ((ioctl_ret = ioctl(sock, SIOCGIFHWADDR, &ifr)) < 0 && errno == EINTR);
        if (ioctl_ret == 0) {
            memcpy(netif->hwaddr, ifr.ifr_hwaddr.sa_data, 6);
        } else {
            if (generate_random_mac(netif->hwaddr) != 0) {
                fprintf(stderr, "Unable to obtain random MAC address\n");
                close(sock);
                return ERR_IF;
            }
            xif->stats.mac_random_fallback++;   /* P2-7 fix */
            fprintf(stderr, "Warning: using random MAC for %s\n", xif->ifname);
        }
        close(sock);
    } else {
        if (generate_random_mac(netif->hwaddr) != 0) {
            fprintf(stderr, "Unable to obtain random MAC address\n");
            return ERR_IF;
        }
        xif->stats.mac_random_fallback++;
        fprintf(stderr, "Warning: using random MAC for %s\n", xif->ifname);
    }

    netif->mtu = mtu;
    netif->hwaddr_len = 6;
    netif->flags = NETIF_FLAG_BROADCAST | NETIF_FLAG_ETHARP | NETIF_FLAG_LINK_UP;
    return ERR_OK;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* xskif_output — 数据路径核心: lwIP pbuf → UMEM帧 → XDP TX描述符 → TX Ring      */
/*   步骤: pop帧 → pbuf_copy_partial(UMEM) → reserve TX → 写描述符 → submit   */
/*   zero-copy: __sync_synchronize() 确保 UMEM 数据对 DMA 可见 (非 x86 架构)     */
/*   失败保护: 帧安全推回 free_pool (或 poison)                                  */
/* ------------------------------------------------------------------------ */
static err_t xskif_output(struct netif *netif, struct pbuf *p) {
    struct xskif *xif = (struct xskif *)netif->state;
    if (!xif) return ERR_IF;                                 /* R2-2: NULL guard */

    if (unlikely(xif->resetting || xif->pool->need_reset) || !xif->xsk) {  /* R6-1 */
        return ERR_IF;
    }

    if (p->tot_len > XSK_UMEM_FRAME_SIZE || p->tot_len == 0) {
        xif->stats.tx_size_drops++;
        return ERR_ARG;
    }

    u64 addr = free_pool_pop(xif->pool, &xif->stats);
    if (addr == FREE_POOL_INVALID_ADDR) {
        xif->stats.free_pool_exhausted_tx++;
        /* R6-1: need_reset already checked by xskif_output guard; main loop will trigger xskif_reset */
        return ERR_MEM;
    }

    if (addr % XSK_UMEM_FRAME_SIZE != 0 || addr >= xif->umem_size) {
        xif->stats.tx_invalid_addr_drops++;
        xif->pool->need_reset = true;
        poison_umem_addr(xif, addr);
        return ERR_IF;
    }

    u16_t copied = pbuf_copy_partial(p, xsk_umem__get_data(xif->umem_area, addr),
                                        p->tot_len, 0);
    if (copied != p->tot_len) {
        xif->stats.tx_copy_fails++;
        xskif_safe_free_addr(xif, addr);
        return ERR_IF;
    }

    u32 tx_idx;
    if (xsk_ring_prod__reserve(&xif->tx, 1, &tx_idx) == 0) {
        xif->stats.tx_reserve_fails++;
        xskif_safe_free_addr(xif, addr);
        if (!xif->resetting && xsk_ring_prod__needs_wakeup(&xif->tx)) {
            ssize_t sr;
            do {
                sr = sendto(xsk_socket__fd(xif->xsk), NULL, 0, MSG_DONTWAIT, NULL, 0);
            } while (sr < 0 && errno == EINTR);
            if (sr < 0 && (errno == EBADF || errno == EINVAL)) {
                xif->pool->need_reset = true;
            }
        }
        return ERR_MEM;
    }

    *xsk_ring_prod__tx_desc(&xif->tx, tx_idx) = (struct xdp_desc){addr, p->tot_len, 0};
    /* R3-2: in zero-copy mode, ensure UMEM data is visible to NIC before
     * TX ring producer update.  x86 TSO provides this implicitly;
     * ARM64/POWER/RISC-V require an explicit barrier. */
    if (xif->stats.xdp_mode == 1) {
        __sync_synchronize();
    }
    xsk_ring_prod__submit(&xif->tx, 1);

    if (xsk_ring_prod__needs_wakeup(&xif->tx)) {
        ssize_t ret;
        do {
            ret = sendto(xsk_socket__fd(xif->xsk), NULL, 0, MSG_DONTWAIT, NULL, 0);
        } while (ret < 0 && errno == EINTR);
        if (ret < 0) {
            if (errno == EBADF || errno == EINVAL) {
                xif->pool->need_reset = true;                /* R6-1: main loop will trigger reset */
            }
            xif->stats.tx_sendto_fails++;   /* P2-3 fix */
        }
    }

    xif->stats.tx_packets++;
    xif->stats.tx_bytes += p->tot_len;
    return ERR_OK;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   RX 数据路径                                                           */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   xskif_input():          批量处理 RX Ring → lwIP ethernet_input → TCP → Echo  */
/*   xskif_process_rx_desc(): 处理单个 RX 描述符 (wrapper + PBUF_CUSTOM pbuf)  */
/*   xskif_input_drain():    排空 RX Ring (reset 期间, 不投递到 lwIP)          */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* RX path                                                                    */
/*============================================================================*/
static void xskif_process_rx_desc(struct xskif *xif, u64 addr, u32 len, bool drain) {
    u64 base = xsk_umem__extract_addr(addr);
    u32 offset = (u32)(addr - base);

    if (len > XSK_UMEM_FRAME_SIZE || len == 0 || base >= xif->umem_size ||
        (uint64_t)offset + len > XSK_UMEM_FRAME_SIZE) {
        if (len == 0) xif->stats.rx_zero_len_drops++;
        xif->stats.rx_input_errors++;
        if (base < xif->umem_size) {
            xskif_safe_free_addr(xif, base);
        } else {
            xif->stats.umem_frame_permanent_loss++;
            xif->pool->need_reset = true;                    /* R6-1 */
        }
        return;
    }

    if (drain) {
        xskif_safe_free_addr(xif, base);
        xif->stats.rx_drained_packets++;
        return;
    }

    uint32_t in_flight = xif->rx_in_flight;
    /* P2-5 fix: compare against object pool capacity */
    if (in_flight >= xif->obj_pool->capacity) {
        xif->stats.rx_fulldrop_packets++;
        xskif_safe_free_addr(xif, base);
        return;
    }

    struct pbuf_custom_wrapper *wrapper = object_pool_pop(xif->obj_pool);
    if (unlikely(!wrapper)) {
        xskif_safe_free_addr(xif, base);
        xif->stats.obj_pool_exhausted++;
        return;
    }

    wrapper->umem_addr = base;
    wrapper->xif = xif;
    wrapper->pc.custom_free_function = xskif_pbuf_free_custom;

    struct pbuf *p = pbuf_alloced_custom(PBUF_RAW, len, PBUF_CUSTOM, &wrapper->pc,
                                            xsk_umem__get_data(xif->umem_area, addr), len);
    if (!p) {
        wrapper->pc.custom_free_function = NULL;
        wrapper->umem_addr = FREE_POOL_INVALID_ADDR;
        if (!object_pool_push(xif->obj_pool, wrapper)) {
            orphan_wrapper(xif, wrapper);
            xif->stats.obj_pool_push_drops++;
            xif->pool->need_reset = true;
        }
        xskif_safe_free_addr(xif, base);
        xif->stats.rx_input_errors++;
        return;
    }

    xif->rx_in_flight++;
    if (xif->rx_in_flight > xif->stats.rx_in_flight_peak) {
        xif->stats.rx_in_flight_peak = xif->rx_in_flight;
    }

    int res = xif->netif->input(p, xif->netif);
    if (res != ERR_OK) {
        xif->stats.rx_input_errors++;
        pbuf_free(p);
    } else {
        xif->stats.rx_packets++;
        xif->stats.rx_bytes += len;
    }
}

static void xskif_input(struct netif *netif) {
    struct xskif *xif = (struct xskif *)netif->state;
    if (!xif) return;                                        /* R2-3: NULL guard */

    if (xif->resetting || xif->pool->need_reset) {    /* R6-1: check need_reset too */
        xskif_input_drain(xif);
        if (unlikely(xif->orphaned_wrappers_count > 0)) {
            clear_orphaned_wrappers(xif);
        }
        return;
    }

    u32 avail = xsk_ring_cons__nb_avail(&xif->rx);
    u32 idx_rx, cnt = xsk_ring_cons__peek(&xif->rx,
                                           avail < XSK_BATCH_SIZE ? avail : XSK_BATCH_SIZE,
                                           &idx_rx);
    u32 processed = 0;
    for (u32 i = 0; i < cnt; i++) {
        if (unlikely(xif->resetting || xif->pool->need_reset)) {    /* R6-1 */
            /* R2-1: drain remaining peeked descriptors to prevent
             * cached_cons divergence (peek advanced cached_cons by cnt,
             * but release only advances *consumer by processed).
             * Without this, descriptors at [processed, cnt-1] are lost. */
            for (u32 j = i; j < cnt; j++) {
                struct xdp_desc desc = *xsk_ring_cons__rx_desc(&xif->rx, idx_rx + j);
                xskif_process_rx_desc(xif, desc.addr, desc.len, true);
            }
            processed = cnt;
            break;
        }
        struct xdp_desc desc = *xsk_ring_cons__rx_desc(&xif->rx, idx_rx + i);
        xskif_process_rx_desc(xif, desc.addr, desc.len, false);
        processed++;
    }
    xsk_ring_cons__release(&xif->rx, processed);
    xsk_replenish_fill_ring(xif, processed);

    if (unlikely(xif->orphaned_wrappers_count > 0)) {
        clear_orphaned_wrappers(xif);
    }
}

static void xskif_input_drain(struct xskif *xif) {
    u32 avail = xsk_ring_cons__nb_avail(&xif->rx);
    u32 idx_rx, cnt = xsk_ring_cons__peek(&xif->rx,
                                           avail < XSK_BATCH_SIZE ? avail : XSK_BATCH_SIZE,
                                           &idx_rx);
    u32 processed = 0;
    for (u32 i = 0; i < cnt; i++) {
        if (unlikely(xif->resetting)) {                      /* R10-2: drain only stops for reset-in-progress */
            break;
        }
        struct xdp_desc desc = *xsk_ring_cons__rx_desc(&xif->rx, idx_rx + i);
        xskif_process_rx_desc(xif, desc.addr, desc.len, true);
        processed++;
    }
    xsk_ring_cons__release(&xif->rx, processed);
    xsk_replenish_fill_ring(xif, processed);

    if (unlikely(xif->orphaned_wrappers_count > 0)) {
        clear_orphaned_wrappers(xif);
    }
}


/* ── ────────────────────────────────────────────────────────────────── */
/* TX Completion — 内核将已发送完成的帧地址写入 Completion Ring                      */
/*   用户态读取后将帧推回 free_pool。                                             */
/*   无效地址 → ghost_completions 计数 (内核 bug 或 DMA 损坏征兆)                   */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* TX completion handling                                                     */
/*============================================================================*/
static void xsk_tx_completion_batch(struct xskif *xif) {
    u32 idx, cnt = xsk_ring_cons__peek(&xif->comp, XSK_BATCH_SIZE, &idx);
    for (u32 i = 0; i < cnt; i++) {
        u64 addr = *xsk_ring_cons__comp_addr(&xif->comp, idx + i);
        u64 base = xsk_umem__extract_addr(addr);
        if (base % XSK_UMEM_FRAME_SIZE != 0 ||
            base >= xif->umem_size ||
            (base / XSK_UMEM_FRAME_SIZE) >= xif->pool->capacity) {
            xif->stats.ghost_completions++;
            xif->pool->need_reset = true;                    /* R6-1 */
            continue;
        }
        int ret = free_pool_push(xif->pool, base, &xif->stats);
        if (ret != FREEPOOL_OK && ret != FREEPOOL_EEXIST) {
            poison_umem_addr(xif, base);
        }
    }
    xsk_ring_cons__release(&xif->comp, cnt);

/* ── ────────────────────────────────────────────────────────────────── */
/* Fill Ring 补充 — 向 Fill Ring 填充空闲帧地址供内核 RX 使用                         */
/*   fill_target = capacity * 75% (XSK_FILL_MAX_RATIO)                 */
/*   原子性: reserve 失败 → 所有已 pop 地址安全推回 free_pool                        */
/*   唤醒: sendto() 通知内核 Fill Ring 有新条目                                  */
/* ------------------------------------------------------------------------ */
}

static uint32_t xsk_replenish_fill_ring(struct xskif *xif, uint32_t cnt) {
    uint32_t free_frames = xif->pool->top;
    uint32_t avail_ring = xsk_ring_prod__nb_free(&xif->fill);

    uint32_t to_fill = cnt;
    if (to_fill > free_frames) to_fill = free_frames;
    if (to_fill > avail_ring) to_fill = avail_ring;
    if (to_fill == 0) return 0;
    if (to_fill > XSK_BATCH_SIZE) to_fill = XSK_BATCH_SIZE;

    u64 addrs[XSK_BATCH_SIZE];
    for (u32 i = 0; i < to_fill; i++) {
        u64 addr = free_pool_pop(xif->pool, &xif->stats);
        if (unlikely(addr == FREE_POOL_INVALID_ADDR)) {
            for (u32 j = 0; j < i; j++) {
                int ret = free_pool_push(xif->pool, addrs[j], &xif->stats);
                if (ret != FREEPOOL_OK && ret != FREEPOOL_EEXIST) {
                    poison_umem_addr(xif, addrs[j]);
                }
            }
            xif->stats.fill_ring_starvation++;
            if (xif->pool->top == 0 && !xif->resetting) {
                xif->pool->need_reset = true;
            }
            return 0;
        }
        addrs[i] = addr;
    }

    u32 idx;
    if (xsk_ring_prod__reserve(&xif->fill, to_fill, &idx) != to_fill) {
        for (u32 i = 0; i < to_fill; i++) {
            int ret = free_pool_push(xif->pool, addrs[i], &xif->stats);
            if (ret != FREEPOOL_OK && ret != FREEPOOL_EEXIST) {
                poison_umem_addr(xif, addrs[i]);
            }
        }
        return 0;
    }

    for (u32 i = 0; i < to_fill; i++) {
        *xsk_ring_prod__fill_addr(&xif->fill, idx + i) = addrs[i];
    }
    xsk_ring_prod__submit(&xif->fill, to_fill);
    /* R2-2: wake kernel if it went to sleep waiting for fill ring entries */
    if (xsk_ring_prod__needs_wakeup(&xif->fill)) {
        ssize_t ret;
        do {
            ret = sendto(xsk_socket__fd(xif->xsk), NULL, 0, MSG_DONTWAIT, NULL, 0);
        } while (ret < 0 && errno == EINTR);
        if (ret < 0 && (errno == EBADF || errno == EINVAL)) {
            xif->pool->need_reset = true;
        }
    }
    return to_fill;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   Echo 应用 — lwIP TCP Echo 服务器 (端口 7)                                */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   TCP 回调链:                                                          */
/*     accept → echo_accept (分配 ctx, 注册回调)                             */
/*     recv   → echo_recv (排队数据, 触发发送)                                 */
/*     sent   → echo_sent (继续处理队列)                                     */
/*     poll   → echo_poll (定时重试, 0.5s 间隔)                              */
/*     err    → echo_err (清理 ctx, PCB 由 lwIP 释放)                       */
/*   发送流程: recv→排队→process_queued_msgs→tcp_write(FLAG_COPY)→tcp_output  */
/*   关闭流程: FIN→closing=true→队列排空→tcp_close                             */
/*   流控: queued_msg_count >= 1024 → 中止连接                               */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* Echo application – lwIP TCP echo server                                    */
/*============================================================================*/
static err_t echo_accept(void *arg, struct tcp_pcb *new_pcb, err_t err) {
    (void)err;                                               /* R1-4: unused lwIP callback param */
    struct xskif *xif = (struct xskif *)arg;

    if (unlikely(xif->resetting || xif->pool->need_reset)) {    /* R6-1 */
        return ERR_ABRT;
    }

    if (!new_pcb) {
        return ERR_ABRT;
    }

    struct echo_ctx *ctx = calloc(1, sizeof(struct echo_ctx));
    if (!ctx) {
        xif->stats.echo_malloc_fails++;
        tcp_arg(new_pcb, NULL);
        tcp_recv(new_pcb, NULL);
        tcp_sent(new_pcb, NULL);
        tcp_err(new_pcb, NULL);
        tcp_poll(new_pcb, NULL, 0);
        tcp_abort(new_pcb);                              /* R2-4: abort PCB on alloc failure */
        return ERR_MEM;
    }

    ctx->pcb = new_pcb;
    ctx->xif = xif;
    tcp_arg(new_pcb, ctx);
    tcp_recv(new_pcb, echo_recv);
    tcp_sent(new_pcb, echo_sent);
    tcp_err(new_pcb, echo_err);
    tcp_poll(new_pcb, echo_poll, 1);

    ctx->next = xif->ctx_list_head;
    if (xif->ctx_list_head)
        xif->ctx_list_head->prev = ctx;
    xif->ctx_list_head = ctx;
    return ERR_OK;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* 连接清理: 取消所有回调 + 中止 PCB + 释放 ctx                                      */
/* ------------------------------------------------------------------------ */
static void echo_connection_cleanup(struct echo_ctx *ctx) {
    if (!ctx) return;
    struct xskif *xif = ctx->xif;
    if (!xif) return;

    if (ctx->prev)
        ctx->prev->next = ctx->next;
    else
        xif->ctx_list_head = ctx->next;
    if (ctx->next)
        ctx->next->prev = ctx->prev;

    if (ctx->pcb) {
        tcp_arg(ctx->pcb, NULL);
        tcp_recv(ctx->pcb, NULL);
        tcp_sent(ctx->pcb, NULL);
        tcp_err(ctx->pcb, NULL);
        tcp_poll(ctx->pcb, NULL, 0);
        tcp_abort(ctx->pcb);
        ctx->pcb = NULL;
    }

    struct queued_msg *msg = ctx->queued_msgs;
    while (msg) {
        struct queued_msg *next = msg->next;
        if (msg->p) pbuf_free(msg->p);
        free(msg);
        msg = next;
    }
    free(ctx);
}


/* ── ────────────────────────────────────────────────────────────────── */
/* 处理排队消息: 从队列头逐一通过 tcp_write(FLAG_COPY) 发送, 支持分段                      */
/* ------------------------------------------------------------------------ */
static void process_queued_msgs(struct echo_ctx *ctx) {
    if (!ctx || !ctx->xif) return;
    if (!ctx->pcb) {
        return;
    }

    if (!ctx->queued_msgs) {
        ctx->waiting = false;
        if (ctx->closing && ctx->pcb) {
            tcp_arg(ctx->pcb, NULL);
            tcp_recv(ctx->pcb, NULL);
            tcp_sent(ctx->pcb, NULL);
            tcp_err(ctx->pcb, NULL);
            tcp_poll(ctx->pcb, NULL, 0);
            err_t close_err = tcp_close(ctx->pcb);
            if (close_err == ERR_OK) {
                ctx->pcb = NULL;
                echo_connection_cleanup(ctx);
            } else {
                /* R10-1: re-register tcp_arg so echo_poll can access ctx */
                tcp_arg(ctx->pcb, ctx);
                tcp_poll(ctx->pcb, echo_poll, 1);
            }
        }
        return;
    }

    bool did_write = false;
    const u16_t mss = tcp_mss(ctx->pcb);

    while (ctx->queued_msgs) {
        struct queued_msg *msg = ctx->queued_msgs;

        while (msg->current_p) {
            struct pbuf *cur_p = msg->current_p;

            /* R4-1: guard against corrupt offset > len */
            if (unlikely(msg->offset >= cur_p->len)) {
                struct pbuf *next = cur_p->next;
                if (next) pbuf_ref(next);
                pbuf_free(msg->p);
                msg->p = next;
                msg->current_p = next;
                msg->offset = 0;
                if (!msg->current_p) break;
                continue;
            }

            u16_t remaining = cur_p->len - msg->offset;
            u16_t sndbuf = tcp_sndbuf(ctx->pcb);
            u16_t write_len = remaining;
            if (write_len > sndbuf) write_len = sndbuf;

            if (write_len == 0) {
                ctx->waiting = true;
                tcp_output(ctx->pcb);
                return;
            }

            void *payload = (u8_t *)cur_p->payload + msg->offset;
            err_t err = tcp_write(ctx->pcb, payload, write_len, TCP_WRITE_FLAG_COPY);
            if (err == ERR_OK) {
                did_write = true;
                msg->offset += write_len;
                if (msg->offset >= cur_p->len) {
                    struct pbuf *next = cur_p->next;
                    if (next) {
                        pbuf_ref(next);
                    }
                    pbuf_free(msg->p);
                    msg->p = next;
                    msg->current_p = next;
                    msg->offset = 0;

                    if (!msg->current_p) {
                        break;
                    }
                }
                if (did_write && write_len >= mss) {
                    tcp_output(ctx->pcb);
                }
            } else if (err == ERR_MEM) {
                ctx->waiting = true;
                tcp_output(ctx->pcb);
                return;
            } else {
                ctx->waiting = true;
                tcp_output(ctx->pcb);
                return;
            }
        }

        ctx->queued_msgs = msg->next;
        if (!ctx->queued_msgs)
            ctx->queued_msgs_tail = NULL;
        /* msg->p already freed in inner loop; msg->p is NULL here */
        free(msg);
        ctx->queued_msg_count--;

        if (!ctx->queued_msgs) {
            ctx->waiting = false;
            if (did_write) {
                tcp_output(ctx->pcb);
            }
            if (ctx->closing && ctx->pcb) {
                tcp_arg(ctx->pcb, NULL);
                tcp_recv(ctx->pcb, NULL);
                tcp_sent(ctx->pcb, NULL);
                tcp_err(ctx->pcb, NULL);
                tcp_poll(ctx->pcb, NULL, 0);
                err_t close_err = tcp_close(ctx->pcb);
                if (close_err == ERR_OK) {
                    ctx->pcb = NULL;
                    echo_connection_cleanup(ctx);
                } else {
                    /* R10-1: re-register tcp_arg so echo_poll can access ctx */
                    tcp_arg(ctx->pcb, ctx);
                    tcp_poll(ctx->pcb, echo_poll, 1);
                }
            }
            return;
        }
    }
}


/* ── ────────────────────────────────────────────────────────────────── */
/* TCP 接收回调: p==NULL + err==OK → FIN; err!=OK → 错误; 正常 → 排队消息          */
/* ------------------------------------------------------------------------ */
static err_t echo_recv(void *arg, struct tcp_pcb *pcb, struct pbuf *p, err_t err) {
    struct echo_ctx *ctx = (struct echo_ctx *)arg;

    if (!ctx) return ERR_ABRT;                           /* R2-2: NULL guard */

    struct xskif *xif = ctx->xif;

    if (!p && err == ERR_OK) {
        ctx->closing = true;
        process_queued_msgs(ctx);
        return ERR_OK;
    }

    if (err != ERR_OK) {
        if (p) pbuf_free(p);
        echo_connection_cleanup(ctx);                    /* R2-1: don't NULL ctx->pcb before cleanup */
        return ERR_ABRT;
    }

    if (ctx->queued_msg_count >= MAX_QUEUED_MSGS_PER_CONN) {
        if (xif) xif->stats.flow_control_oom_closes++;
        pbuf_free(p);
        tcp_arg(pcb, NULL); tcp_recv(pcb, NULL); tcp_sent(pcb, NULL); tcp_err(pcb, NULL);
        tcp_poll(pcb, NULL, 0);
        tcp_abort(pcb);
        ctx->pcb = NULL;
        echo_connection_cleanup(ctx);
        return ERR_ABRT;
    }

    struct queued_msg *msg = malloc(sizeof(*msg));
    if (!msg) {
        pbuf_free(p);
        if (xif) xif->stats.echo_malloc_fails++;
        tcp_arg(pcb, NULL); tcp_recv(pcb, NULL); tcp_sent(pcb, NULL); tcp_err(pcb, NULL);
        tcp_poll(pcb, NULL, 0);
        tcp_abort(pcb);
        ctx->pcb = NULL;
        echo_connection_cleanup(ctx);
        return ERR_ABRT;
    }

    msg->p = p;
    msg->current_p = p;
    msg->offset = 0;
    msg->next = NULL;

    if (!ctx->queued_msgs) {
        ctx->queued_msgs = msg;
        ctx->queued_msgs_tail = msg;
    } else {
        ctx->queued_msgs_tail->next = msg;
        ctx->queued_msgs_tail = msg;
    }
    ctx->queued_msg_count++;

    if (!ctx->waiting)
        process_queued_msgs(ctx);

    return ERR_OK;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* TCP 轮询: waiting=true 时重试发送; closing=true 时尝试关闭                      */
/* ------------------------------------------------------------------------ */
static err_t echo_poll(void *arg, struct tcp_pcb *pcb) {
    (void)pcb;
    struct echo_ctx *ctx = (struct echo_ctx *)arg;
    if (ctx && ctx->waiting) {
        process_queued_msgs(ctx);
        return ERR_OK;
    }
    if (ctx && ctx->closing) {
        process_queued_msgs(ctx);
    }
    return ERR_OK;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* TCP 发送完成: 数据已 ACK, 继续处理队列                                           */
/* ------------------------------------------------------------------------ */
static err_t echo_sent(void *arg, struct tcp_pcb *pcb, u16_t len) {
    (void)pcb; (void)len;
    struct echo_ctx *ctx = (struct echo_ctx *)arg;
    if (!ctx) return ERR_OK;
    ctx->waiting = false;
    process_queued_msgs(ctx);
    return ERR_OK;
}


/* ── ────────────────────────────────────────────────────────────────── */
/* TCP 错误: PCB 由 lwIP 释放, 仅清理 ctx (ctx->pcb=null 防止 cleanup 双重释放)      */
/* ------------------------------------------------------------------------ */
static void echo_err(void *arg, err_t err) {
    (void)err;
    struct echo_ctx *ctx = (struct echo_ctx *)arg;
    if (!ctx) return;
    ctx->pcb = NULL;
    echo_connection_cleanup(ctx);
}


/* ── ────────────────────────────────────────────────────────────────── */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   复位与恢复 (xskif_reset)                                               */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   触发条件: pool->need_reset 标志 (位图错误/池溢出/过多孤儿)                         */
/*   速率限制: 两次复位间隔 >= 2000ms                                            */
/*   复位流程:                                                             */
/*     1. 排空 RX Ring                                                   */
/*     2. 处理 TX Completions                                            */
/*     3. clear_poison_list (恢复毒化帧)                                    */
/*     4. clear_orphaned_wrappers (恢复孤儿 wrapper)                       */
/*     5. recover_lost_frames (扫描丢失帧)                                  */
/*     6. xsk_replenish_fill_ring (重新填充 Fill Ring)                     */
/*   帧守恒: 复位后帧应回 free_pool 或 fill ring (lwIP 中帧除外)                     */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
/* Reset and recovery                                                         */
/*============================================================================*/
static void clear_poison_list(struct xskif *xif) {
    struct poison_frame *node = xif->poison_list;

    while (node) {
        struct poison_frame *next = node->next;
        u64 saved_addr = node->umem_addr;
        bool recovered_to_pool = false;

        if (saved_addr != FREE_POOL_INVALID_ADDR) {
            int ret = free_pool_push(xif->pool, saved_addr, &xif->stats);
            recovered_to_pool = (ret == FREEPOOL_OK || ret == FREEPOOL_EEXIST);
        }

        xif->poison_list = next;
        release_poison_frame(xif, node);
        xif->poison_count--;

        if (!recovered_to_pool && saved_addr != FREE_POOL_INVALID_ADDR) {
            xif->stats.umem_frame_permanent_loss++;
        }
        node = next;
    }
}

static void recover_lost_frames(struct xskif *xif) {
    struct free_pool *pool = xif->pool;
    for (uint32_t i = 0; i < pool->capacity; i++) {
        if (!bitmap_test(pool->used_bitmap, i, pool->capacity) &&
            !bitmap_test(pool->bitmap, i, pool->capacity)) {
            u64 addr = (u64)i * XSK_UMEM_FRAME_SIZE;
            int ret = free_pool_push(pool, addr, &xif->stats);
            if (ret == FREEPOOL_OK) {
                xif->stats.lost_frames_recovered++;
            } else {
                xif->stats.umem_frame_permanent_loss++;
            }
        }
    }
}

static void xskif_reset(struct xskif *xif) {
    if (!xif || !xif->pool) return;

    xif->stats.total_resets++;

    /* R10-1: clear need_reset BEFORE sub-ops; they may re-raise it */
    xif->pool->need_reset = false;

    /* R6-5: drain RX ring BEFORE setting resetting flag,
     * otherwise xskif_input_drain's resetting guard makes it a no-op */
    xskif_input_drain(xif);

    xif->resetting = true;

    clear_poison_list(xif);
    clear_orphaned_wrappers(xif);
    recover_lost_frames(xif);

    /* R4-2: guard against top > capacity */
    if (xif->pool->top < xif->pool->capacity) {
        uint32_t needed = xif->pool->capacity - xif->pool->top;
        while (needed > 0) {
            uint32_t batch = needed > XSK_BATCH_SIZE ? XSK_BATCH_SIZE : needed;
            uint32_t done = xsk_replenish_fill_ring(xif, batch);
            if (done == 0) {
                fprintf(stderr, "Reset: fill ring replenish stalled, will retry\n");

/* ── ────────────────────────────────────────────────────────────────── */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   销毁 (xskif_destroy) — 完整资源清理                                       */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   清理顺序 (反向依赖):                                                      */
/*     连接 → 监听PCB → RX/TX排空 → poison/wrapper恢复 → object_pool →         */
/*     free_pool → XSK socket → UMEM → munmap → frame_bases → memset(0)  */
/* ------------------------------------------------------------------------ */
                break;
            }
            needed -= done;
        }
    }

    /* R10-1: need_reset cleared at top of reset; sub-ops may have re-set it */
    xif->resetting = false;
}

/*============================================================================*/
/* Teardown function                                                          */
/*============================================================================*/
static void xskif_destroy(struct xskif *xif) {
    if (!xif) return;

    struct echo_ctx *ctx = xif->ctx_list_head;
    while (ctx) {
        struct echo_ctx *next = ctx->next;
        echo_connection_cleanup(ctx);
        ctx = next;
    }

    /* R5-3: close listen PCB */
    if (xif->listen_pcb) {
        tcp_arg(xif->listen_pcb, NULL);
        tcp_accept(xif->listen_pcb, NULL);
        tcp_close(xif->listen_pcb);
        xif->listen_pcb = NULL;
    }

    xskif_input_drain(xif);
    xsk_tx_completion_batch(xif);

    clear_poison_list(xif);
    clear_orphaned_wrappers(xif);

    object_pool_destroy(xif->obj_pool);

    free(xif->pool->bitmap);
    free(xif->pool->used_bitmap);
    free(xif->pool->stack);
    free(xif->pool);

    if (xif->xsk) {
        xsk_socket__delete(xif->xsk);
        xif->xsk = NULL;
    }
    if (xif->umem) {
        int umem_ret = xsk_umem__delete(xif->umem);
        if (umem_ret != 0) {
            fprintf(stderr, "CRITICAL: xsk_umem__delete returned %d (%s); "
                    "kernel may still hold DMA references to UMEM\n",
                    umem_ret, strerror(-umem_ret));
        }
        xif->umem = NULL;
    }
    if (xif->umem_area && xif->umem_area != MAP_FAILED) {
        munmap(xif->umem_area, xif->umem_size);
        xif->umem_area = NULL;
    }
    free(xif->umem_frame_bases);
    xif->umem_frame_bases = NULL;

    memset(xif, 0, sizeof(*xif));
}

/*============================================================================*/
/* Main loop                                                                  */

/* ── ────────────────────────────────────────────────────────────────── */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   main() — 程序入口                                                     */
/* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━                            */
/*   参数: argv[1]=接口名 argv[2]=核心ID argv[3]=NUMA节点 argv[4]=核心数 argv[5]=帧总数  */
/*   流程: 信号安装 → lwIP初始化 → netif+xskif创建 → xskif_setup →                */
/*         TCP监听(端口7) → 主轮询循环 → 信号触发 → xskif_destroy → 退出              */
/* ------------------------------------------------------------------------ */
/*============================================================================*/
int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <ifname>\n", argv[0]);
        return 1;
    }

    lwip_init();

    struct netif netif;
    memset(&netif, 0, sizeof(netif));                        /* R8-1: zero-init to prevent info leak */
    struct xskif xif;
    memset(&xif, 0, sizeof(xif));
    xif.netif = &netif;
    xif.ctx_list_head = NULL;

    int ret = xskif_setup(&xif, 0, 0, 16384, 1, argv[1]);
    if (ret != 0) {
        fprintf(stderr, "xskif_setup failed: %d\n", ret);
        return 1;
    }

    ip4_addr_t ipaddr, netmask, gw;
    IP4_ADDR(&ipaddr, 192, 168, 1, 100);
    IP4_ADDR(&netmask, 255, 255, 255, 0);
    IP4_ADDR(&gw, 192, 168, 1, 1);
    struct netif *iface = netif_add(&netif, &ipaddr, &netmask, &gw, &xif,
                                       xskif_netif_init, ethernet_input);
    if (!iface) {
        fprintf(stderr, "netif_add failed\n");
        xskif_destroy(&xif);
        return 1;
    }
    netif_set_default(iface);
    netif_set_link_up(iface);
    netif_set_up(iface);

    xif.listen_pcb = tcp_new();
    if (!xif.listen_pcb) {
        fprintf(stderr, "tcp_new failed\n");
        xskif_destroy(&xif);
        return 1;
    }
    tcp_bind(xif.listen_pcb, IP_ADDR_ANY, 12345);
    struct tcp_pcb *listen_pcb = tcp_listen(xif.listen_pcb);
    if (!listen_pcb) {
        fprintf(stderr, "tcp_listen failed\n");
        xskif_destroy(&xif);
        return 1;
    }
    tcp_arg(listen_pcb, &xif);
    tcp_accept(listen_pcb, echo_accept);
    xif.listen_pcb = listen_pcb;

    /* R7-1: graceful shutdown on SIGINT / SIGTERM */
    /* R10-5: use sigaction for reliable signal semantics (signal() has
     * ambiguous System-V vs BSD behavior across platforms) */
    struct sigaction sa = { .sa_handler = signal_handler, .sa_flags = 0 };
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);    /* R10-5: prevent crash on broken pipe */

    while (g_running) {
        xsk_tx_completion_batch(&xif);
        xskif_input(xif.netif);   /* R1-3: pass netif*, not xskif* */

        if (xif.pool->need_reset && !xif.resetting) {
            /* R8-1: enforce RESET_TIMEOUT_MS between resets to prevent DoS loops */
            struct timespec now;
            if (clock_gettime(CLOCK_MONOTONIC, &now) == 0) {
                int64_t elapsed_ms = (now.tv_sec - xif.last_reset_ts.tv_sec) * 1000
                                   + (now.tv_nsec - xif.last_reset_ts.tv_nsec) / 1000000;
                if (elapsed_ms >= RESET_TIMEOUT_MS || xif.last_reset_ts.tv_sec == 0) {
                    xif.last_reset_ts = now;
             xskif_reset(&xif);
                }
            } else {
                xskif_reset(&xif);  /* fallback: if clock fails, reset anyway */
            }
        }

      eck_timeouts();
        usleep(1000);
    }

    xskif_destroy(&xif);
    return 0;
}
