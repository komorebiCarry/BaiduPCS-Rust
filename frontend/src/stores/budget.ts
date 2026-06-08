/**
 * 多账号资源配额 Pinia Store
 *
 * 状态来源：
 *   1. HTTP 主动拉取：`GET /api/v1/budget` → `getBudgetSnapshot()`（首屏 + 手动刷新）
 *   2. WebSocket 推送（仅 BudgetPanel 订阅）：
 *      - `BudgetEvent::BudgetRecomputed`  → 全量替换 machineBudget + perAccountBudget
 *      - `BudgetEvent::UsageSnapshot`     → 仅替换 perAccountUsage
 *
 * 订阅语义：
 *   - 由 `BudgetPanel.vue` 在 onMounted 时显式订阅，onUnmounted 时取消
 *   - 不会因 logout 自动取消（与 AccountEvent 同机制）
 *
 * 与 `BudgetEvent` 形态一一对应；视图层基于派生 computed `accountViews` 渲染三段进度条。
 */

import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import {
  getBudgetSnapshot,
  type AccountBudget,
  type AccountBudgetView,
  type AccountUsage,
  type BudgetSnapshot,
} from '@/api/budget'
import type {
  BudgetEvent,
  BudgetEventRecomputed,
  BudgetEventUsageSnapshot,
} from '@/types/events'

export const useBudgetStore = defineStore('budget', () => {
  // ==========================================================================
  // 状态
  // ==========================================================================

  /** 全局机器总上限（下载） — 当前生效值 */
  const machineBudgetDownload = ref<number>(0)
  /** 全局机器总上限（上传） */
  const machineBudgetUpload = ref<number>(0)

  /** 每账号 base/vip_cap 配额（事件驱动重算） */
  const perAccountBudget = ref<AccountBudget[]>([])
  /** 每账号实时 used（每 1s 推送） */
  const perAccountUsage = ref<AccountUsage[]>([])

  /** 是否已成功加载至少一次（用于"加载中"占位与"再次拉取" 区分） */
  const loaded = ref(false)
  /** 进行中的 fetch；并发去抖 */
  const loading = ref(false)
  /** 最近一次错误信息（HTTP 拉取） */
  const lastError = ref<string | null>(null)

  // ==========================================================================
  // 派生
  // ==========================================================================

  /**
   * 合并 budget + usage 为视图数据（按 uid join）。
   *
   * 缺失 usage（事件未来过 / 单账号空闲场景） → used_* = 0。
   */
  const accountViews = computed<AccountBudgetView[]>(() => {
    const usageMap = new Map<number, AccountUsage>()
    for (const u of perAccountUsage.value) usageMap.set(u.uid, u)

    return perAccountBudget.value.map((b) => {
      const u = usageMap.get(b.uid)
      return {
        uid: b.uid,
        vip_cap_download: b.vip_cap_download,
        base_download: b.base_download,
        used_download: u?.used_download ?? 0,
        vip_cap_upload: b.vip_cap_upload,
        base_upload: b.base_upload,
        used_upload: u?.used_upload ?? 0,
      }
    })
  })

  /** 是否多账号场景（决定 UsageSnapshot 是否会推送） */
  const isMultiAccount = computed(() => perAccountBudget.value.length > 1)

  // ==========================================================================
  // Action：HTTP 拉取
  // ==========================================================================

  /**
   * 从后端拉取一份完整 BudgetSnapshot 并替换本地状态。
   *
   * 用于：
   *   - BudgetPanel onMounted 首屏
   *   - 用户点击"重新计算"按钮
   *   - 收到 BudgetRecomputed 事件后做一次确认拉取（可选）
   */
  async function fetchSnapshot(): Promise<void> {
    if (loading.value) return
    loading.value = true
    lastError.value = null
    try {
      const snap: BudgetSnapshot = await getBudgetSnapshot()
      machineBudgetDownload.value = snap.machine_budget_download
      machineBudgetUpload.value = snap.machine_budget_upload
      // 后端 per_account 是合并形态（同时含 base/vip_cap/used 6 字段），
      // 前端 store 维护两份独立切片以匹配 BudgetRecomputed / UsageSnapshot 各自的更新粒度
      const perAccount = snap.per_account ?? []
      perAccountBudget.value = perAccount.map((p) => ({
        uid: p.uid,
        vip_cap_download: p.vip_cap_download,
        base_download: p.base_download,
        vip_cap_upload: p.vip_cap_upload,
        base_upload: p.base_upload,
      }))
      perAccountUsage.value = perAccount.map((p) => ({
        uid: p.uid,
        used_download: p.used_download,
        used_upload: p.used_upload,
      }))
      loaded.value = true
    } catch (e: unknown) {
      const err = e as { message?: string; response?: { data?: { message?: string } } }
      lastError.value =
          err?.response?.data?.message || err?.message || '获取配额快照失败'
      throw e
    } finally {
      loading.value = false
    }
  }

  // ==========================================================================
  // Action：WS 事件 patch
  // ==========================================================================

  /**
   * 应用 `BudgetEvent::BudgetRecomputed` — 全量替换 machineBudget + perAccountBudget。
   *
   * **不**触碰 perAccountUsage。
   */
  function applyRecomputed(event: BudgetEventRecomputed): void {
    machineBudgetDownload.value = event.machine_budget_download
    machineBudgetUpload.value = event.machine_budget_upload
    perAccountBudget.value = event.per_account.map((p) => ({
      uid: p.uid,
      vip_cap_download: p.vip_cap_download,
      base_download: p.base_download,
      vip_cap_upload: p.vip_cap_upload,
      base_upload: p.base_upload,
    }))
    loaded.value = true
  }

  /**
   * 应用 `BudgetEvent::UsageSnapshot` — 仅替换 perAccountUsage。
   *
   * **不**触发 base/vip_cap 重渲染。
   */
  function applyUsageSnapshot(event: BudgetEventUsageSnapshot): void {
    perAccountUsage.value = event.per_account.map((u) => ({
      uid: u.uid,
      used_download: u.used_download,
      used_upload: u.used_upload,
    }))
  }

  /**
   * 统一事件入口：由 `utils/websocket.ts` `case 'budget':` 调用。
   */
  function applyBudgetEvent(event: BudgetEvent): void {
    if (event.type === 'budget_recomputed') {
      applyRecomputed(event)
    } else if (event.type === 'usage_snapshot') {
      applyUsageSnapshot(event)
    }
  }

  // ==========================================================================
  // Action：清空（用于 logout / 路由离开 BudgetPanel）
  // ==========================================================================

  function $reset(): void {
    machineBudgetDownload.value = 0
    machineBudgetUpload.value = 0
    perAccountBudget.value = []
    perAccountUsage.value = []
    loaded.value = false
    loading.value = false
    lastError.value = null
  }

  return {
    // state
    machineBudgetDownload,
    machineBudgetUpload,
    perAccountBudget,
    perAccountUsage,
    loaded,
    loading,
    lastError,
    // derived
    accountViews,
    isMultiAccount,
    // actions
    fetchSnapshot,
    applyRecomputed,
    applyUsageSnapshot,
    applyBudgetEvent,
    $reset,
  }
})
