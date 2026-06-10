import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { generateQRCode as apiGenerateQRCode, getQRCodeStatus, getCurrentUser, logout as apiLogout, cookieLogin as apiCookieLogin } from '@/api/auth'
import type { QRCode, UserAuth, CookieLoginResult } from '@/api/auth'
import {
  listAccounts as apiListAccounts,
  switchAccount as apiSwitchAccount,
  deleteAccount as apiDeleteAccount,
} from '@/api/accounts'
import type { AccountSummary } from '@/api/accounts'

/**
 * 多账号 auth store。
 *
 * 保留向后兼容字段：`user`/`isLoggedIn`/`username`/`avatar`/`qrcode`/`isPolling`
 * 以及方法 `generateQRCode`/`startPolling`/`stopPolling`/`fetchUserInfo`/
 * `loginWithCookies`/`logout`，原有调用点（LoginView/MainLayout/router）无需改动。
 *
 * 新增多账号语义：
 *   - `accounts`     全账号脱敏摘要列表
 *   - `activeUid`    当前活跃 UID（`null` = 无活跃账号）
 *   - `activeAccount`  根据 activeUid 解析得到的当前账号摘要
 *   - `readonlyMode` 服务端只读模式（来自 Snapshot），UI 三重提示用
 *   - `fetchAccountList()` / `switchAccount(uid)` / `deleteAccount(uid)`
 *   - `applySnapshot(snapshot)` 处理 WsServerSnapshot 多账号字段
 *   - `applyAccountEvent(event)` 处理 AccountEvent (switched / list_changed)
 *
 * 与 backend 契约：`uid` 在 HTTP/WS 线缆上始终为 `number`（u64）；前端态保持
 * `number | null`，`null` 表示无活跃账号 / 未登录。
 */
export const useAuthStore = defineStore('auth', () => {
  // ───────── 兼容字段（旧版） ─────────
  const user = ref<UserAuth | null>(null)
  const qrcode = ref<QRCode | null>(null)
  const isPolling = ref(false)
  const pollingTimer = ref<number | null>(null)

  // ───────── 多账号字段（新增） ─────────
  /** 全账号摘要 */
  const accounts = ref<AccountSummary[]>([])
  /** 当前活跃账号 UID（`null` = 无活跃账号 / 未登录） */
  const activeUid = ref<number | null>(null)
  /** 服务端只读模式（来自 Snapshot 或 Switched 后重拉） */
  const readonlyMode = ref(false)
  /** 账号列表是否已经至少拉取过一次 */
  const accountsLoaded = ref(false)

  // ───────── 计算属性 ─────────
  const isLoggedIn = computed(() => {
    // 任一信号成立即算登录：旧 user 字段存在 / 新 activeUid 存在 / accounts 非空
    return user.value !== null || activeUid.value !== null
  })
  const activeAccount = computed<AccountSummary | null>(() => {
    if (activeUid.value === null) return null
    return accounts.value.find((a) => a.uid === activeUid.value) ?? null
  })
  const username = computed(() => {
    // 优先用 activeAccount（多账号），降级到 user（兼容旧路径）
    if (activeAccount.value) {
      return activeAccount.value.nickname || activeAccount.value.username || ''
    }
    return user.value?.nickname || user.value?.username || ''
  })
  const avatar = computed(() => {
    if (activeAccount.value) return activeAccount.value.avatar_url || ''
    return user.value?.avatar_url || ''
  })
  /** 是否有多账号（true 时 UI 应显示账号切换器 / 过滤器） */
  const hasMultipleAccounts = computed(() => accounts.value.length > 1)

  // 生成二维码
  async function generateQRCode(): Promise<QRCode> {
    try {
      qrcode.value = await apiGenerateQRCode()
      return qrcode.value
    } catch (error) {
      console.error('生成二维码失败:', error)
      throw error
    }
  }

  // 开始轮询二维码状态
  function startPolling(
      onSuccess: () => void,
      onError: (error: any) => void,
      onScanned?: () => void,
      addMode = false
  ) {
    if (isPolling.value || !qrcode.value) return

    isPolling.value = true

    const poll = async () => {
      try {
        if (!qrcode.value) {
          stopPolling()
          return
        }

        const status = await getQRCodeStatus(qrcode.value.sign, addMode)

        switch (status.status) {
          case 'success':
            // 登录成功
            stopPolling()
            try {
              await fetchUserInfo()
            } catch (error) {
              console.error('获取用户信息失败，但登录成功:', error)
              // 即使获取失败，也设置基本用户信息
              if (status.user) {
                user.value = status.user
              }
            }
            onSuccess()
            break
          case 'expired':
            // 二维码过期
            stopPolling()
            onError(new Error('二维码已过期'))
            break
          case 'failed':
            // 登录失败
            stopPolling()
            onError(new Error(status.reason || '登录失败'))
            break
          case 'scanned':
            // 已扫码，等待确认
            console.log('已扫码，等待确认...')
            onScanned?.()
            break
          case 'waiting':
            // 等待扫码
            console.log('等待扫码...')
            break
        }
      } catch (error) {
        console.error('轮询失败:', error)
        // 继续轮询，不停止
      }
    }

    // 开始轮询
    poll()
    pollingTimer.value = window.setInterval(poll, 3000)
  }

  // 停止轮询
  function stopPolling() {
    if (pollingTimer.value) {
      clearInterval(pollingTimer.value)
      pollingTimer.value = null
    }
    isPolling.value = false
  }

  // 获取用户信息
  async function fetchUserInfo() {
    try {
      user.value = await getCurrentUser()
    } catch (error) {
      console.error('获取用户信息失败:', error)
      throw error
    }
  }

  // Cookie 登录
  async function loginWithCookies(cookies: string): Promise<CookieLoginResult> {
    try {
      const result = await apiCookieLogin(cookies)
      user.value = result.user
      return result
    } catch (error) {
      console.error('Cookie 登录失败:', error)
      throw error
    }
  }

  // 登出
  async function logout() {
    try {
      await apiLogout()
      user.value = null
      qrcode.value = null
      // ───── 多账号字段也一并清空 ─────
      accounts.value = []
      activeUid.value = null
      readonlyMode.value = false
      accountsLoaded.value = false
      stopPolling()
    } catch (error) {
      console.error('登出失败:', error)
      throw error
    }
  }

  // ============================================================================
  // 多账号方法
  // ============================================================================

  /**
   * 拉取全账号列表 + active_uid（HTTP）。
   *
   * 通常只在以下时机调用：
   *   1. 应用启动（main.ts 或 router beforeEach 首次命中 `requiresAuth`）
   *   2. 收到 `AccountEvent::ListChanged` 但前端 `accounts` 为空（兜底）
   *   3. 用户在 SettingsView 手动刷新
   *
   * `AccountEvent::ListChanged` 的 payload 已自带最新 accounts，正常路径
   * 应直接调用 `applyAccountEvent()` 而非 `fetchAccountList()`。
   */
  async function fetchAccountList(): Promise<void> {
    try {
      const resp = await apiListAccounts()
      accounts.value = resp.accounts
      activeUid.value = resp.active_uid ?? null
      accountsLoaded.value = true
    } catch (error) {
      console.error('拉取账号列表失败:', error)
      throw error
    }
  }

  /**
   * 切换活跃账号（HTTP）。
   *
   * 写入成功后后端会发射 `AccountEvent::Switched` 与 `BudgetEvent::BudgetRecomputed`，
   * 前端通过 WS 监听同步剩余 store 状态；此处除本地立即更新 `activeUid` 外，
   * 还会**主动派发** `multi-account:active-changed` 自定义事件，作为 WS 路径的
   * 兜底（首次切换时 WS 可能尚未订阅完成，依赖 WS 回环会丢事件）。
   *
   * App.vue 的 WS Switched handler 中通过 `lastBroadcastedActiveUid` 做幂等判
   * 重，避免同一次切换触发两次刷新。
   */
  async function switchAccount(uid: number): Promise<void> {
    const resp = await apiSwitchAccount(uid)
    activeUid.value = resp.active_uid
    // 同步 accounts 中各项 is_active 字段
    accounts.value = accounts.value.map((a) => ({
      ...a,
      is_active: a.uid === resp.active_uid,
    }))
    // 主动派发跨视图刷新事件（WS 路径兜底；与 App.vue 内 WS handler 幂等）
    if (typeof window !== 'undefined') {
      window.dispatchEvent(
          new CustomEvent('multi-account:active-changed', {
            detail: { newActiveUid: resp.active_uid },
          }),
      )
    }
  }

  /**
   * 强制删除账号（HTTP，默认 force=true）。
   *
   * 删除最后一个账号时 `new_active_uid = null`，调用方应检测后跳转 `/login`。
   */
  async function deleteAccount(uid: number, force = true): Promise<number | null> {
    const resp = await apiDeleteAccount(uid, force)
    accounts.value = accounts.value.filter((a) => a.uid !== resp.deleted_uid)
    const previousActive = activeUid.value
    activeUid.value = resp.new_active_uid
    // 若被删除的账号刚好是兼容字段 user，清空之
    if (user.value && user.value.uid === resp.deleted_uid) {
      user.value = null
    }
    // 删除导致 active_uid 变化时也派发刷新事件（与 switchAccount 一致兜底）
    if (typeof window !== 'undefined' && previousActive !== resp.new_active_uid) {
      window.dispatchEvent(
          new CustomEvent('multi-account:active-changed', {
            detail: { newActiveUid: resp.new_active_uid },
          }),
      )
    }
    return resp.new_active_uid
  }

  /**
   * 处理 WsServerSnapshot 中的多账号字段。
   *
   * 后端字段：accounts / active_uid / readonly_mode 均为可选；
   * 不存在时 store 保持当前值（避免清空后又被旧消息覆盖）。
   */
  function applySnapshot(snapshot: {
    accounts?: AccountSummary[]
    active_uid?: number | null
    readonly_mode?: boolean
  }): void {
    if (Array.isArray(snapshot.accounts)) {
      accounts.value = snapshot.accounts
      accountsLoaded.value = true
    }
    if (snapshot.active_uid !== undefined) {
      activeUid.value = snapshot.active_uid ?? null
    }
    if (typeof snapshot.readonly_mode === 'boolean') {
      readonlyMode.value = snapshot.readonly_mode
    }
  }

  /**
   * 处理 AccountEvent（来自 WS）。
   *
   * - `switched`     更新 `activeUid` + accounts.is_active 同步
   * - `list_changed` 整表替换 accounts + active_uid
   */
  function applyAccountEvent(event:
                                 | { event_type: 'switched'; new_active_uid: number | null }
                                 | { event_type: 'list_changed'; accounts: AccountSummary[]; active_uid: number | null }
  ): void {
    if (event.event_type === 'switched') {
      activeUid.value = event.new_active_uid
      accounts.value = accounts.value.map((a) => ({
        ...a,
        is_active: a.uid === event.new_active_uid,
      }))
    } else if (event.event_type === 'list_changed') {
      accounts.value = event.accounts
      activeUid.value = event.active_uid
      accountsLoaded.value = true
    }
  }

  return {
    // ───── 兼容字段 ─────
    user,
    qrcode,
    isPolling,
    // ───── 多账号状态 ─────
    accounts,
    activeUid,
    readonlyMode,
    accountsLoaded,
    // ───── 计算属性 ─────
    isLoggedIn,
    username,
    avatar,
    activeAccount,
    hasMultipleAccounts,
    // ───── 兼容方法 ─────
    generateQRCode,
    startPolling,
    stopPolling,
    fetchUserInfo,
    loginWithCookies,
    logout,
    // ───── 多账号方法 ─────
    fetchAccountList,
    switchAccount,
    deleteAccount,
    applySnapshot,
    applyAccountEvent,
  }
})
