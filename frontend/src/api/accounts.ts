/**
 * 多账号管理 API
 *
 * 对应后端 `backend/src/server/handlers/accounts.rs`：
 *   - GET    /api/v1/accounts/list      → 列出所有账号 + active_uid
 *   - POST   /api/v1/accounts/switch    → 切换活跃账号
 *   - DELETE /api/v1/accounts/:uid      → 强制删除账号
 *
 * `uid` 在 HTTP 线缆上始终为 `number`（后端 `u64`）；前端运行态保持 `number | null`，
 * `null` 表示无活跃账号（已删除最后一个 / 未登录）。
 */

import axios from 'axios'

// 与 webAuth store 一致
const WEB_AUTH_ACCESS_TOKEN_KEY = 'web_auth_access_token'

const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 10000,
  headers: { 'Content-Type': 'application/json' },
})

apiClient.interceptors.request.use(
    (config) => {
      const token = localStorage.getItem(WEB_AUTH_ACCESS_TOKEN_KEY)
      if (token) config.headers.Authorization = `Bearer ${token}`
      return config
    },
    (error) => Promise.reject(error),
)

apiClient.interceptors.response.use(
    (response) => response.data,
    (error) => Promise.reject(error),
)

// ============================================================================
// DTO
// ============================================================================

export interface AccountDownloadConfig {
  max_global_threads: number
  /** 可选 — 不驱动运行时分片大小（自适应计算） */
  chunk_size_mb?: number
  max_concurrent_tasks: number
  max_retries: number
}

export interface AccountUploadConfig {
  max_global_threads: number
  /** 可选 — 不驱动运行时分片大小（自适应计算） */
  chunk_size_mb?: number
  max_concurrent_tasks: number
  max_retries: number
  skip_hidden_files: boolean
}

export interface AccountCustomConfigSummary {
  /** 是否自动按 VIP 等级应用推荐配置（默认 true） */
  auto_apply_recommended: boolean
  download: AccountDownloadConfig
  upload: AccountUploadConfig
}

export interface AccountSummary {
  uid: number
  username: string
  nickname: string | null
  avatar_url: string | null
  vip_type: number | null
  is_active: boolean
  /**
   * 持久化的账号自定义配置。
   *
   * 用于设置页 `BudgetPanel.vue` 初始化每账号配置表单 — 必须用真实持久化值，
   * 否则点保存会用 VIP 推荐默认值覆盖旧账号 `auto_apply_recommended=false` 的
   * 自定义配置。
   *
   * 兼容性：旧后端不返回此字段，前端做 `?? null` 兜底，落到默认值
   * 表单仍可用（与本字段未引入前的行为一致）。
   */
  custom_config?: AccountCustomConfigSummary
}

export interface ListAccountsResponse {
  accounts: AccountSummary[]
  /** 后端 Option<u64> + #[serde(skip_serializing_if = "Option::is_none")]，无活跃账号时字段缺失 */
  active_uid?: number | null
}

export interface SwitchAccountResponse {
  active_uid: number
}

export interface DeleteAccountResponse {
  deleted_uid: number
  /** `null` = 删除的是最后一个账号 */
  new_active_uid: number | null
}

export interface ApiResponse<T> {
  code: number
  message: string
  data?: T
}

// ============================================================================
// API
// ============================================================================

/**
 * GET /api/v1/accounts/list
 *
 * 注意：后端返回结构是 `ListAccountsResponse` 直接序列化（不包 ApiResponse 外层），
 * 但其它 handler 多数包了 ApiResponse；此 handler v3.x 经核对返回 200 直接 JSON。
 * 为兼容两种形式，做一次 fallback 解构。
 */
export async function listAccounts(): Promise<ListAccountsResponse> {
  const raw = (await apiClient.get('/accounts/list')) as
      | ListAccountsResponse
      | ApiResponse<ListAccountsResponse>
  if ('accounts' in (raw as ListAccountsResponse) && Array.isArray((raw as ListAccountsResponse).accounts)) {
    return raw as ListAccountsResponse
  }
  const wrapped = raw as ApiResponse<ListAccountsResponse>
  if (wrapped.code !== 0 || !wrapped.data) {
    throw new Error(wrapped.message || '获取账号列表失败')
  }
  return wrapped.data
}

/**
 * POST /api/v1/accounts/switch
 *
 * 切换活跃账号。`uid` 必须存在，否则返回 400 `account_not_available`。
 */
export async function switchAccount(uid: number): Promise<SwitchAccountResponse> {
  const raw = (await apiClient.post('/accounts/switch', { uid })) as
      | SwitchAccountResponse
      | ApiResponse<SwitchAccountResponse>
  if ('active_uid' in (raw as SwitchAccountResponse)) {
    return raw as SwitchAccountResponse
  }
  const wrapped = raw as ApiResponse<SwitchAccountResponse>
  if (wrapped.code !== 0 || !wrapped.data) {
    throw new Error(wrapped.message || '切换账号失败')
  }
  return wrapped.data
}

/**
 * DELETE /api/v1/accounts/:uid?force=true
 *
 * 删除账号（默认强制模式 — 关闭运行中任务 + 清理持久化）。
 * 删除最后一个账号后 `new_active_uid = null`，调用方应跳转到 /login。
 */
export async function deleteAccount(uid: number, force = true): Promise<DeleteAccountResponse> {
  const raw = (await apiClient.delete(`/accounts/${uid}`, {
    params: { force },
  })) as DeleteAccountResponse | ApiResponse<DeleteAccountResponse>
  if ('deleted_uid' in (raw as DeleteAccountResponse)) {
    return raw as DeleteAccountResponse
  }
  const wrapped = raw as ApiResponse<DeleteAccountResponse>
  if (wrapped.code !== 0 || !wrapped.data) {
    throw new Error(wrapped.message || '删除账号失败')
  }
  return wrapped.data
}
