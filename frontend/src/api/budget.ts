/**
 * 多账号资源配额 API
 *
 * 对应后端 4 条专用路由：
 *
 *   GET    /api/v1/budget                         → 全局 + 跨账号聚合快照
 *   PUT    /api/v1/config/multi_account_budget    → 全局机器总上限 + VIP 权重
 *   PUT    /api/v1/config/vip_recommended         → VIP 推荐配置表（3 等级 × 4 字段）
 *   PUT    /api/v1/accounts/:uid/custom_config    → 单账号 auto/custom 配置
 *
 * 错误码语义：
 *   - 400 `account_not_available` — 客户端不存在（仅 `:uid` 路由）
 *   - 503 `readonly_mode`         — 全局只读模式拦截（仅 PUT 写接口）
 *   - 不返 `legacy_history_readonly`（配额接口无历史数据表）
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
    (response) => {
      // 后端统一返回 {code, message, data} 包裹，此处拆走外层包裹
      // 与 frontend/src/api/client.ts:191/240 进保持一致
      const body = response.data
      if (body && typeof body === 'object' && 'code' in body) {
        if (body.code !== 0) {
          return Promise.reject(new Error(body.message || '请求失败'))
        }
        return body.data
      }
      // 空返回（PUT/DELETE 类）原样返回
      return body
    },
    (error) => Promise.reject(error),
)

// ============================================================================
// DTO — 与 BudgetEvent 结构定义对齐
// ============================================================================

/**
 * 每账号配额详情（对应 `BudgetEvent::BudgetRecomputed.per_account` 元素）。
 *
 * 三水位线：
 *   MIN(=1)  ≤  base_*  ≤  vip_cap_*
 *
 * - `base_*`：压缩算法输出的当前账号"应得"值
 * - `vip_cap_*`：VIP 等级硬上限（百度风控线）
 * - `used_*`：当前已占用的 permit 数（实时使用量）
 */
export interface AccountBudget {
  uid: number
  vip_cap_download: number
  base_download: number
  vip_cap_upload: number
  base_upload: number
}

/**
 * 每账号实时使用量（对应 `BudgetEvent::UsageSnapshot.per_account` 元素）。
 */
export interface AccountUsage {
  uid: number
  used_download: number
  used_upload: number
}

/**
 * 合并后的每账号配额视图（前端 store 自行拼接 budget + usage 得到）。
 *
 * 用于 `BudgetPanel.vue` 三段进度条渲染。
 */
export interface AccountBudgetView {
  uid: number
  vip_cap_download: number
  base_download: number
  used_download: number
  vip_cap_upload: number
  base_upload: number
  used_upload: number
}

/**
 * `GET /api/v1/budget` 响应载荷。
 *
 * 后端 `BudgetSnapshotDto`（`server/handlers/budget.rs:39`）返回合并形态 —
 * `per_account` 中每个元素一次性包含 base / vip_cap / used 各 6 个字段，
 * 与 WS 事件 `BudgetEvent::BudgetRecomputed.per_account` 完全同形。
 *
 * 单账号场景：`per_account.length === 1`。
 */
export interface BudgetSnapshot {
  /** 全局机器总上限（下载） — 当前生效值（自动模式 = `cpu_cores × 8`，手动模式 = 用户设定） */
  machine_budget_download: number
  /** 全局机器总上限（上传） */
  machine_budget_upload: number
  /** 每账号 base/vip_cap/used 三者合并后的完整视图（与 WsAccountBudget 一致） */
  per_account: AccountBudgetView[]
}

// ----------------------------------------------------------------------------
// 全局配置 DTO — `PUT /config/multi_account_budget`
// ----------------------------------------------------------------------------

/**
 * `app.toml [multi_account_budget]` 段的前端形态。
 *
 * `*_machine_budget` 接受两种语义：
 *   - `"auto"` 字符串：使用 `cpu_cores × 8` 自动值
 *   - 数字：手动指定上限（代码强制 `M ≥ N × MIN`）
 */
export interface MultiAccountBudgetConfig {
  download_machine_budget: 'auto' | number
  upload_machine_budget: 'auto' | number
  weight_normal: number
  weight_vip: number
  weight_svip: number
}

// ----------------------------------------------------------------------------
// VIP 推荐配置 DTO — `PUT /config/vip_recommended`
// ----------------------------------------------------------------------------

/**
 * 单一 VIP 等级的推荐配置（共 3 档：normal / vip / svip）。
 *
 * 对应 `app.toml [multi_account_vip_recommended.<level>]` 子表。
 */
export interface VipLevelRecommended {
  threads: number
  chunk_size_mb: number
  max_concurrent_tasks: number
}

export interface VipRecommendedConfig {
  normal: VipLevelRecommended
  vip: VipLevelRecommended
  svip: VipLevelRecommended
}

// ----------------------------------------------------------------------------
// 账号 custom_config DTO — `PUT /accounts/:uid/custom_config`
// ----------------------------------------------------------------------------

/**
 * 单账号下载侧自定义配置。
 *
 * 当 `auto_apply_recommended=true` 时，此处 `*` 字段被忽略。
 *
 * `chunk_size_mb` 为可选 — 该字段不驱动运行时
 * 分片大小（由文件大小 + VIP 等级自适应计算）。前端保存时会从 payload
 * 中剥离；后端仍接受字段保持向后兼容（旧客户端继续提交时不报错）。
 */
export interface AccountDownloadConfig {
  max_global_threads: number
  chunk_size_mb?: number
  max_concurrent_tasks: number
  max_retries: number
}

/**
 * 单账号上传侧自定义配置。
 *
 * `chunk_size_mb` 为可选（同上）。
 */
export interface AccountUploadConfig {
  max_global_threads: number
  chunk_size_mb?: number
  max_concurrent_tasks: number
  max_retries: number
  skip_hidden_files: boolean
}

/**
 * 单账号完整自定义配置（持久化到 `accounts.json` 的 `UserAuth.custom_config`）。
 */
export interface AccountCustomConfig {
  /** 是否自动按 VIP 等级应用推荐配置（默认 true） */
  auto_apply_recommended: boolean
  download: AccountDownloadConfig
  upload: AccountUploadConfig
}

// ============================================================================
// API
// ============================================================================

/**
 * GET /api/v1/budget — 获取全局 + 跨账号配额快照。
 *
 * - 不接受 `?uid=` 参数（前端按 uid 自行筛）
 * - 单账号场景仅返回该账号 + 全局 M
 * - 后端实现：`server/handlers/budget.rs::get_budget` → `state.budget_scheduler.snapshot()`
 */
export async function getBudgetSnapshot(): Promise<BudgetSnapshot> {
  return apiClient.get('/budget')
}

/**
 * PUT /api/v1/config/multi_account_budget — 更新机器总上限 + VIP 权重。
 *
 * 后端流程：
 *   1. 校验 + 落盘 `app.toml [multi_account_budget]`
 *   2. 触发 `budget_scheduler.recompute_budget()`
 *   3. WS 广播 `BudgetEvent::BudgetRecomputed`
 *
 * 写接口受 `readonly_mode` 拦截 → 503。
 */
export async function updateMultiAccountBudget(
    cfg: MultiAccountBudgetConfig,
): Promise<void> {
  await apiClient.put('/config/multi_account_budget', cfg)
}

/**
 * PUT /api/v1/config/vip_recommended — 更新 VIP 推荐配置表。
 *
 * 后端流程：
 *   1. 校验 + 落盘 `app.toml [multi_account_vip_recommended]`
 *   2. 对所有 `auto_apply_recommended=true` 账号触发 `recompute_budget()`
 *   3. WS 广播 `BudgetEvent::BudgetRecomputed`
 *
 * 写接口受 `readonly_mode` 拦截 → 503。
 */
export async function updateVipRecommended(
    cfg: VipRecommendedConfig,
): Promise<void> {
  await apiClient.put('/config/vip_recommended', cfg)
}

/**
 * PUT /api/v1/accounts/:uid/custom_config — 更新指定账号的 custom_config。
 *
 * - 不接受 `?uid=`（已通过路径 `:uid` 唯一定位）
 * - 客户端不存在 → 400 `account_not_available`
 * - 写接口受 `readonly_mode` 拦截 → 503
 *
 * 保存后**仅对该账号**触发 `recompute_budget()` + 推送 `BudgetEvent::BudgetRecomputed`。
 */
export async function updateAccountCustomConfig(
    uid: number,
    cfg: AccountCustomConfig,
): Promise<void> {
  await apiClient.put(`/accounts/${uid}/custom_config`, cfg)
}
