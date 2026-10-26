/**
 * 分享同步 API 客户端
 */

import { rawApiClient } from './client'

// ==================== 类型定义 ====================

export type ConflictStrategy = 'overwrite' | 'versioned' | 'skip'
export type PollMode = 'disabled' | 'interval' | 'scheduled' | 'cron'
export type TargetKind = 'netdisk' | 'local' | 'netdisk_and_local'

/**
 * 本地同步模式（与后端 `LocalSyncMode` 对齐，serde snake_case）：
 * - `share_direct`：分享直下 —— 转存到临时目录 → 下载本地 → 清理（网盘不留存，默认/兼容老订阅）。
 * - `transfer_and_download`：转存并下载 —— 转存到网盘目标 remote_path 保留 → 再下载本地（需同订阅有网盘目标）。
 */
export type LocalSyncMode = 'share_direct' | 'transfer_and_download'

export interface NetdiskTarget {
  kind: 'netdisk'
  remote_path: string
  save_fs_id: number
  conflict_strategy?: ConflictStrategy | null
}

export interface LocalTarget {
  kind: 'local'
  local_path: string
  conflict_strategy?: ConflictStrategy | null
  /** 本地同步模式；缺省时后端按 `share_direct` 反序列化（兼容老订阅） */
  mode?: LocalSyncMode
}

export type SyncTarget = NetdiskTarget | LocalTarget

export interface PollConfig {
  enabled: boolean
  mode: PollMode
  interval_secs: number
  schedule_hour?: number | null
  schedule_minute?: number | null
  /** cron 表达式（mode=cron 时必填）：分 时 日 月 星期，按服务器时区 */
  cron_expr?: string | null
}

export interface CronPreview {
  /** 语法合法且满足最小轮询间隔，可直接保存 */
  valid: boolean
  /** 中文描述（语法合法即返回） */
  description?: string
  error?: string
  /** 接下来的触发时刻（服务器时区，YYYY-MM-DD HH:MM） */
  next_runs: string[]
  /** 服务器时区相对 UTC 的偏移分钟数 */
  utc_offset_minutes: number
}

export interface ShareSubscription {
  id: string
  name: string
  share_url: string
  password?: string | null
  include_paths: string[]
  exclude_patterns: string[]
  /** 排除正则（Rust 语法，整条路径匹配）；旧响应可能缺该字段 */
  exclude_regex_patterns?: string[]
  /** 精确排除的目录路径（相对分享根）；命中的目录连同其中全部内容一起排除 */
  exclude_paths: string[]
  targets: SyncTarget[]
  conflict_strategy: ConflictStrategy
  delete_missing: boolean
  poll_config: PollConfig
  enabled: boolean
  /** 连续「链接确定性失效」失败次数（达阈值后自动暂停） */
  consecutive_link_failures?: number
  /** 链接已确定性失效并被自动暂停轮询，等用户更新链接后「恢复」 */
  link_invalid?: boolean
  /** 链接失效的可读原因 */
  link_invalid_reason?: string | null
  created_at: string
  updated_at: string
  /** 订阅所属账号 uid；前端据此渲染账号徽章 / 按账号过滤 */
  owner_uid: number
}

export interface CreateShareSubscriptionRequest {
  name: string
  share_url: string
  password?: string | null
  include_paths?: string[]
  exclude_patterns?: string[]
  /** 排除正则（Rust 语法，整条路径匹配） */
  exclude_regex_patterns?: string[]
  /** 精确排除的目录路径（相对分享根） */
  exclude_paths?: string[]
  targets: SyncTarget[]
  conflict_strategy?: ConflictStrategy
  delete_missing?: boolean
  poll_config?: PollConfig
  /** 显式指定归属账号；缺省由后端回退到当前活跃账号 */
  owner_uid?: number
}

export interface UpdateShareSubscriptionRequest {
  name?: string
  share_url?: string
  password?: string | null
  include_paths?: string[]
  exclude_patterns?: string[]
  /** 缺省 = 保留已存正则；显式 [] = 清空 */
  exclude_regex_patterns?: string[]
  /** 精确排除的目录路径（相对分享根） */
  exclude_paths?: string[]
  targets?: SyncTarget[]
  conflict_strategy?: ConflictStrategy
  delete_missing?: boolean
  poll_config?: PollConfig
  enabled?: boolean
}

/**
 * 运行中 run 的细化阶段（后端 `share_sync_runs.phase`）。
 * - `scanning`  ：递归列举分享目录（大分享可长达数分钟，是最容易被误认为卡死的一段）
 * - `diffing`   ：与上次基线快照算差异
 * - `executing` ：转存 / 下载
 * 终态 run 为 `null`。
 */
export type RunPhase = 'scanning' | 'diffing' | 'executing'

export interface RunRecord {
  id: string
  started_at: number
  finished_at: number | null
  status: string
  total_count: number
  added_count: number
  modified_count: number
  removed_count: number
  unchanged_count: number
  failed_count: number
  skipped_count: number
  overwritten_count: number
  error: string | null
  /** 运行中 run 的当前阶段；老后端不返回该字段时为 undefined */
  phase?: RunPhase | string | null
}

export interface RunItemRecord {
  id: number
  path: string
  action: string
  target: string
  transfer_task_id: string | null
  download_task_id: string | null
  status: string
  versioned_old_path: string | null
  error: string | null
  reason?: string | null
}

export interface RunDetail {
  id: string
  started_at: number
  finished_at: number | null
  status: string
  total_count: number
  added_count: number
  modified_count: number
  removed_count: number
  unchanged_count: number
  failed_count: number
  skipped_count: number
  overwritten_count: number
  error: string | null
  /** 运行中 run 的当前阶段（后端 RunDetailDto 用 serde flatten 透传 RunRecord 的字段） */
  phase?: RunPhase | string | null
  items: RunItemRecord[]
  item_total_count: number
  item_page: number
  item_page_size: number
}

export interface RunItemsPage {
  items: RunItemRecord[]
  total: number
  page: number
  page_size: number
}

export interface ShareSnapshotItem {
  path: string
  raw_path?: string
  fs_id: number
  size: number
  name: string
  is_dir: boolean
}

export interface ShareSnapshot {
  id: string
  subscription_id: string
  captured_at: string
  items: ShareSnapshotItem[]
}

/**
 * 分享同步「进行中子任务」进度快照（REST 轮询接口 + WS item_progress 广播共用同一形状）。
 */
export interface ShareSyncSubtask {
  /** 底层任务 id（下载任务 id 或内部转存任务 id）；前端据此 upsert/去重 */
  task_id: string
  /** 文件名 / 展示名 */
  name: string
  /** 子任务种类：`transfer`（转存段）| `download`（下载段） */
  kind: 'transfer' | 'download' | string
  /** 状态字符串（downloading / completed / failed / transferring ...） */
  status: string
  /** 已完成字节（下载段）；转存段用已完成文件数 */
  downloaded: number
  /** 总字节（下载段）；转存段用总文件数 */
  total: number
  /** 进度百分比 0-100 */
  progress: number
  /** 瞬时速度（B/s，仅下载段有意义） */
  speed: number
  /** 预计剩余时间（秒，仅下载段且 speed>0 时有值） */
  eta_seconds?: number | null
  /**
   * 所属父任务的 `task_id`；顶层行没有这个字段。
   *
   * 目前只有一种父子关系：文件夹聚合行（`task_id` 形如 `folder:{id}`）与它当前
   * 正在跑的子文件下载任务。文件夹的子任务是**按批物化**的（后端 folder_manager
   * 只维持约等于槽位数的活跃子任务，其余文件还在 pending 队列里没有任务对象），
   * 所以这里能拿到的是「正在跑的那几个文件」，不是文件夹的全量文件列表。
   */
  parent_task_id?: string | null
  /** 订阅所属账号 uid */
  owner_uid: number
}

export interface TriggerSubscriptionResponse {
  subscription_id: string
  triggered?: boolean
  run_id?: string
  /** true：订阅正在续跑重启前未完成的那一轮，本次只是唤醒其残留下载，没有起新一轮 */
  resumed_existing?: boolean
  /** true：重启后的续跑还没处理到该订阅，本次没有触发（为保护残留下载），稍后再试 */
  pending_resume?: boolean
}

/**
 * 取消扫描的结果。只有扫描阶段可取消。
 */
export interface CancelScanResponse {
  subscription_id: string
  /** 被取消的那一轮 run_id */
  run_id: string
  cancelled: boolean
}

/**
 * 分享同步 WS 事件载荷（与后端 ShareSyncEvent 对齐）
 */
export type ShareSyncWsEvent =
    | { type: 'subscription_created'; subscription_id: string; name: string; owner_uid?: number }
    | { type: 'subscription_updated'; subscription_id: string; owner_uid?: number }
    | { type: 'subscription_deleted'; subscription_id: string; owner_uid?: number }
    | { type: 'status_changed'; subscription_id: string; enabled: boolean; owner_uid?: number }
    | { type: 'diff_detected'; run_id: string; subscription_id: string; added: number; modified: number; removed: number; owner_uid?: number }
    | { type: 'run_started'; run_id: string; subscription_id: string; owner_uid?: number }
    | {
  /**
   * 抓取（递归列目录）阶段的实时进度。后端节流约 500ms 一帧。
   *
   * 注意 `dirs_pending` 会随 BFS 发现新子目录而增长，**不要**用它算百分比，
   * 否则进度条会倒退；按计数式文案展示。
   */
  type: 'scan_progress'
  run_id: string
  subscription_id: string
  /** 已完成列举的目录数 */
  dirs_done: number
  /** 待扫描目录数（动态增长） */
  dirs_pending: number
  /** 累计发现文件数（不含目录） */
  files_seen: number
  /** 当前正在扫描的目录 */
  current_dir: string
  /** 第几次整轮抓取尝试；>1 表示网络异常正在重试 */
  attempt: number
  /** 命中缓存、本轮无需重新请求的目录数（重试续爬的效果） */
  cached_hits: number
  owner_uid?: number
}
    | { type: 'run_completed'; run_id: string; subscription_id: string; added: number; modified: number; removed: number; failed: number; owner_uid?: number }
    | { type: 'run_failed'; run_id: string; subscription_id: string; error: string; owner_uid?: number }
    | { type: 'run_cancelled'; run_id: string; subscription_id: string; owner_uid?: number }
    | {
  type: 'item_progress'
  run_id: string
  subscription_id: string
  task_id: string
  name: string
  kind: 'transfer' | 'download' | string
  status: string
  downloaded: number
  total: number
  progress: number
  speed: number
  eta_seconds?: number | null
  /** 文件夹子文件行指向文件夹那一行的 `folder:{id}`；顶层行缺省 */
  parent_task_id?: string | null
  owner_uid?: number
}

// ==================== API ====================

const BASE = '/share-sync'

export async function listSubscriptions(uid?: number | null): Promise<ShareSubscription[]> {
  // 缺省返回全部账号（与 transfer/autobackup 一致）；传 uid 时按账号过滤。
  const params = typeof uid === 'number' ? { uid } : undefined
  const r = await rawApiClient.get<{ success: boolean; data: ShareSubscription[] }>(`${BASE}/subscriptions`, { params })
  return r.data.data
}

export async function getSubscription(id: string): Promise<ShareSubscription> {
  const r = await rawApiClient.get<{ success: boolean; data: ShareSubscription }>(`${BASE}/subscriptions/${id}`)
  return r.data.data
}

export async function createSubscription(req: CreateShareSubscriptionRequest): Promise<ShareSubscription> {
  const r = await rawApiClient.post<{ success: boolean; data: ShareSubscription }>(`${BASE}/subscriptions`, req)
  return r.data.data
}

export async function updateSubscription(id: string, req: UpdateShareSubscriptionRequest): Promise<ShareSubscription> {
  const r = await rawApiClient.put<{ success: boolean; data: ShareSubscription }>(`${BASE}/subscriptions/${id}`, req)
  return r.data.data
}

export async function deleteSubscription(id: string): Promise<void> {
  await rawApiClient.delete(`${BASE}/subscriptions/${id}`)
}

export async function setSubscriptionEnabled(id: string, enabled: boolean): Promise<void> {
  const path = enabled ? 'enable' : 'disable'
  await rawApiClient.post(`${BASE}/subscriptions/${id}/${path}`)
}

export async function triggerSubscription(id: string): Promise<TriggerSubscriptionResponse> {
  const r = await rawApiClient.post<{ success: boolean; data: TriggerSubscriptionResponse }>(`${BASE}/subscriptions/${id}/trigger`)
  return r.data.data
}

/** 「我已更新链接，恢复」：清除链接失效标记，恢复轮询并立即重试一次 */
export async function resumeSubscription(id: string): Promise<void> {
  await rawApiClient.post(`${BASE}/subscriptions/${id}/resume`)
}

/**
 * 取消当前扫描（不影响订阅本身）。`runId` 是用户观察到的那一轮，后端先比对
 * run 身份再取消，迟到请求不会取消新一轮；不可取消时返回 409。
 */
export async function cancelScan(id: string, runId: string): Promise<CancelScanResponse> {
  const r = await rawApiClient.post<{ success: boolean; data: CancelScanResponse }>(
      `${BASE}/subscriptions/${id}/cancel-scan`,
      { run_id: runId }
  )
  return r.data.data
}

export async function listRuns(id: string, page = 1, pageSize = 20): Promise<RunRecord[]> {
  const r = await rawApiClient.get<{ success: boolean; data: RunRecord[] }>(`${BASE}/subscriptions/${id}/runs`, {
    params: { page, page_size: pageSize }
  })
  return r.data.data
}

export async function getRun(runId: string, page = 1, pageSize = 100): Promise<RunDetail> {
  const r = await rawApiClient.get<{ success: boolean; data: RunDetail }>(`${BASE}/runs/${runId}`, {
    params: { page, page_size: pageSize }
  })
  return r.data.data
}

export async function listRunItems(runId: string, page = 1, pageSize = 100): Promise<RunItemsPage> {
  const r = await rawApiClient.get<{ success: boolean; data: RunItemsPage }>(`${BASE}/runs/${runId}/items`, {
    params: { page, page_size: pageSize }
  })
  return r.data.data
}

/**
 * 列出某订阅当前「进行中」的子任务（下载段 + 内部转存段）。
 * WS `item_progress` 断线时作为轮询兜底；与 WS 广播共用同一数据形状。
 */
export async function listSubtasks(id: string): Promise<ShareSyncSubtask[]> {
  const r = await rawApiClient.get<{ success: boolean; data: ShareSyncSubtask[] }>(
      `${BASE}/subscriptions/${id}/subtasks`,
  )
  return r.data.data
}

export async function getLatestSnapshot(id: string): Promise<ShareSnapshot> {
  const r = await rawApiClient.get<{ success: boolean; data: ShareSnapshot }>(`${BASE}/subscriptions/${id}/snapshots/latest`)
  return r.data.data
}

// ==================== 目录树预览 ====================

export interface TreeNode {
  path: string
  name: string
  is_dir: boolean
  size: number
  fs_id: number
  children?: TreeNode[] | null
}

export interface PreviewTreeResponse {
  root: TreeNode[]
  short_key: string
  shareid: string
}

/**
 * 解释 cron 表达式：中文描述 + 接下来几次触发时间（服务器时区）
 */
export async function previewCron(expr: string, count = 5): Promise<CronPreview> {
  const r = await rawApiClient.post<{ success: boolean; data: CronPreview }>(
      `${BASE}/cron/preview`,
      { expr, count }
  )
  return r.data.data
}

/**
 * 预览分享的目录树（用于前端勾选 include_paths）
 */
export async function previewTree(
    share_url: string,
    password?: string | null,
    depth = 2,
    ownerUid?: number | null
): Promise<PreviewTreeResponse> {
  const r = await rawApiClient.post<{ success: boolean; data: PreviewTreeResponse }>(
      `${BASE}/preview-tree`,
      {
        share_url,
        password: password || null,
        depth,
        // 按订阅所属账号路由网盘 client（编辑非当前账号订阅时必需）；不传则后端回退 active 账号
        owner_uid: ownerUid ?? undefined,
      }
  )
  return r.data.data
}

// ==================== 排除规则能力与试匹配 ====================

/** 规则错误类型，与后端 `kind` 取值一致 */
export type ExcludeRuleKind = 'glob' | 'regex' | 'path' | 'rules'

export interface ExcludeRuleError {
  kind: ExcludeRuleKind
  /** 0 起的列表下标；列表级错误没有 */
  index?: number
  message: string
}

export interface ExcludeRegexLimits {
  max_rules: number
  max_rule_bytes: number
  max_total_bytes: number
  max_test_path_bytes: number
}

export interface ExcludeRuleCapabilities {
  supports_regex: boolean
  regex_limits: ExcludeRegexLimits
}

export interface ValidateExcludeRulesRequest {
  exclude_patterns?: string[]
  exclude_regex_patterns?: string[]
  /** 可选试匹配路径（相对分享根） */
  test_path?: string
}

export interface ValidateExcludeRulesResponse {
  valid: boolean
  errors: ExcludeRuleError[]
  /** 仅规则合法且提供了 test_path 时返回 */
  excluded?: boolean
}

/**
 * 服务端是否支持正则排除规则及其限制。
 *
 * 失败（404 / 网络不可用）时调用方必须保持纯 glob 编辑，不能把异常当成「支持」。
 */
export async function getExcludeRuleCapabilities(): Promise<ExcludeRuleCapabilities> {
  const r = await rawApiClient.get<{ success: boolean; data: ExcludeRuleCapabilities }>(
      `${BASE}/exclude-rules/capabilities`
  )
  return r.data.data
}

/**
 * 校验排除规则，可选对一条相对分享根路径试匹配。
 *
 * 只覆盖请求里的两份规则列表（不含同步范围与精确排除目录）；规则非法时
 * `valid:false` 且后端不会执行匹配。
 */
export async function validateExcludeRules(
    req: ValidateExcludeRulesRequest
): Promise<ValidateExcludeRulesResponse> {
  const r = await rawApiClient.post<{ success: boolean; data: ValidateExcludeRulesResponse }>(
      `${BASE}/exclude-rules/validate`,
      {
        exclude_patterns: req.exclude_patterns ?? [],
        exclude_regex_patterns: req.exclude_regex_patterns ?? [],
        ...(req.test_path === undefined ? {} : { test_path: req.test_path }),
      }
  )
  return r.data.data
}
