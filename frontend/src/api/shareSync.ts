/**
 * 分享同步 API 客户端
 */

import { rawApiClient } from './client'

// ==================== 类型定义 ====================

export type ConflictStrategy = 'overwrite' | 'versioned' | 'skip'
export type PollMode = 'disabled' | 'interval' | 'scheduled'
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
}

export interface ShareSubscription {
  id: string
  name: string
  share_url: string
  password?: string | null
  include_paths: string[]
  exclude_patterns: string[]
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
  targets?: SyncTarget[]
  conflict_strategy?: ConflictStrategy
  delete_missing?: boolean
  poll_config?: PollConfig
  enabled?: boolean
}

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
  /** 订阅所属账号 uid */
  owner_uid: number
}

export interface TriggerSubscriptionResponse {
  subscription_id: string
  triggered?: boolean
  run_id?: string
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
    | { type: 'run_completed'; run_id: string; subscription_id: string; added: number; modified: number; removed: number; failed: number; owner_uid?: number }
    | { type: 'run_failed'; run_id: string; subscription_id: string; error: string; owner_uid?: number }
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
