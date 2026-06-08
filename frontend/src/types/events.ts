/**
 * WebSocket 事件类型定义
 * 与后端 Rust 事件类型保持一致
 *
 * 多账号：所有任务事件变体均可携带
 * `owner_uid?: number` 标识任务归属账号。后端使用 `Option<Uid>` +
 * `#[serde(skip_serializing_if = "Option::is_none")]`，缺失时字段完全不传；
 * 前端以可选字段（`?:`）表达 「可能缺失」 语义，**不**用 `number | null`。
 */

// ============ 下载事件 ============

export interface DownloadEventCreated {
  event_type: 'created'
  task_id: string
  fs_id: number
  remote_path: string
  local_path: string
  total_size: number
  group_id?: string
  is_backup?: boolean
  original_filename?: string
  owner_uid?: number
}

export interface DownloadEventProgress {
  event_type: 'progress'
  task_id: string
  downloaded_size: number
  total_size: number
  speed: number
  progress: number
  /** progress 类载荷高频事件，后端可选不携带；前端需通过 task_id 反查 owner_uid */
  owner_uid?: number
}

export interface DownloadEventStatusChanged {
  event_type: 'status_changed'
  task_id: string
  old_status: string
  new_status: string
  group_id?: string
  is_backup?: boolean
  /**
   * 状态变更原因（仅 auto_requeue_task 退回等待队列时携带）
   * 后端 #[serde(skip_serializing_if = "Option::is_none")]，None 时整个字段不发送
   */
  error?: string
  owner_uid?: number
}

export interface DownloadEventCompleted {
  event_type: 'completed'
  task_id: string
  completed_at: number
  group_id?: string
  owner_uid?: number
}

export interface DownloadEventFailed {
  event_type: 'failed'
  task_id: string
  error: string
  owner_uid?: number
}

export interface DownloadEventPaused {
  event_type: 'paused'
  task_id: string
  owner_uid?: number
}

export interface DownloadEventResumed {
  event_type: 'resumed'
  task_id: string
  owner_uid?: number
}

export interface DownloadEventDeleted {
  event_type: 'deleted'
  task_id: string
  group_id?: string
  owner_uid?: number
}

export interface DownloadEventDecryptProgress {
  event_type: 'decrypt_progress'
  task_id: string
  decrypt_progress: number
  processed_bytes: number
  total_bytes: number
  group_id?: string
  is_backup?: boolean
  owner_uid?: number
}

/**
 * `DecryptCompleted` 优先级 High、**不** 属于 progress 类，
 * 后端必须携带 `owner_uid`。
 */
export interface DownloadEventDecryptCompleted {
  event_type: 'decrypt_completed'
  task_id: string
  original_size: number
  decrypted_path: string
  group_id?: string
  is_backup?: boolean
  owner_uid?: number
}

export type DownloadEvent =
    | DownloadEventCreated
    | DownloadEventProgress
    | DownloadEventStatusChanged
    | DownloadEventCompleted
    | DownloadEventFailed
    | DownloadEventPaused
    | DownloadEventResumed
    | DownloadEventDeleted
    | DownloadEventDecryptProgress
    | DownloadEventDecryptCompleted

// ============ 文件夹事件 ============

export interface FolderEventCreated {
  event_type: 'created'
  folder_id: string
  name: string
  remote_root: string
  local_root: string
  owner_uid?: number
}

export interface FolderEventProgress {
  event_type: 'progress'
  folder_id: string
  downloaded_size: number
  total_size: number
  completed_files: number
  total_files: number
  speed: number
  status: string
  owner_uid?: number
}

export interface FolderEventStatusChanged {
  event_type: 'status_changed'
  folder_id: string
  old_status: string
  new_status: string
  owner_uid?: number
}

export interface FolderEventScanCompleted {
  event_type: 'scan_completed'
  folder_id: string
  total_files: number
  total_size: number
  owner_uid?: number
}

export interface FolderEventCompleted {
  event_type: 'completed'
  folder_id: string
  completed_at: number
  owner_uid?: number
}

export interface FolderEventFailed {
  event_type: 'failed'
  folder_id: string
  error: string
  owner_uid?: number
}

export interface FolderEventPaused {
  event_type: 'paused'
  folder_id: string
  owner_uid?: number
}

export interface FolderEventResumed {
  event_type: 'resumed'
  folder_id: string
  owner_uid?: number
}

export interface FolderEventDeleted {
  event_type: 'deleted'
  folder_id: string
  owner_uid?: number
}

export type FolderEvent =
    | FolderEventCreated
    | FolderEventProgress
    | FolderEventStatusChanged
    | FolderEventScanCompleted
    | FolderEventCompleted
    | FolderEventFailed
    | FolderEventPaused
    | FolderEventResumed
    | FolderEventDeleted

// ============ 上传事件 ============

export interface UploadEventCreated {
  event_type: 'created'
  task_id: string
  local_path: string
  remote_path: string
  total_size: number
  owner_uid?: number
}

export interface UploadEventProgress {
  event_type: 'progress'
  task_id: string
  uploaded_size: number
  total_size: number
  speed: number
  progress: number
  completed_chunks: number
  total_chunks: number
  owner_uid?: number
}

export interface UploadEventStatusChanged {
  event_type: 'status_changed'
  task_id: string
  old_status: string
  new_status: string
  owner_uid?: number
}

export interface UploadEventCompleted {
  event_type: 'completed'
  task_id: string
  completed_at: number
  is_rapid_upload: boolean
  owner_uid?: number
}

export interface UploadEventFailed {
  event_type: 'failed'
  task_id: string
  error: string
  owner_uid?: number
}

export interface UploadEventPaused {
  event_type: 'paused'
  task_id: string
  owner_uid?: number
}

export interface UploadEventResumed {
  event_type: 'resumed'
  task_id: string
  owner_uid?: number
}

export interface UploadEventDeleted {
  event_type: 'deleted'
  task_id: string
  owner_uid?: number
}

export interface UploadEventEncryptProgress {
  event_type: 'encrypt_progress'
  task_id: string
  encrypt_progress: number
  processed_bytes: number
  total_bytes: number
  is_backup?: boolean
  owner_uid?: number
}

export interface UploadEventEncryptCompleted {
  event_type: 'encrypt_completed'
  task_id: string
  encrypted_size: number
  original_size: number
  is_backup?: boolean
  owner_uid?: number
}

export type UploadEvent =
    | UploadEventCreated
    | UploadEventProgress
    | UploadEventStatusChanged
    | UploadEventCompleted
    | UploadEventFailed
    | UploadEventPaused
    | UploadEventResumed
    | UploadEventDeleted
    | UploadEventEncryptProgress
    | UploadEventEncryptCompleted

// ============ 备份事件 ============
// 注意：event_type 与后端 BackupEvent 的 serde rename_all = "snake_case" 保持一致

export interface BackupEventCreated {
  event_type: 'created'
  task_id: string
  config_id: string
  config_name: string
  direction: string
  trigger_type: string
  owner_uid?: number
}

export interface BackupEventScanProgress {
  event_type: 'scan_progress'
  task_id: string
  scanned_files: number
  scanned_dirs: number
  owner_uid?: number
}

export interface BackupEventScanCompleted {
  event_type: 'scan_completed'
  task_id: string
  total_files: number
  total_bytes: number
  owner_uid?: number
}

export interface BackupEventFileProgress {
  event_type: 'file_progress'
  task_id: string
  file_task_id: string
  file_name: string
  transferred_bytes: number
  total_bytes: number
  status: string
  owner_uid?: number
}

export interface BackupEventFileStatusChanged {
  event_type: 'file_status_changed'
  task_id: string
  file_task_id: string
  file_name: string
  old_status: string
  new_status: string
  owner_uid?: number
}

export interface BackupEventProgress {
  event_type: 'progress'
  task_id: string
  completed_count: number
  failed_count: number
  skipped_count: number
  total_count: number
  transferred_bytes: number
  total_bytes: number
  owner_uid?: number
}

export interface BackupEventStatusChanged {
  event_type: 'status_changed'
  task_id: string
  old_status: string
  new_status: string
  owner_uid?: number
}

export interface BackupEventCompleted {
  event_type: 'completed'
  task_id: string
  completed_at: number
  success_count: number
  failed_count: number
  skipped_count: number
  owner_uid?: number
}

export interface BackupEventFailed {
  event_type: 'failed'
  task_id: string
  error: string
  owner_uid?: number
}

export interface BackupEventPaused {
  event_type: 'paused'
  task_id: string
  owner_uid?: number
}

export interface BackupEventResumed {
  event_type: 'resumed'
  task_id: string
  owner_uid?: number
}

export interface BackupEventCancelled {
  event_type: 'cancelled'
  task_id: string
  owner_uid?: number
}

export interface BackupEventFileEncrypting {
  event_type: 'file_encrypting'
  task_id: string
  file_task_id: string
  file_name: string
  owner_uid?: number
}

export interface BackupEventFileEncrypted {
  event_type: 'file_encrypted'
  task_id: string
  file_task_id: string
  file_name: string
  encrypted_name: string
  encrypted_size: number
  owner_uid?: number
}

export interface BackupEventFileDecrypting {
  event_type: 'file_decrypting'
  task_id: string
  file_task_id: string
  file_name: string
  owner_uid?: number
}

export interface BackupEventFileDecrypted {
  event_type: 'file_decrypted'
  task_id: string
  file_task_id: string
  file_name: string
  original_name: string
  original_size: number
  owner_uid?: number
}

export interface BackupEventFileEncryptProgress {
  event_type: 'file_encrypt_progress'
  task_id: string
  file_task_id: string
  file_name: string
  progress: number
  processed_bytes: number
  total_bytes: number
  owner_uid?: number
}

export interface BackupEventFileDecryptProgress {
  event_type: 'file_decrypt_progress'
  task_id: string
  file_task_id: string
  file_name: string
  progress: number
  processed_bytes: number
  total_bytes: number
  owner_uid?: number
}

export type BackupEvent =
    | BackupEventCreated
    | BackupEventScanProgress
    | BackupEventScanCompleted
    | BackupEventFileProgress
    | BackupEventFileStatusChanged
    | BackupEventProgress
    | BackupEventStatusChanged
    | BackupEventCompleted
    | BackupEventFailed
    | BackupEventPaused
    | BackupEventResumed
    | BackupEventCancelled
    | BackupEventFileEncrypting
    | BackupEventFileEncrypted
    | BackupEventFileDecrypting
    | BackupEventFileDecrypted
    | BackupEventFileEncryptProgress
    | BackupEventFileDecryptProgress

// ============ 转存事件 ============

export interface TransferEventCreated {
  event_type: 'created'
  task_id: string
  share_url: string
  save_path: string
  auto_download: boolean
  owner_uid?: number
}

export interface TransferEventProgress {
  event_type: 'progress'
  task_id: string
  status: string
  transferred_count: number
  total_count: number
  progress: number
  owner_uid?: number
}

export interface TransferEventStatusChanged {
  event_type: 'status_changed'
  task_id: string
  old_status: string
  new_status: string
  owner_uid?: number
}

export interface TransferEventCompleted {
  event_type: 'completed'
  task_id: string
  completed_at: number
  owner_uid?: number
}

export interface TransferEventFailed {
  event_type: 'failed'
  task_id: string
  error: string
  error_type: string
  owner_uid?: number
}

export interface TransferEventDeleted {
  event_type: 'deleted'
  task_id: string
  owner_uid?: number
}

export type TransferEvent =
    | TransferEventCreated
    | TransferEventProgress
    | TransferEventStatusChanged
    | TransferEventCompleted
    | TransferEventFailed
    | TransferEventDeleted

// ============ 离线下载事件 ============

export interface CloudDlEventStatusChanged {
  event_type: 'status_changed'
  task_id: number
  old_status: number | null
  new_status: number
  task: any
  owner_uid?: number
}

export interface CloudDlEventTaskCompleted {
  event_type: 'task_completed'
  task_id: number
  task: any
  auto_download_config: any | null
  owner_uid?: number
}

export interface CloudDlEventProgressUpdate {
  event_type: 'progress_update'
  task_id: number
  finished_size: number
  file_size: number
  progress_percent: number
  owner_uid?: number
}

export interface CloudDlEventTaskListRefreshed {
  event_type: 'task_list_refreshed'
  tasks: any[]
  owner_uid?: number
}

export type CloudDlEvent =
    | CloudDlEventStatusChanged
    | CloudDlEventTaskCompleted
    | CloudDlEventProgressUpdate
    | CloudDlEventTaskListRefreshed

// ============ 账号事件 ============

import type { AccountSummary } from '@/api/accounts'

/**
 * 活跃账号已切换。
 * `new_active_uid = null` 代表删除了最后一个账号 / 未登录状态。
 */
export interface AccountEventSwitched {
  event_type: 'switched'
  new_active_uid: number | null
}

/**
 * 账号列表变更（新增 / 删除 / 元数据更新）。
 */
export interface AccountEventListChanged {
  event_type: 'list_changed'
  accounts: AccountSummary[]
  active_uid: number | null
}

export type AccountEvent = AccountEventSwitched | AccountEventListChanged

// ============ 多账号资源配额事件 (BudgetEvent) ============

/**
 * 压缩算法重算结果（账号登录/删除/配置变更触发）。
 *
 * 全量替换 store 的 machineBudget + perAccountBudget。
 */
export interface BudgetEventRecomputed {
  type: 'budget_recomputed'
  machine_budget_download: number
  machine_budget_upload: number
  per_account: Array<{
    uid: number
    vip_cap_download: number
    base_download: number
    vip_cap_upload: number
    base_upload: number
  }>
}

/**
 * 实时使用量快照（独立定时器每 1s 推送一次）。
 *
 * 仅在「>1 账号且存在活跃任务」时推送，单账号或全空闲时跳过。
 * 仅更新 perAccountUsage，不触发 base/vip_cap 重渲染。
 */
export interface BudgetEventUsageSnapshot {
  type: 'usage_snapshot'
  per_account: Array<{
    uid: number
    used_download: number
    used_upload: number
  }>
}

export type BudgetEvent = BudgetEventRecomputed | BudgetEventUsageSnapshot

// ============ 统一任务事件 ============

export interface TaskEventDownload {
  category: 'download'
  event: DownloadEvent
}

export interface TaskEventFolder {
  category: 'folder'
  event: FolderEvent
}

export interface TaskEventUpload {
  category: 'upload'
  event: UploadEvent
}

export interface TaskEventTransfer {
  category: 'transfer'
  event: TransferEvent
}

export interface TaskEventBackup {
  category: 'backup'
  event: BackupEvent
}

export interface TaskEventCloudDl {
  category: 'cloud_dl'
  event: CloudDlEvent
}

export interface TaskEventAccount {
  category: 'account'
  event: AccountEvent
}

export interface TaskEventBudget {
  category: 'budget'
  event: BudgetEvent
}

export type TaskEvent =
    | TaskEventDownload
    | TaskEventFolder
    | TaskEventUpload
    | TaskEventTransfer
    | TaskEventBackup
    | TaskEventCloudDl
    | TaskEventAccount
    | TaskEventBudget

// ============ 带时间戳的事件 ============

export interface TimestampedEvent {
  event_id: number
  timestamp: number
  category: string
  event:
      | DownloadEvent
      | FolderEvent
      | UploadEvent
      | TransferEvent
      | BackupEvent
      | CloudDlEvent
      | AccountEvent
      | BudgetEvent
}

// ============ WebSocket 消息类型 ============

export interface WsClientPing {
  type: 'ping'
  timestamp: number
}

export interface WsClientRequestSnapshot {
  type: 'request_snapshot'
}

export interface WsClientSubscribe {
  type: 'subscribe'
  subscriptions: string[]
}

export interface WsClientUnsubscribe {
  type: 'unsubscribe'
  subscriptions: string[]
}

export type WsClientMessage = WsClientPing | WsClientRequestSnapshot | WsClientSubscribe | WsClientUnsubscribe

export interface WsServerPong {
  type: 'pong'
  timestamp: number
  client_timestamp?: number
}

export interface WsServerEvent {
  type: 'event'
  event_id: number
  timestamp: number
  category: string
  event: DownloadEvent | FolderEvent | UploadEvent | TransferEvent
}

export interface WsServerEventBatch {
  type: 'event_batch'
  events: TimestampedEvent[]
}

export interface WsServerSnapshot {
  type: 'snapshot'
  downloads: any[]
  uploads: any[]
  transfers: any[]
  folders: any[]
  // ───── 多账号字段 ─────
  /** 当前活跃账号 UID，后端 `Option<u64>` + skip_serializing_if，无活跃账号时字段缺失 */
  active_uid?: number
  /** 所有账号脱敏摘要列表（后端字段 #[serde(default)]，可能为空数组） */
  accounts?: AccountSummary[]
  /** 全局只读模式标志（后端字段 #[serde(default)]，初始=false） */
  readonly_mode?: boolean
}

export interface WsServerConnected {
  type: 'connected'
  connection_id: string
  timestamp: number
}

export interface WsServerError {
  type: 'error'
  code: string
  message: string
}

export interface WsServerSubscribeSuccess {
  type: 'subscribe_success'
  subscriptions: string[]
}

export interface WsServerUnsubscribeSuccess {
  type: 'unsubscribe_success'
  subscriptions: string[]
}

export type WsServerMessage =
    | WsServerPong
    | WsServerEvent
    | WsServerEventBatch
    | WsServerSnapshot
    | WsServerConnected
    | WsServerError
    | WsServerSubscribeSuccess
    | WsServerUnsubscribeSuccess
