/**
 * WebSocket 事件类型定义
 * 与后端 Rust 事件类型保持一致
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
}

export interface DownloadEventProgress {
  event_type: 'progress'
  task_id: string
  downloaded_size: number
  total_size: number
  speed: number
  progress: number
}

export interface DownloadEventStatusChanged {
  event_type: 'status_changed'
  task_id: string
  old_status: string
  new_status: string
}

export interface DownloadEventCompleted {
  event_type: 'completed'
  task_id: string
  completed_at: number
}

export interface DownloadEventFailed {
  event_type: 'failed'
  task_id: string
  error: string
}

export interface DownloadEventPaused {
  event_type: 'paused'
  task_id: string
}

export interface DownloadEventResumed {
  event_type: 'resumed'
  task_id: string
}

export interface DownloadEventDeleted {
  event_type: 'deleted'
  task_id: string
}

export interface DownloadEventDecryptProgress {
  event_type: 'decrypt_progress'
  task_id: string
  decrypt_progress: number
  processed_bytes: number
  total_bytes: number
  group_id?: string
  is_backup?: boolean
}

export interface DownloadEventDecryptCompleted {
  event_type: 'decrypt_completed'
  task_id: string
  original_size: number
  decrypted_path: string
  group_id?: string
  is_backup?: boolean
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
}

export interface FolderEventStatusChanged {
  event_type: 'status_changed'
  folder_id: string
  old_status: string
  new_status: string
}

export interface FolderEventScanCompleted {
  event_type: 'scan_completed'
  folder_id: string
  total_files: number
  total_size: number
}

export interface FolderEventCompleted {
  event_type: 'completed'
  folder_id: string
  completed_at: number
}

export interface FolderEventFailed {
  event_type: 'failed'
  folder_id: string
  error: string
}

export interface FolderEventPaused {
  event_type: 'paused'
  folder_id: string
}

export interface FolderEventResumed {
  event_type: 'resumed'
  folder_id: string
}

export interface FolderEventDeleted {
  event_type: 'deleted'
  folder_id: string
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
}

export interface UploadEventStatusChanged {
  event_type: 'status_changed'
  task_id: string
  old_status: string
  new_status: string
}

export interface UploadEventCompleted {
  event_type: 'completed'
  task_id: string
  completed_at: number
  is_rapid_upload: boolean
}

export interface UploadEventFailed {
  event_type: 'failed'
  task_id: string
  error: string
}

export interface UploadEventPaused {
  event_type: 'paused'
  task_id: string
}

export interface UploadEventResumed {
  event_type: 'resumed'
  task_id: string
}

export interface UploadEventDeleted {
  event_type: 'deleted'
  task_id: string
}

export interface UploadEventEncryptProgress {
  event_type: 'encrypt_progress'
  task_id: string
  encrypt_progress: number
  processed_bytes: number
  total_bytes: number
  is_backup?: boolean
}

export interface UploadEventEncryptCompleted {
  event_type: 'encrypt_completed'
  task_id: string
  encrypted_size: number
  original_size: number
  is_backup?: boolean
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
}

export interface BackupEventScanProgress {
  event_type: 'scan_progress'
  task_id: string
  scanned_files: number
  scanned_dirs: number
}

export interface BackupEventScanCompleted {
  event_type: 'scan_completed'
  task_id: string
  total_files: number
  total_bytes: number
}

export interface BackupEventFileProgress {
  event_type: 'file_progress'
  task_id: string
  file_task_id: string
  file_name: string
  transferred_bytes: number
  total_bytes: number
  status: string
}

export interface BackupEventFileStatusChanged {
  event_type: 'file_status_changed'
  task_id: string
  file_task_id: string
  file_name: string
  old_status: string
  new_status: string
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
}

export interface BackupEventStatusChanged {
  event_type: 'status_changed'
  task_id: string
  old_status: string
  new_status: string
}

export interface BackupEventCompleted {
  event_type: 'completed'
  task_id: string
  completed_at: number
  success_count: number
  failed_count: number
  skipped_count: number
}

export interface BackupEventFailed {
  event_type: 'failed'
  task_id: string
  error: string
}

export interface BackupEventPaused {
  event_type: 'paused'
  task_id: string
}

export interface BackupEventResumed {
  event_type: 'resumed'
  task_id: string
}

export interface BackupEventCancelled {
  event_type: 'cancelled'
  task_id: string
}

export interface BackupEventFileEncrypting {
  event_type: 'file_encrypting'
  task_id: string
  file_task_id: string
  file_name: string
}

export interface BackupEventFileEncrypted {
  event_type: 'file_encrypted'
  task_id: string
  file_task_id: string
  file_name: string
  encrypted_name: string
  encrypted_size: number
}

export interface BackupEventFileDecrypting {
  event_type: 'file_decrypting'
  task_id: string
  file_task_id: string
  file_name: string
}

export interface BackupEventFileDecrypted {
  event_type: 'file_decrypted'
  task_id: string
  file_task_id: string
  file_name: string
  original_name: string
  original_size: number
}

export interface BackupEventFileEncryptProgress {
  event_type: 'file_encrypt_progress'
  task_id: string
  file_task_id: string
  file_name: string
  progress: number
  processed_bytes: number
  total_bytes: number
}

export interface BackupEventFileDecryptProgress {
  event_type: 'file_decrypt_progress'
  task_id: string
  file_task_id: string
  file_name: string
  progress: number
  processed_bytes: number
  total_bytes: number
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
}

export interface TransferEventProgress {
  event_type: 'progress'
  task_id: string
  status: string
  transferred_count: number
  total_count: number
  progress: number
}

export interface TransferEventStatusChanged {
  event_type: 'status_changed'
  task_id: string
  old_status: string
  new_status: string
}

export interface TransferEventCompleted {
  event_type: 'completed'
  task_id: string
  completed_at: number
}

export interface TransferEventFailed {
  event_type: 'failed'
  task_id: string
  error: string
  error_type: string
}

export interface TransferEventDeleted {
  event_type: 'deleted'
  task_id: string
}

export type TransferEvent =
    | TransferEventCreated
    | TransferEventProgress
    | TransferEventStatusChanged
    | TransferEventCompleted
    | TransferEventFailed
    | TransferEventDeleted

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

export type TaskEvent =
    | TaskEventDownload
    | TaskEventFolder
    | TaskEventUpload
    | TaskEventTransfer
    | TaskEventBackup

// ============ 带时间戳的事件 ============

export interface TimestampedEvent {
  event_id: number
  timestamp: number
  category: string
  event: DownloadEvent | FolderEvent | UploadEvent | TransferEvent | BackupEvent
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
