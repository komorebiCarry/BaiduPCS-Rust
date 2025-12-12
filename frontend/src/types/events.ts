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

export type DownloadEvent =
  | DownloadEventCreated
  | DownloadEventProgress
  | DownloadEventStatusChanged
  | DownloadEventCompleted
  | DownloadEventFailed
  | DownloadEventPaused
  | DownloadEventResumed
  | DownloadEventDeleted

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

export type UploadEvent =
  | UploadEventCreated
  | UploadEventProgress
  | UploadEventStatusChanged
  | UploadEventCompleted
  | UploadEventFailed
  | UploadEventPaused
  | UploadEventResumed
  | UploadEventDeleted

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

export type TaskEvent =
  | TaskEventDownload
  | TaskEventFolder
  | TaskEventUpload
  | TaskEventTransfer

// ============ 带时间戳的事件 ============

export interface TimestampedEvent {
  event_id: number
  timestamp: number
  category: string
  event: DownloadEvent | FolderEvent | UploadEvent | TransferEvent
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
