/**
 * 自动备份 API
 */

import { rawApiClient } from './client'

// ==================== 类型定义 ====================

/** 备份方向 */
export type BackupDirection = 'upload' | 'download'

/** 轮询模式 */
export type PollMode = 'disabled' | 'interval' | 'scheduled'

/** 备份任务状态 */
export type BackupTaskStatus = 'queued' | 'preparing' | 'transferring' | 'completed' | 'partially_completed' | 'failed' | 'cancelled' | 'paused'

/** 触发类型 */
export type TriggerType = 'manual' | 'watch' | 'poll' | 'scheduled'

/** 监听配置 */
export interface WatchConfig {
  enabled: boolean
  debounce_ms: number
  recursive: boolean
}

/** 轮询配置 */
export interface PollConfig {
  enabled: boolean
  mode: PollMode
  interval_minutes: number
  schedule_hour?: number
  schedule_minute?: number
}

/** 过滤配置 */
export interface FilterConfig {
  include_patterns: string[]
  exclude_patterns: string[]
  max_file_size?: number
  min_file_size?: number
}

/** 备份配置 */
export interface BackupConfig {
  id: string
  name: string
  local_path: string
  remote_path: string
  direction: BackupDirection
  watch_config: WatchConfig
  poll_config: PollConfig
  filter_config: FilterConfig
  encrypt_enabled: boolean
  enabled: boolean
  created_at: string
  updated_at: string
}

/** 创建备份配置请求 */
export interface CreateBackupConfigRequest {
  name: string
  local_path: string
  remote_path: string
  direction: BackupDirection
  watch_config: WatchConfig
  poll_config: PollConfig
  filter_config: FilterConfig
  encrypt_enabled: boolean
}

/** 更新备份配置请求 */
export interface UpdateBackupConfigRequest {
  name?: string
  local_path?: string
  remote_path?: string
  watch_config?: WatchConfig
  poll_config?: PollConfig
  filter_config?: FilterConfig
  enabled?: boolean
}

/** 备份任务 */
export interface BackupTask {
  id: string
  config_id: string
  status: BackupTaskStatus
  trigger_type: TriggerType
  completed_count: number
  failed_count: number
  skipped_count: number
  total_count: number
  transferred_bytes: number
  total_bytes: number
  created_at: string
  started_at?: string
  completed_at?: string
  error_message?: string
}

/** 文件备份状态 */
export type BackupFileStatus = 'pending' | 'checking' | 'skipped' | 'encrypting' | 'decrypting' | 'waiting_transfer' | 'transferring' | 'completed' | 'failed'

/** 过滤原因类型 */
export type FilterReasonType =
  | { extension_not_included: string }
  | { extension_excluded: string }
  | { directory_excluded: string }
  | { file_too_large: { size: number; max: number } }
  | { file_too_small: { size: number; min: number } }
  | 'hidden_file'
  | 'system_file'
  | 'temp_file'

/** 跳过原因 */
export type SkipReason =
  | 'already_exists'
  | 'unchanged'
  | { filtered: FilterReasonType }
  | 'user_cancelled'
  | 'config_disabled'

/** 单个文件的备份任务 */
export interface BackupFileTask {
  id: string
  parent_task_id: string
  local_path: string
  remote_path: string
  file_size: number
  status: BackupFileStatus
  skip_reason?: SkipReason
  encrypted: boolean
  encrypted_name?: string
  temp_encrypted_path?: string
  transferred_bytes: number
  error_message?: string
  retry_count: number
  created_at: string
  updated_at: string
  /** 加密进度 (0.0 - 100.0) */
  encrypt_progress?: number
  /** 解密进度 (0.0 - 100.0) */
  decrypt_progress?: number
}

/** 文件任务列表响应 */
export interface FileTasksResponse {
  file_tasks: BackupFileTask[]
  total: number
  page: number
  page_size: number
}

/** 加密状态 */
export interface EncryptionStatus {
  enabled: boolean
  has_key: boolean
  algorithm: string
  key_created_at?: string
}

/** 管理器状态 */
export interface ManagerStatus {
  config_count: number
  active_task_count: number
  watcher_running: boolean
  watched_path_count: number
  poll_schedule_count: number
  encryption_enabled: boolean
  scan_slots: string
  encrypt_slots: string
}

/** 记录统计 */
export interface RecordStats {
  upload_count: number
  download_count: number
  snapshot_count: number
}

/** API 响应 */
export interface ApiResponse<T> {
  success: boolean
  data?: T
  error?: string
}

// ==================== 备份配置 API ====================

/** 获取所有备份配置 */
export async function listBackupConfigs(): Promise<BackupConfig[]> {
  const response = await rawApiClient.get<ApiResponse<BackupConfig[]>>('/autobackup/configs')
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取配置列表失败')
}

/** 获取单个备份配置 */
export async function getBackupConfig(id: string): Promise<BackupConfig> {
  const response = await rawApiClient.get<ApiResponse<BackupConfig>>(`/autobackup/configs/${id}`)
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取配置失败')
}

/** 创建备份配置 */
export async function createBackupConfig(request: CreateBackupConfigRequest): Promise<BackupConfig> {
  const response = await rawApiClient.post<ApiResponse<BackupConfig>>('/autobackup/configs', request)
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '创建配置失败')
}

/** 更新备份配置 */
export async function updateBackupConfig(id: string, request: UpdateBackupConfigRequest): Promise<BackupConfig> {
  const response = await rawApiClient.put<ApiResponse<BackupConfig>>(`/autobackup/configs/${id}`, request)
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '更新配置失败')
}

/** 删除备份配置 */
export async function deleteBackupConfig(id: string): Promise<void> {
  const response = await rawApiClient.delete<ApiResponse<void>>(`/autobackup/configs/${id}`)
  if (!response.data.success) {
    throw new Error(response.data.error || '删除配置失败')
  }
}

/** 启用备份配置 */
export async function enableBackupConfig(id: string): Promise<BackupConfig> {
  const response = await rawApiClient.post<ApiResponse<BackupConfig>>(`/autobackup/configs/${id}/enable`)
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '启用配置失败')
}

/** 禁用备份配置 */
export async function disableBackupConfig(id: string): Promise<BackupConfig> {
  const response = await rawApiClient.post<ApiResponse<BackupConfig>>(`/autobackup/configs/${id}/disable`)
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '禁用配置失败')
}

// ==================== 备份任务 API ====================

/** 手动触发备份 */
export async function triggerBackup(configId: string): Promise<string> {
  const response = await rawApiClient.post<ApiResponse<{ task_id: string }>>(`/autobackup/configs/${configId}/trigger`)
  if (response.data.success && response.data.data) {
    return response.data.data.task_id
  }
  throw new Error(response.data.error || '触发备份失败')
}

/** 获取备份任务 */
export async function getBackupTask(taskId: string): Promise<BackupTask> {
  const response = await rawApiClient.get<ApiResponse<BackupTask>>(`/autobackup/tasks/${taskId}`)
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取任务失败')
}

/** 获取配置的所有任务 */
export async function listBackupTasks(configId: string): Promise<BackupTask[]> {
  const response = await rawApiClient.get<ApiResponse<BackupTask[]>>(`/autobackup/configs/${configId}/tasks`)
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取任务列表失败')
}

/** 取消备份任务 */
export async function cancelBackupTask(taskId: string): Promise<void> {
  const response = await rawApiClient.post<ApiResponse<void>>(`/autobackup/tasks/${taskId}/cancel`)
  if (!response.data.success) {
    throw new Error(response.data.error || '取消任务失败')
  }
}

/** 暂停备份任务 */
export async function pauseBackupTask(taskId: string): Promise<void> {
  const response = await rawApiClient.post<ApiResponse<void>>(`/autobackup/tasks/${taskId}/pause`)
  if (!response.data.success) {
    throw new Error(response.data.error || '暂停任务失败')
  }
}

/** 恢复备份任务 */
export async function resumeBackupTask(taskId: string): Promise<void> {
  const response = await rawApiClient.post<ApiResponse<void>>(`/autobackup/tasks/${taskId}/resume`)
  if (!response.data.success) {
    throw new Error(response.data.error || '恢复任务失败')
  }
}

/** 获取任务的文件任务列表（分页） */
export async function listFileTasks(taskId: string, page = 1, pageSize = 20): Promise<FileTasksResponse> {
  const response = await rawApiClient.get<ApiResponse<FileTasksResponse>>(`/autobackup/tasks/${taskId}/files`, {
    params: { page, page_size: pageSize }
  })
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取文件任务列表失败')
}

/** 重试单个文件任务 */
export async function retryFileTask(taskId: string, fileTaskId: string): Promise<void> {
  const response = await rawApiClient.post<ApiResponse<void>>(`/autobackup/tasks/${taskId}/files/${fileTaskId}/retry`)
  if (!response.data.success) {
    throw new Error(response.data.error || '重试文件任务失败')
  }
}

// ==================== 加密 API ====================

/** 获取加密状态 */
export async function getEncryptionStatus(): Promise<EncryptionStatus> {
  const response = await rawApiClient.get<ApiResponse<EncryptionStatus>>('/encryption/status')
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取加密状态失败')
}

/** 生成加密密钥 */
export async function generateEncryptionKey(algorithm?: string): Promise<string> {
  const response = await rawApiClient.post<ApiResponse<{ key: string }>>('/encryption/key/generate', { algorithm })
  if (response.data.success && response.data.data) {
    return response.data.data.key
  }
  throw new Error(response.data.error || '生成密钥失败')
}

/** 导入加密密钥 */
export async function importEncryptionKey(key: string, algorithm?: string): Promise<void> {
  const response = await rawApiClient.post<ApiResponse<void>>('/encryption/key/import', { key, algorithm })
  if (!response.data.success) {
    throw new Error(response.data.error || '导入密钥失败')
  }
}

/** 导出加密密钥 */
export async function exportEncryptionKey(): Promise<string> {
  const response = await rawApiClient.get<ApiResponse<{ key: string }>>('/encryption/key/export')
  if (response.data.success && response.data.data) {
    return response.data.data.key
  }
  throw new Error(response.data.error || '导出密钥失败')
}

/** 删除加密密钥 */
export async function deleteEncryptionKey(): Promise<void> {
  const response = await rawApiClient.delete<ApiResponse<void>>('/encryption/key')
  if (!response.data.success) {
    throw new Error(response.data.error || '删除密钥失败')
  }
}

// ==================== 状态和统计 API ====================

/** 获取管理器状态 */
export async function getManagerStatus(): Promise<ManagerStatus> {
  const response = await rawApiClient.get<ApiResponse<ManagerStatus>>('/autobackup/status')
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取状态失败')
}

/** 获取记录统计 */
export async function getRecordStats(): Promise<RecordStats> {
  const response = await rawApiClient.get<ApiResponse<RecordStats>>('/autobackup/stats')
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取统计失败')
}

/** 清理过期记录 */
export async function cleanupRecords(days?: number): Promise<{ upload_deleted: number; download_deleted: number; snapshot_deleted: number }> {
  const response = await rawApiClient.post<ApiResponse<{ upload_deleted: number; download_deleted: number; snapshot_deleted: number }>>('/autobackup/cleanup', { days })
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '清理记录失败')
}

// ==================== 文件监听能力检测 API ====================

/** 文件监听能力 */
export interface WatchCapability {
  available: boolean
  platform: string
  backend: string
  reason?: string
  suggestion?: string
  warnings: string[]
}

/** 获取文件监听能力 */
export async function getWatchCapability(): Promise<WatchCapability> {
  const response = await rawApiClient.get<ApiResponse<WatchCapability>>('/system/watch-capability')
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取文件监听能力失败')
}

// ==================== 全局触发配置 API ====================

/** 上传触发配置 */
export interface UploadTriggerConfig {
  watch_enabled: boolean
  watch_debounce_ms: number
  watch_recursive: boolean
  fallback_interval_enabled: boolean
  fallback_interval_minutes: number
  fallback_scheduled_enabled: boolean
  fallback_scheduled_hour: number
  fallback_scheduled_minute: number
}

/** 下载触发配置 */
export interface DownloadTriggerConfig {
  poll_mode: 'interval' | 'scheduled'
  poll_interval_minutes: number
  poll_scheduled_hour: number
  poll_scheduled_minute: number
}

/** 全局触发配置 */
export interface GlobalTriggerConfig {
  upload_trigger: UploadTriggerConfig
  download_trigger: DownloadTriggerConfig
}

/** 获取全局触发配置 */
export async function getTriggerConfig(): Promise<GlobalTriggerConfig> {
  const response = await rawApiClient.get<ApiResponse<GlobalTriggerConfig>>('/config/autobackup/trigger')
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '获取触发配置失败')
}

/** 更新上传触发配置请求 */
export interface UpdateUploadTriggerRequest {
  watch_enabled?: boolean
  watch_debounce_ms?: number
  watch_recursive?: boolean
  fallback_interval_enabled?: boolean
  fallback_interval_minutes?: number
  fallback_scheduled_enabled?: boolean
  fallback_scheduled_hour?: number
  fallback_scheduled_minute?: number
}

/** 更新下载触发配置请求 */
export interface UpdateDownloadTriggerRequest {
  poll_mode?: 'interval' | 'scheduled'
  poll_interval_minutes?: number
  poll_scheduled_hour?: number
  poll_scheduled_minute?: number
}

/** 更新全局触发配置请求 */
export interface UpdateTriggerConfigRequest {
  upload_trigger?: UpdateUploadTriggerRequest
  download_trigger?: UpdateDownloadTriggerRequest
}

/** 更新全局触发配置 */
export async function updateTriggerConfig(request: UpdateTriggerConfigRequest): Promise<GlobalTriggerConfig> {
  const response = await rawApiClient.put<ApiResponse<GlobalTriggerConfig>>('/config/autobackup/trigger', request)
  if (response.data.success && response.data.data) {
    return response.data.data
  }
  throw new Error(response.data.error || '更新触发配置失败')
}
