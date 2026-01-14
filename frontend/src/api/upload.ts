import { apiClient } from './client'
import { formatFileSize as sharedFormatFileSize, formatSpeed as sharedFormatSpeed, formatETA as sharedFormatETA, extractFilename as sharedExtractFilename } from './utils'

/// 任务状态
export type UploadTaskStatus = 'pending' | 'checking_rapid' | 'encrypting' | 'uploading' | 'paused' | 'completed' | 'rapid_upload_success' | 'failed'

/// 上传任务
export interface UploadTask {
  id: string
  local_path: string
  remote_path: string
  total_size: number
  uploaded_size: number
  status: UploadTaskStatus
  speed: number
  created_at: number
  started_at?: number
  completed_at?: number
  error?: string
  is_rapid_upload?: boolean // 是否秒传
  // 分片信息
  total_chunks?: number
  completed_chunks?: number
  // 加密相关字段
  encrypt_enabled?: boolean
  encrypt_progress?: number
  original_size?: number
}

/// 创建上传任务请求
export interface CreateUploadRequest {
  local_path: string
  remote_path: string
  encrypt?: boolean
}

/// 文件夹扫描选项
export interface FolderScanOptions {
  follow_symlinks?: boolean
  max_file_size?: number
  max_files?: number
  skip_hidden?: boolean
}

/// 创建文件夹上传任务请求
export interface CreateFolderUploadRequest {
  local_folder: string
  remote_folder: string
  scan_options?: FolderScanOptions
  encrypt?: boolean
}

/// 批量创建上传任务请求
export interface CreateBatchUploadRequest {
  files: [string, string][] // [(本地路径, 远程路径)]
  encrypt?: boolean
}

/**
 * 创建上传任务
 */
export async function createUpload(req: CreateUploadRequest): Promise<string> {
  return apiClient.post('/uploads', req)
}

/**
 * 创建文件夹上传任务
 */
export async function createFolderUpload(req: CreateFolderUploadRequest): Promise<string[]> {
  return apiClient.post('/uploads/folder', req)
}

/**
 * 批量创建上传任务
 */
export async function createBatchUpload(req: CreateBatchUploadRequest): Promise<string[]> {
  return apiClient.post('/uploads/batch', req)
}

/**
 * 获取所有上传任务
 */
export async function getAllUploads(): Promise<UploadTask[]> {
  return apiClient.get('/uploads')
}

/**
 * 获取指定上传任务
 */
export async function getUpload(taskId: string): Promise<UploadTask> {
  return apiClient.get(`/uploads/${taskId}`)
}

/**
 * 暂停上传任务
 */
export async function pauseUpload(taskId: string): Promise<string> {
  return apiClient.post(`/uploads/${taskId}/pause`)
}

/**
 * 恢复上传任务
 */
export async function resumeUpload(taskId: string): Promise<string> {
  return apiClient.post(`/uploads/${taskId}/resume`)
}

/**
 * 删除上传任务
 */
export async function deleteUpload(taskId: string): Promise<string> {
  return apiClient.delete(`/uploads/${taskId}`)
}

/**
 * 清除已完成的任务
 */
export async function clearCompleted(): Promise<number> {
  return apiClient.post('/uploads/clear/completed')
}

/**
 * 清除失败的任务
 */
export async function clearFailed(): Promise<number> {
  return apiClient.post('/uploads/clear/failed')
}

/**
 * 计算上传进度百分比
 */
export function calculateProgress(task: UploadTask): number {
  if (task.total_size === 0) return 0
  return (task.uploaded_size / task.total_size) * 100
}

// 重新导出共享工具函数，保持向后兼容
export const formatFileSize = sharedFormatFileSize
export const formatSpeed = sharedFormatSpeed
export const formatETA = sharedFormatETA
export const extractFilename = sharedExtractFilename

/**
 * 计算剩余时间（秒）
 */
export function calculateETA(task: UploadTask): number | null {
  if (task.speed === 0 || task.uploaded_size >= task.total_size) {
    return null
  }
  const remaining = task.total_size - task.uploaded_size
  return Math.floor(remaining / task.speed)
}

/**
 * 获取状态文本
 */
export function getStatusText(status: UploadTaskStatus): string {
  const statusMap: Record<UploadTaskStatus, string> = {
    pending: '等待中',
    checking_rapid: '秒传检查中',
    encrypting: '加密中',
    uploading: '上传中',
    paused: '已暂停',
    completed: '已完成',
    rapid_upload_success: '秒传成功',
    failed: '失败',
  }
  return statusMap[status] || '未知'
}

/**
 * 获取状态类型（用于Element Plus组件）
 */
export function getStatusType(status: UploadTaskStatus): 'success' | 'warning' | 'danger' | 'info' {
  const typeMap: Record<UploadTaskStatus, 'success' | 'warning' | 'danger' | 'info'> = {
    pending: 'info',
    checking_rapid: 'warning',
    encrypting: 'warning',
    uploading: 'warning',
    paused: 'info',
    completed: 'success',
    rapid_upload_success: 'success',
    failed: 'danger',
  }
  return typeMap[status] || 'info'
}

