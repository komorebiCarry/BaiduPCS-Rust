import axios from 'axios'
import { ElMessage } from 'element-plus'

const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 30000,
})

// 响应拦截器
apiClient.interceptors.response.use(
  (response) => {
    const { code, message } = response.data
    if (code !== 0) {
      ElMessage.error(message || '请求失败')
      return Promise.reject(new Error(message || '请求失败'))
    }
    return response.data.data
  },
  (error) => {
    ElMessage.error(error.response?.data?.message || error.message || '网络错误')
    return Promise.reject(error)
  }
)

/// 任务状态
export type UploadTaskStatus = 'pending' | 'uploading' | 'paused' | 'completed' | 'failed'

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
}

/// 创建上传任务请求
export interface CreateUploadRequest {
  local_path: string
  remote_path: string
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
}

/// 批量创建上传任务请求
export interface CreateBatchUploadRequest {
  files: [string, string][] // [(本地路径, 远程路径)]
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

/**
 * 格式化文件大小
 */
export function formatFileSize(bytes: number): string {
  if (bytes === 0) return '0 B'
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(1024))
  return `${(bytes / Math.pow(1024, i)).toFixed(2)} ${sizes[i]}`
}

/**
 * 格式化速度
 */
export function formatSpeed(bytesPerSec: number): string {
  return `${formatFileSize(bytesPerSec)}/s`
}

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
 * 格式化剩余时间
 */
export function formatETA(seconds: number | null): string {
  if (seconds === null || seconds === 0) {
    return '即将完成'
  }

  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const secs = seconds % 60

  if (hours > 0) {
    return `${hours}小时${minutes}分钟`
  } else if (minutes > 0) {
    return `${minutes}分钟${secs}秒`
  } else {
    return `${secs}秒`
  }
}

/**
 * 获取状态文本
 */
export function getStatusText(status: UploadTaskStatus): string {
  const statusMap: Record<UploadTaskStatus, string> = {
    pending: '等待中',
    uploading: '上传中',
    paused: '已暂停',
    completed: '已完成',
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
    uploading: 'warning',
    paused: 'info',
    completed: 'success',
    failed: 'danger',
  }
  return typeMap[status] || 'info'
}

/**
 * 从文件名中提取文件名（去除路径）
 */
export function extractFilename(path: string): string {
  const parts = path.replace(/\\/g, '/').split('/')
  return parts[parts.length - 1] || path
}
