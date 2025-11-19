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
export type TaskStatus = 'pending' | 'downloading' | 'paused' | 'completed' | 'failed'

/// 下载任务
export interface DownloadTask {
  id: string
  fs_id: number
  remote_path: string
  local_path: string
  total_size: number
  downloaded_size: number
  status: TaskStatus
  speed: number
  created_at: number
  started_at?: number
  completed_at?: number
  error?: string
}

/// 创建下载任务请求
export interface CreateDownloadRequest {
  fs_id: number
  remote_path: string
  filename: string
  total_size: number
}

/**
 * 创建下载任务
 */
export async function createDownload(req: CreateDownloadRequest): Promise<string> {
  return apiClient.post('/downloads', req)
}

/**
 * 获取所有下载任务
 */
export async function getAllDownloads(): Promise<DownloadTask[]> {
  return apiClient.get('/downloads')
}

/**
 * 获取指定下载任务
 */
export async function getDownload(taskId: string): Promise<DownloadTask> {
  return apiClient.get(`/downloads/${taskId}`)
}

/**
 * 暂停下载任务
 */
export async function pauseDownload(taskId: string): Promise<string> {
  return apiClient.post(`/downloads/${taskId}/pause`)
}

/**
 * 恢复下载任务
 */
export async function resumeDownload(taskId: string): Promise<string> {
  return apiClient.post(`/downloads/${taskId}/resume`)
}

/**
 * 删除下载任务
 * @param taskId 任务ID
 * @param deleteFile 是否删除本地文件
 */
export async function deleteDownload(taskId: string, deleteFile: boolean = false): Promise<string> {
  return apiClient.delete(`/downloads/${taskId}`, { params: { delete_file: deleteFile } })
}

/**
 * 清除已完成的任务
 */
export async function clearCompleted(): Promise<number> {
  return apiClient.delete('/downloads/clear/completed')
}

/**
 * 清除失败的任务
 */
export async function clearFailed(): Promise<number> {
  return apiClient.delete('/downloads/clear/failed')
}

/**
 * 计算下载进度百分比
 */
export function calculateProgress(task: DownloadTask): number {
  if (task.total_size === 0) return 0
  return (task.downloaded_size / task.total_size) * 100
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
export function calculateETA(task: DownloadTask): number | null {
  if (task.speed === 0 || task.downloaded_size >= task.total_size) {
    return null
  }
  const remaining = task.total_size - task.downloaded_size
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
export function getStatusText(status: TaskStatus): string {
  const statusMap: Record<TaskStatus, string> = {
    pending: '等待中',
    downloading: '下载中',
    paused: '已暂停',
    completed: '已完成',
    failed: '失败',
  }
  return statusMap[status] || '未知'
}

/**
 * 获取状态类型（用于Element Plus组件）
 */
export function getStatusType(status: TaskStatus): 'success' | 'warning' | 'danger' | 'info' {
  const typeMap: Record<TaskStatus, 'success' | 'warning' | 'danger' | 'info'> = {
    pending: 'info',
    downloading: 'warning',
    paused: 'info',
    completed: 'success',
    failed: 'danger',
  }
  return typeMap[status] || 'info'
}

