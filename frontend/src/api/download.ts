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
  // 文件夹下载相关字段
  group_id?: string
  group_root?: string
  relative_path?: string
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

// ============================================
// 文件夹下载相关类型和函数
// ============================================

/// 文件夹下载状态
export type FolderStatus = 'scanning' | 'downloading' | 'paused' | 'completed' | 'failed' | 'cancelled'

/// 文件夹下载任务组
export interface FolderDownload {
  id: string
  name: string
  remote_root: string
  local_root: string
  status: FolderStatus
  total_files: number
  total_size: number
  created_count: number
  completed_count: number
  downloaded_size: number
  scan_completed: boolean
  scan_progress?: string
  created_at: number
  started_at?: number
  completed_at?: number
  error?: string
}

/// 树形节点（用于展示）
export interface DownloadTreeNode {
  name: string
  path: string
  isFolder: boolean
  children: DownloadTreeNode[]
  tasks: DownloadTask[]
  // 聚合数据
  totalSize: number
  downloadedSize: number
  totalFiles: number
  completedFiles: number
}

/// 统一下载项（用于混合列表展示）
export interface DownloadItem {
  id: string
  name: string
  isFolder: boolean
  created_at: number
  status: TaskStatus | FolderStatus
  total_size: number
  downloaded_size: number
  speed: number
  // 文件夹特有
  folder?: FolderDownload
  total_files?: number
  completed_files?: number
  // 单文件特有
  task?: DownloadTask
}

/**
 * 创建文件夹下载
 */
export async function createFolderDownload(remotePath: string): Promise<string> {
  return apiClient.post('/downloads/folder', { path: remotePath })
}

/**
 * 获取所有文件夹下载
 */
export async function getAllFolderDownloads(): Promise<FolderDownload[]> {
  return apiClient.get('/downloads/folders')
}

/**
 * 获取指定文件夹下载详情
 */
export async function getFolderDownload(folderId: string): Promise<FolderDownload> {
  return apiClient.get(`/downloads/folder/${folderId}`)
}

/**
 * 暂停文件夹下载
 */
export async function pauseFolderDownload(folderId: string): Promise<string> {
  return apiClient.post(`/downloads/folder/${folderId}/pause`)
}

/**
 * 恢复文件夹下载
 */
export async function resumeFolderDownload(folderId: string): Promise<string> {
  return apiClient.post(`/downloads/folder/${folderId}/resume`)
}

/**
 * 取消文件夹下载
 */
export async function cancelFolderDownload(
    folderId: string,
    deleteFiles: boolean = false
): Promise<string> {
  return apiClient.delete(`/downloads/folder/${folderId}`, {
    params: { delete_files: deleteFiles },
  })
}

/**
 * 获取文件夹状态文本
 */
export function getFolderStatusText(status: FolderStatus): string {
  const map: Record<FolderStatus, string> = {
    scanning: '扫描中',
    downloading: '下载中',
    paused: '已暂停',
    completed: '已完成',
    failed: '失败',
    cancelled: '已取消',
  }
  return map[status] || status
}

/**
 * 获取文件夹状态类型
 */
export function getFolderStatusType(status: FolderStatus): 'success' | 'warning' | 'danger' | 'info' {
  const map: Record<FolderStatus, 'success' | 'warning' | 'danger' | 'info'> = {
    scanning: 'info',
    downloading: 'warning',
    paused: 'info',
    completed: 'success',
    failed: 'danger',
    cancelled: 'info',
  }
  return map[status] || 'info'
}

/**
 * 计算文件夹聚合速度
 */
export function calculateFolderSpeed(tasks: DownloadTask[]): number {
  return tasks.filter((t) => t.status === 'downloading').reduce((sum, t) => sum + t.speed, 0)
}

/**
 * 计算文件夹ETA
 */
export function calculateFolderETA(folder: FolderDownload, speed: number): number | null {
  if (speed <= 0) return null
  const remaining = folder.total_size - folder.downloaded_size
  return Math.ceil(remaining / speed)
}

/**
 * 根据 relative_path 构建树形结构
 */
export function buildDownloadTree(folderName: string, tasks: DownloadTask[]): DownloadTreeNode {
  const root: DownloadTreeNode = {
    name: folderName,
    path: '',
    isFolder: true,
    children: [],
    tasks: [],
    totalSize: 0,
    downloadedSize: 0,
    totalFiles: 0,
    completedFiles: 0,
  }

  for (const task of tasks) {
    if (!task.relative_path) continue

    const parts = task.relative_path.split('/')
    let current = root

    // 遍历路径创建文件夹节点
    for (let i = 0; i < parts.length - 1; i++) {
      const folderName = parts[i]
      let child = current.children.find((c) => c.name === folderName && c.isFolder)

      if (!child) {
        child = {
          name: folderName,
          path: parts.slice(0, i + 1).join('/'),
          isFolder: true,
          children: [],
          tasks: [],
          totalSize: 0,
          downloadedSize: 0,
          totalFiles: 0,
          completedFiles: 0,
        }
        current.children.push(child)
      }
      current = child
    }

    // 添加文件任务
    current.tasks.push(task)
  }

  // 递归计算聚合数据
  calculateTreeStats(root)

  return root
}

/**
 * 递归计算树节点的统计数据
 */
function calculateTreeStats(node: DownloadTreeNode): void {
  // 先递归计算子节点
  for (const child of node.children) {
    calculateTreeStats(child)
  }

  // 计算当前节点
  let totalSize = 0
  let downloadedSize = 0
  let totalFiles = 0
  let completedFiles = 0

  // 加上直接子任务
  for (const task of node.tasks) {
    totalSize += task.total_size
    downloadedSize += task.downloaded_size
    totalFiles += 1
    if (task.status === 'completed') {
      completedFiles += 1
    }
  }

  // 加上子文件夹
  for (const child of node.children) {
    totalSize += child.totalSize
    downloadedSize += child.downloadedSize
    totalFiles += child.totalFiles
    completedFiles += child.completedFiles
  }

  node.totalSize = totalSize
  node.downloadedSize = downloadedSize
  node.totalFiles = totalFiles
  node.completedFiles = completedFiles
}

/**
 * 合并文件任务和文件夹任务，按创建时间排序
 */
export function mergeDownloadItems(
    tasks: DownloadTask[],
    folders: FolderDownload[]
): DownloadItem[] {
  const items: DownloadItem[] = []

  // 添加单文件任务（排除属于文件夹的）
  for (const task of tasks) {
    if (!task.group_id) {
      items.push({
        id: task.id,
        name: task.remote_path.split('/').pop() || task.id,
        isFolder: false,
        created_at: task.created_at,
        status: task.status,
        total_size: task.total_size,
        downloaded_size: task.downloaded_size,
        speed: task.speed,
        task: task,
      })
    }
  }

  // 添加文件夹任务
  for (const folder of folders) {
    const folderTasks = tasks.filter((t) => t.group_id === folder.id)
    const speed = calculateFolderSpeed(folderTasks)
    const completedFiles = folderTasks.filter((t) => t.status === 'completed').length

    items.push({
      id: folder.id,
      name: folder.name,
      isFolder: true,
      created_at: folder.created_at,
      status: folder.status,
      total_size: folder.total_size,
      downloaded_size: folder.downloaded_size,
      speed: speed,
      folder: folder,
      total_files: folder.total_files,
      completed_files: completedFiles,
    })
  }

  // 按创建时间倒序排序（最新的在前面）
  items.sort((a, b) => b.created_at - a.created_at)

  return items
}

// ============================================
// 统一获取接口（推荐使用，由后端混合和排序）
// ============================================

/// 后端返回的统一下载项
export interface DownloadItemFromBackend {
  type: 'file' | 'folder'
  // 文件类型的字段（type=file时）
  id?: string
  fs_id?: number
  remote_path?: string
  local_path?: string
  total_size?: number
  downloaded_size?: number
  status?: TaskStatus | FolderStatus
  speed?: number
  created_at?: number
  started_at?: number
  completed_at?: number
  error?: string
  group_id?: string
  group_root?: string
  relative_path?: string
  // 文件夹类型的字段（type=folder时）
  name?: string
  remote_root?: string
  local_root?: string
  total_files?: number
  created_count?: number
  completed_count?: number
  scan_completed?: boolean
  scan_progress?: string
  completed_files?: number
}

/**
 * 获取所有下载（文件+文件夹混合，由后端排序）
 * 推荐使用此接口，一次请求获取所有数据
 */
export async function getAllDownloadsMixed(): Promise<DownloadItemFromBackend[]> {
  return apiClient.get('/downloads/all')
}
