import { apiClient } from './client'
import { formatFileSize, formatSpeed, formatETA, extractFilename } from './utils'

// 重新导出工具函数，保持向后兼容
export { formatFileSize, formatSpeed, formatETA, extractFilename }

/// 任务状态
export type TaskStatus = 'pending' | 'downloading' | 'decrypting' | 'paused' | 'completed' | 'failed'

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
  /** 🔥 新增：关联的转存任务 ID（如果此下载任务由转存任务自动创建） */
  transfer_task_id?: string
  // 解密相关字段
  is_encrypted?: boolean
  decrypt_progress?: number
  decrypted_path?: string
  original_filename?: string
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

// ============================================
// 批量下载相关类型和函数
// ============================================

/// 批量下载项
export interface BatchDownloadItem {
  /// 文件系统ID
  fs_id: number
  /// 远程路径
  path: string
  /// 文件/文件夹名称
  name: string
  /// 是否为目录
  is_dir: boolean
  /// 文件大小（文件夹为 undefined 或 0）
  size?: number
  /// 原始名称（加密文件/文件夹的还原名称）
  original_name?: string
}

/// 批量下载请求
export interface CreateBatchDownloadRequest {
  /// 下载项列表
  items: BatchDownloadItem[]
  /// 本地下载目录
  target_dir: string
}

/// 批量下载错误项
export interface BatchDownloadError {
  /// 文件/文件夹路径
  path: string
  /// 失败原因
  reason: string
}

/// 批量下载响应
export interface BatchDownloadResponse {
  /// 成功创建的单文件任务ID列表
  task_ids: string[]
  /// 成功创建的文件夹任务ID列表
  folder_task_ids: string[]
  /// 失败的项
  failed: BatchDownloadError[]
}

/**
 * 批量下载文件/文件夹
 * @param req 批量下载请求
 * @returns 批量下载响应
 */
export async function createBatchDownload(req: CreateBatchDownloadRequest): Promise<BatchDownloadResponse> {
  return apiClient.post('/downloads/batch', req)
}

/**
 * 计算下载进度百分比
 */
export function calculateProgress(task: DownloadTask): number {
  if (task.total_size === 0) return 0
  return (task.downloaded_size / task.total_size) * 100
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
 * 获取状态文本
 */
export function getStatusText(status: TaskStatus): string {
  const statusMap: Record<TaskStatus, string> = {
    pending: '等待中',
    downloading: '下载中',
    decrypting: '解密中',
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
    decrypting: 'warning',
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
 * @param remotePath 远程路径
 * @param originalName 原始文件夹名（如果是加密文件夹，传入还原后的名称）
 */
export async function createFolderDownload(remotePath: string, originalName?: string): Promise<string> {
  return apiClient.post('/downloads/folder', { path: remotePath, original_name: originalName })
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
  /** 🔥 新增：关联的转存任务 ID（如果此下载任务由转存任务自动创建） */
  transfer_task_id?: string
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
  // 解密相关字段
  /** 是否为加密文件 */
  is_encrypted?: boolean
  /** 解密进度 (0.0 - 100.0) */
  decrypt_progress?: number
  /** 解密后的文件路径 */
  decrypted_path?: string
  /** 原始文件名（解密后恢复的文件名） */
  original_filename?: string
}

/**
 * 获取所有下载（文件+文件夹混合，由后端排序）
 * 推荐使用此接口，一次请求获取所有数据
 */
export async function getAllDownloadsMixed(): Promise<DownloadItemFromBackend[]> {
  return apiClient.get('/downloads/all')
}

// ============================================
// 批量操作相关类型和函数
// ============================================

export interface BatchOperationRequest {
  task_ids?: string[]
  all?: boolean
  delete_files?: boolean
}

export interface BatchOperationResponse {
  total: number
  success_count: number
  failed_count: number
  results: { task_id: string; success: boolean; error?: string }[]
}

/** 批量暂停下载 */
export async function batchPauseDownloads(req: BatchOperationRequest): Promise<BatchOperationResponse> {
  return apiClient.post('/downloads/batch/pause', req)
}

/** 批量恢复下载 */
export async function batchResumeDownloads(req: BatchOperationRequest): Promise<BatchOperationResponse> {
  return apiClient.post('/downloads/batch/resume', req)
}

/** 批量删除下载 */
export async function batchDeleteDownloads(req: BatchOperationRequest): Promise<BatchOperationResponse> {
  return apiClient.post('/downloads/batch/delete', req)
}
