import { apiClientWithErrorCode } from './client'
import { formatTimestampShort } from './utils'

const apiClient = apiClientWithErrorCode

// ============================================
// 业务错误码
// ============================================
export const TransferErrorCodes = {
  /** 需要提取码 */
  NEED_PASSWORD: 1001,
  /** 提取码错误 */
  INVALID_PASSWORD: 1002,
  /** 分享已失效 */
  SHARE_EXPIRED: 1003,
  /** 分享不存在 */
  SHARE_NOT_FOUND: 1004,
  /** 转存管理器未初始化 */
  MANAGER_NOT_READY: 1005,
  /** 任务不存在 */
  TASK_NOT_FOUND: 1006,
} as const

// ============================================
// 类型定义
// ============================================

/// 转存任务状态
export type TransferStatus =
    | 'queued'
    | 'checking_share'
    | 'transferring'
    | 'transfer_failed'
    | 'transferred'
    | 'downloading'
    | 'download_failed'
    | 'completed'

/// 分享页面信息
export interface SharePageInfo {
  shareid: string
  uk: string
  share_uk: string
  bdstoken: string
}

/// 分享文件信息
export interface SharedFileInfo {
  fs_id: number
  is_dir: boolean
  path: string
  size: number
  name: string
}

/// 转存任务
export interface TransferTask {
  id: string
  share_url: string
  password?: string
  save_path: string
  save_fs_id: number
  auto_download: boolean
  local_download_path?: string
  status: TransferStatus
  error?: string
  download_task_ids: string[]
  share_info?: SharePageInfo
  file_list: SharedFileInfo[]
  transferred_count: number
  total_count: number
  created_at: number
  updated_at: number
  failed_download_ids: string[]
  completed_download_ids: string[]
  download_started_at?: number
  /** 🔥 新增：转存文件名称（用于展示，从分享文件列表中提取） */
  file_name?: string
}

/// 创建转存任务请求
export interface CreateTransferRequest {
  share_url: string
  password?: string
  save_path: string
  save_fs_id: number
  auto_download?: boolean
  local_download_path?: string
}

/// 创建转存任务响应
export interface CreateTransferResponse {
  task_id?: string
  status?: TransferStatus
  need_password: boolean
}

/// 转存任务列表响应
export interface TransferListResponse {
  tasks: TransferTask[]
  total: number
}

/// 转存 API 错误
export interface TransferApiError {
  code: number
  message: string
  data?: any
}

// ============================================
// API 函数
// ============================================

/**
 * 创建转存任务
 * @throws TransferApiError 特殊错误（需要密码、密码错误等）
 */
export async function createTransfer(req: CreateTransferRequest): Promise<CreateTransferResponse> {
  return apiClient.post('/transfers', req)
}

/**
 * 获取所有转存任务
 */
export async function getAllTransfers(): Promise<TransferListResponse> {
  return apiClient.get('/transfers')
}

/**
 * 获取单个转存任务
 */
export async function getTransfer(taskId: string): Promise<TransferTask> {
  return apiClient.get(`/transfers/${taskId}`)
}

/**
 * 删除转存任务
 */
export async function deleteTransfer(taskId: string): Promise<string> {
  return apiClient.delete(`/transfers/${taskId}`)
}

/**
 * 取消转存任务
 */
export async function cancelTransfer(taskId: string): Promise<string> {
  return apiClient.post(`/transfers/${taskId}/cancel`)
}

// ============================================
// 辅助函数
// ============================================

/**
 * 获取状态文本
 */
export function getTransferStatusText(status: TransferStatus): string {
  const statusMap: Record<TransferStatus, string> = {
    queued: '排队中',
    checking_share: '检查分享...',
    transferring: '转存中',
    transfer_failed: '转存失败',
    transferred: '已转存',
    downloading: '下载中',
    download_failed: '下载失败',
    completed: '已完成',
  }
  return statusMap[status] || '未知'
}

/**
 * 获取状态类型（用于 Element Plus 组件）
 */
export function getTransferStatusType(status: TransferStatus): 'success' | 'warning' | 'danger' | 'info' {
  const typeMap: Record<TransferStatus, 'success' | 'warning' | 'danger' | 'info'> = {
    queued: 'info',
    checking_share: 'info',
    transferring: 'warning',
    transfer_failed: 'danger',
    transferred: 'success',
    downloading: 'warning',
    download_failed: 'danger',
    completed: 'success',
  }
  return typeMap[status] || 'info'
}

/**
 * 计算转存进度百分比
 */
export function calculateTransferProgress(task: TransferTask): number {
  if (task.total_count === 0) return 0
  return (task.transferred_count / task.total_count) * 100
}

/**
 * 判断是否为终止状态
 */
export function isTerminalStatus(status: TransferStatus): boolean {
  return ['transfer_failed', 'transferred', 'download_failed', 'completed'].includes(status)
}

/**
 * 判断错误码是否为需要密码
 */
export function isNeedPasswordError(error: TransferApiError): boolean {
  return error.code === TransferErrorCodes.NEED_PASSWORD
}

/**
 * 判断错误码是否为密码错误
 */
export function isInvalidPasswordError(error: TransferApiError): boolean {
  return error.code === TransferErrorCodes.INVALID_PASSWORD
}

/**
 * 获取简短的分享链接显示
 */
export function getShortShareUrl(url: string): string {
  // 从 URL 中提取关键部分
  const match = url.match(/\/s\/([a-zA-Z0-9_-]+)/)
  if (match) {
    return `pan.baidu.com/s/${match[1].substring(0, 8)}...`
  }
  // 回退：截断显示
  if (url.length > 40) {
    return url.substring(0, 37) + '...'
  }
  return url
}

/**
 * 格式化时间戳
 */
export const formatTransferTime = formatTimestampShort
