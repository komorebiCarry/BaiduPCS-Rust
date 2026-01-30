/**
 * 离线下载（Cloud Download）API 封装
 *
 * 本模块提供离线下载功能的前端 API 接口，包括：
 * - 添加离线下载任务
 * - 查询任务列表
 * - 查询单个任务详情
 * - 取消任务
 * - 删除任务
 * - 清空任务记录
 * - 手动刷新任务列表
 */

import { apiClient } from './client'

// =====================================================
// TypeScript 接口类型定义
// =====================================================

/**
 * 离线下载任务状态码
 *
 * 状态码对应百度网盘 API 返回的 status 字段
 */
export enum CloudDlTaskStatus {
  /** 下载成功 */
  Success = 0,
  /** 下载进行中 */
  Running = 1,
  /** 系统错误 */
  SystemError = 2,
  /** 资源不存在 */
  ResourceNotFound = 3,
  /** 下载超时 */
  Timeout = 4,
  /** 资源存在但下载失败 */
  DownloadFailed = 5,
  /** 存储空间不足 */
  InsufficientSpace = 6,
  /** 任务取消 */
  Cancelled = 7,
}

/**
 * 离线下载文件信息
 */
export interface CloudDlFileInfo {
  /** 文件名 */
  file_name: string
  /** 文件大小（字节） */
  file_size: number
}

/**
 * 离线下载任务信息
 */
export interface CloudDlTaskInfo {
  /** 任务唯一标识 */
  task_id: number
  /** 任务状态码 (0-7) */
  status: number
  /** 状态文本描述 */
  status_text: string
  /** 文件总大小（字节） */
  file_size: number
  /** 已下载大小（字节） */
  finished_size: number
  /** 创建时间戳（秒） */
  create_time: number
  /** 开始时间戳（秒） */
  start_time: number
  /** 完成时间戳（秒，未完成时为 0） */
  finish_time: number
  /** 网盘保存路径 */
  save_path: string
  /** 下载源链接 */
  source_url: string
  /** 任务名称（通常是文件名） */
  task_name: string
  /** 离线下载类型（0=普通，其他值表示特殊类型） */
  od_type: number
  /** 文件列表 */
  file_list: CloudDlFileInfo[]
  /** 结果码 */
  result: number
}

/**
 * 添加离线下载任务请求
 */
export interface AddTaskRequest {
  /** 下载源链接（支持 HTTP/HTTPS/磁力链接/ed2k） */
  source_url: string
  /** 网盘保存路径（默认为根目录 "/"） */
  save_path?: string
  /** 是否启用自动下载到本地 */
  auto_download?: boolean
  /** 本地下载目录（自动下载时使用） */
  local_download_path?: string
  /** 完成时是否询问下载目录 */
  ask_download_path?: boolean
}

/**
 * 添加任务响应
 */
export interface AddTaskResponse {
  /** 新创建的任务 ID */
  task_id: number
}

/**
 * 任务列表响应
 */
export interface TaskListResponse {
  /** 任务列表 */
  tasks: CloudDlTaskInfo[]
}

/**
 * 清空任务响应
 */
export interface ClearTasksResponse {
  /** 清空的任务数量 */
  total: number
}

/**
 * 通用操作响应
 */
export interface OperationResponse {
  /** 操作是否成功 */
  success: boolean
  /** 可选的消息 */
  message?: string
}

/**
 * 自动下载配置
 */
export interface AutoDownloadConfig {
  /** 关联的任务 ID */
  task_id: number
  /** 是否启用自动下载 */
  enabled: boolean
  /** 本地下载目录（为空时使用默认目录） */
  local_path?: string
  /** 完成时是否每次询问下载目录 */
  ask_each_time: boolean
}

// =====================================================
// API 方法
// =====================================================

/**
 * 添加离线下载任务
 *
 * @param req 添加任务请求
 * @returns 添加任务响应，包含新创建的任务 ID
 */
export async function addTask(req: AddTaskRequest): Promise<AddTaskResponse> {
  return apiClient.post('/cloud-dl/tasks', req)
}

/**
 * 获取离线下载任务列表
 *
 * @returns 任务列表响应
 */
export async function listTasks(): Promise<TaskListResponse> {
  return apiClient.get('/cloud-dl/tasks')
}

/**
 * 查询单个离线下载任务详情
 *
 * @param taskId 任务 ID
 * @returns 任务详情
 */
export async function queryTask(taskId: number): Promise<CloudDlTaskInfo> {
  return apiClient.get(`/cloud-dl/tasks/${taskId}`)
}

/**
 * 取消离线下载任务
 *
 * @param taskId 任务 ID
 * @returns 操作响应
 */
export async function cancelTask(taskId: number): Promise<OperationResponse> {
  return apiClient.post(`/cloud-dl/tasks/${taskId}/cancel`)
}

/**
 * 删除离线下载任务
 *
 * @param taskId 任务 ID
 * @returns 操作响应
 */
export async function deleteTask(taskId: number): Promise<OperationResponse> {
  return apiClient.delete(`/cloud-dl/tasks/${taskId}`)
}

/**
 * 清空离线下载任务记录
 *
 * @returns 清空任务响应，包含清空的任务数量
 */
export async function clearTasks(): Promise<ClearTasksResponse> {
  return apiClient.delete('/cloud-dl/tasks/clear')
}

/**
 * 手动刷新离线下载任务列表
 *
 * 触发后台监听服务立即刷新任务列表，并通过 WebSocket 推送更新。
 *
 * @returns 任务列表响应
 */
export async function refreshTasks(): Promise<TaskListResponse> {
  return apiClient.post('/cloud-dl/tasks/refresh')
}

// =====================================================
// 工具函数
// =====================================================

/**
 * 获取状态文本
 *
 * @param status 状态码 (0-7)
 * @returns 状态的中文描述文本
 */
export function getStatusText(status: number): string {
  const statusMap: Record<number, string> = {
    [CloudDlTaskStatus.Success]: '下载成功',
    [CloudDlTaskStatus.Running]: '下载进行中',
    [CloudDlTaskStatus.SystemError]: '系统错误',
    [CloudDlTaskStatus.ResourceNotFound]: '资源不存在',
    [CloudDlTaskStatus.Timeout]: '下载超时',
    [CloudDlTaskStatus.DownloadFailed]: '下载失败',
    [CloudDlTaskStatus.InsufficientSpace]: '存储空间不足',
    [CloudDlTaskStatus.Cancelled]: '已取消',
  }
  return statusMap[status] || '未知状态'
}

/**
 * 获取状态类型（用于 Element Plus Tag 组件）
 *
 * @param status 状态码 (0-7)
 * @returns Element Plus Tag 组件的 type 属性值
 */
export function getStatusType(status: number): 'success' | 'warning' | 'danger' | 'info' {
  switch (status) {
    case CloudDlTaskStatus.Success:
      return 'success'
    case CloudDlTaskStatus.Running:
      return 'warning'
    case CloudDlTaskStatus.Cancelled:
      return 'info'
    default:
      // SystemError, ResourceNotFound, Timeout, DownloadFailed, InsufficientSpace
      return 'danger'
  }
}

/**
 * 计算下载进度百分比
 *
 * @param task 任务信息
 * @returns 进度百分比 (0-100)
 */
export function calculateProgress(task: CloudDlTaskInfo): number {
  if (task.file_size <= 0) {
    return 0
  }
  return Math.min(100, (task.finished_size / task.file_size) * 100)
}

/**
 * 判断任务是否已完成（成功或失败）
 *
 * @param status 状态码
 * @returns 是否已完成
 */
export function isTaskFinished(status: number): boolean {
  return status !== CloudDlTaskStatus.Running
}

/**
 * 判断任务是否成功
 *
 * @param status 状态码
 * @returns 是否成功
 */
export function isTaskSuccess(status: number): boolean {
  return status === CloudDlTaskStatus.Success
}

/**
 * 判断任务是否失败
 *
 * @param status 状态码
 * @returns 是否失败
 */
export function isTaskFailed(status: number): boolean {
  return (
    status === CloudDlTaskStatus.SystemError ||
    status === CloudDlTaskStatus.ResourceNotFound ||
    status === CloudDlTaskStatus.Timeout ||
    status === CloudDlTaskStatus.DownloadFailed ||
    status === CloudDlTaskStatus.InsufficientSpace ||
    status === CloudDlTaskStatus.Cancelled
  )
}

/**
 * 格式化时间戳为可读字符串
 *
 * @param timestamp 时间戳（秒）
 * @returns 格式化后的时间字符串
 */
export function formatTimestamp(timestamp: number): string {
  if (timestamp <= 0) {
    return '-'
  }
  const date = new Date(timestamp * 1000)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  })
}
