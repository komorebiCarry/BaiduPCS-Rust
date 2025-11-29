// 本地文件系统 API 封装

import axios from 'axios'

const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 30000,
})

export interface ApiResponse<T> {
  code: number
  message: string
  data?: T
}

/// 文件系统错误码
export const FS_ERROR_CODES = {
  PATH_NOT_ALLOWED: 50001,
  DIRECTORY_NOT_FOUND: 50002,
  PERMISSION_DENIED: 50003,
  SYMLINK_REJECTED: 50004,
  DIRECTORY_READ_FAILED: 50005,
  INVALID_PATH_FORMAT: 50006,
  PATH_TRAVERSAL_DETECTED: 50007,
  FILE_NOT_FOUND: 50008,
  NOT_A_DIRECTORY: 50009,
  NOT_A_FILE: 50010,
} as const

/// 错误码对应的友好提示
const ERROR_MESSAGES: Record<number, string> = {
  [FS_ERROR_CODES.PATH_NOT_ALLOWED]: '该路径不在允许访问的范围内',
  [FS_ERROR_CODES.DIRECTORY_NOT_FOUND]: '目录不存在',
  [FS_ERROR_CODES.PERMISSION_DENIED]: '没有权限访问该路径',
  [FS_ERROR_CODES.SYMLINK_REJECTED]: '不允许访问符号链接',
  [FS_ERROR_CODES.DIRECTORY_READ_FAILED]: '读取目录失败，请稍后重试',
  [FS_ERROR_CODES.INVALID_PATH_FORMAT]: '路径格式无效',
  [FS_ERROR_CODES.PATH_TRAVERSAL_DETECTED]: '非法路径访问',
  [FS_ERROR_CODES.FILE_NOT_FOUND]: '文件不存在',
  [FS_ERROR_CODES.NOT_A_DIRECTORY]: '指定路径不是目录',
  [FS_ERROR_CODES.NOT_A_FILE]: '指定路径不是文件',
}

/// 获取友好错误信息
export function getFriendlyErrorMessage(code: number, fallback?: string): string {
  return ERROR_MESSAGES[code] || fallback || '操作失败'
}

/// 文件条目类型
export type EntryType = 'file' | 'directory'

/// 文件条目
export interface FileEntry {
  id: string
  name: string
  entryType: EntryType       // 后端返回 "entryType"
  size: number | null
  createdAt: string          // 后端返回 camelCase
  updatedAt: string          // 后端返回 camelCase
  icon?: string
  path: string
}

/// 排序字段
export type SortField = 'name' | 'created_at' | 'updated_at' | 'size'

/// 排序顺序
export type SortOrder = 'asc' | 'desc'

/// 列目录请求（支持分页）
export interface ListDirectoryRequest {
  path: string
  page?: number
  page_size?: number
  sort_field?: SortField
  sort_order?: SortOrder
}

/// 列目录响应（支持分页）
export interface ListDirectoryResponse {
  entries: FileEntry[]
  currentPath: string         // 后端返回 camelCase
  parentPath: string | null   // 后端返回 camelCase
  total: number
  page: number
  pageSize: number            // 后端返回 camelCase
  hasMore: boolean            // 后端返回 camelCase
}

/// 路径跳转请求
export interface GotoPathRequest {
  path: string
}

/// 路径跳转响应
export interface GotoPathResponse {
  valid: boolean
  resolvedPath: string        // 后端返回 camelCase
  entryType: EntryType | null // 后端返回 camelCase
  message: string | null
}

/// 校验请求
export interface ValidatePathRequest {
  path: string
  type?: EntryType            // 后端接收 "type"
}

/// 校验响应
export interface ValidatePathResponse {
  valid: boolean
  exists: boolean
  entryType: EntryType | null // 后端返回 "entryType"
  message: string | null
}

/**
 * 列出目录内容（支持分页）
 */
export async function listDirectory(req: ListDirectoryRequest): Promise<ListDirectoryResponse> {
  const response = await apiClient.get<ApiResponse<ListDirectoryResponse>>('/fs/list', {
    params: req
  })

  if (response.data.code !== 0 || !response.data.data) {
    const message = getFriendlyErrorMessage(response.data.code, response.data.message)
    throw new Error(message)
  }

  return response.data.data
}

/**
 * 路径跳转（直达路径）
 */
export async function gotoPath(req: GotoPathRequest): Promise<GotoPathResponse> {
  const response = await apiClient.get<ApiResponse<GotoPathResponse>>('/fs/goto', {
    params: req
  })

  if (response.data.code !== 0 || !response.data.data) {
    const message = getFriendlyErrorMessage(response.data.code, response.data.message)
    throw new Error(message)
  }

  return response.data.data
}

/**
 * 校验路径
 */
export async function validatePath(req: ValidatePathRequest): Promise<ValidatePathResponse> {
  const response = await apiClient.get<ApiResponse<ValidatePathResponse>>('/fs/validate', {
    params: req
  })

  if (response.data.code !== 0 || !response.data.data) {
    const message = getFriendlyErrorMessage(response.data.code, response.data.message)
    throw new Error(message)
  }

  return response.data.data
}

/**
 * 获取根目录列表
 */
export async function getRoots(): Promise<FileEntry[]> {
  const response = await apiClient.get<ApiResponse<FileEntry[]>>('/fs/roots')

  if (response.data.code !== 0 || !response.data.data) {
    const message = getFriendlyErrorMessage(response.data.code, response.data.message)
    throw new Error(message)
  }

  return response.data.data
}

/**
 * 格式化文件大小
 */
export function formatFileSize(bytes: number | null): string {
  if (bytes === null || bytes === 0) return '-'

  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))

  return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i]
}

/**
 * 格式化时间
 */
export function formatTime(isoString: string): string {
  const date = new Date(isoString)
  return date.toLocaleString('zh-CN')
}

/**
 * 获取文件图标
 */
export function getFileIcon(entry: FileEntry): string {
  if (entry.entryType === 'directory') {
    return 'folder'
  }

  const ext = entry.name.split('.').pop()?.toLowerCase()
  const iconMap: Record<string, string> = {
    // 图片
    jpg: 'image', jpeg: 'image', png: 'image', gif: 'image', bmp: 'image', webp: 'image', svg: 'image',
    // 视频
    mp4: 'video', avi: 'video', mkv: 'video', mov: 'video', wmv: 'video', flv: 'video',
    // 音频
    mp3: 'audio', wav: 'audio', flac: 'audio', aac: 'audio', ogg: 'audio',
    // 文档
    pdf: 'pdf', doc: 'word', docx: 'word', xls: 'excel', xlsx: 'excel', ppt: 'ppt', pptx: 'ppt',
    txt: 'text', md: 'text', json: 'code', xml: 'code', html: 'code', css: 'code', js: 'code', ts: 'code',
    // 压缩
    zip: 'archive', rar: 'archive', '7z': 'archive', tar: 'archive', gz: 'archive',
    // 可执行
    exe: 'executable', msi: 'executable', dmg: 'executable', app: 'executable',
  }

  return iconMap[ext || ''] || 'file'
}