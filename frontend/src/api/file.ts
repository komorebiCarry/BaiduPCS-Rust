// 文件API封装

import axios from 'axios'
import { formatFileSize as sharedFormatFileSize, formatTimestamp } from './utils'

// 本地存储键名（与 webAuth store 保持一致）
const WEB_AUTH_ACCESS_TOKEN_KEY = 'web_auth_access_token'

const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 10000,
})

// 添加 Web 认证拦截器
apiClient.interceptors.request.use(
    (config) => {
      const token = localStorage.getItem(WEB_AUTH_ACCESS_TOKEN_KEY)
      if (token) {
        config.headers.Authorization = `Bearer ${token}`
      }
      return config
    },
    (error) => Promise.reject(error)
)

export interface ApiResponse<T> {
  code: number
  message: string
  data?: T
}

export interface FileItem {
  fs_id: number
  path: string
  server_filename: string
  size: number
  isdir: number
  category: number
  md5?: string
  server_ctime: number
  server_mtime: number
  local_ctime: number
  local_mtime: number
  // 加密文件相关字段
  is_encrypted: boolean
  is_encrypted_folder: boolean
  original_name?: string
  original_size?: number
}

export interface FileListData {
  list: FileItem[]
  dir: string
  page: number
  total: number
  has_more: boolean
}

export interface DownloadUrlData {
  fs_id: number
  url: string
}

export interface CreateFolderData {
  fs_id: number
  path: string
  isdir: number
}

/**
 * 获取文件列表
 */
export async function getFileList(
    dir: string = '/',
    page: number = 1,
    pageSize: number = 50
): Promise<FileListData> {
  const response = await apiClient.get<ApiResponse<FileListData>>('/files', {
    params: { dir, page, page_size: pageSize }
  })

  if (response.data.code !== 0 || !response.data.data) {
    throw new Error(response.data.message || '获取文件列表失败')
  }

  return response.data.data
}

/**
 * 获取下载链接
 */
export async function getDownloadUrl(fsId: number): Promise<string> {
  const response = await apiClient.get<ApiResponse<DownloadUrlData>>('/files/download', {
    params: { fs_id: fsId }
  })

  if (response.data.code !== 0 || !response.data.data) {
    throw new Error(response.data.message || '获取下载链接失败')
  }

  return response.data.data.url
}

/**
 * 创建文件夹
 */
export async function createFolder(path: string): Promise<CreateFolderData> {
  const response = await apiClient.post<ApiResponse<CreateFolderData>>('/files/folder', {
    path
  })

  if (response.data.code !== 0 || !response.data.data) {
    throw new Error(response.data.message || '创建文件夹失败')
  }

  return response.data.data
}

export interface DeleteFilesData {
  deleted_count: number
  failed_paths: string[]
}

/**
 * 删除文件
 */
export async function deleteFiles(paths: string[]): Promise<DeleteFilesData> {
  const response = await apiClient.post<ApiResponse<DeleteFilesData>>('/files/delete', {
    paths
  })

  if (response.data.code !== 0 || !response.data.data) {
    const err = new Error(response.data.message || '删除文件失败') as Error & { code?: number }
    err.code = response.data.code
    throw err
  }

  return response.data.data
}

export interface SearchData {
  list: FileItem[]
  has_more: boolean
}

/**
 * 搜索文件
 */
export async function searchFiles(
    key: string,
    page: number = 1,
    num: number = 100,
    recursion: number = 1
): Promise<SearchData> {
  const response = await apiClient.get<ApiResponse<SearchData>>('/files/search', {
    params: { key, page, num, recursion }
  })

  if (response.data.code !== 0 || !response.data.data) {
    throw new Error(response.data.message || '搜索文件失败')
  }

  return response.data.data
}

// 重新导出共享工具函数，保持向后兼容
export const formatFileSize = sharedFormatFileSize
export const formatTime = formatTimestamp

// =====================================================
// 文件管理操作（filemanager: copy / move / rename）
// =====================================================

import { createApiClient } from './client'

/**
 * 长轮询客户端：复用 `createApiClient` 工厂以共享 419 token 刷新拦截器与
 * `response.data.data` 自动剥层逻辑；超时设置较长以容纳后端 share/taskquery
 * 多次轮询合计耗时。
 *
 * 超时边界计算：后端 MAX_ATTEMPTS=60，阶梯式延迟
 *   attempt 2-5:  ~1s × 4  =  4s
 *   attempt 6-10: ~2s × 5  = 10s
 *   attempt 11-60: ~5s × 50 = 250s
 * 再叠加 60 次 HTTP 往返（每次几十-几百 ms）与 handler 重试 warmup，
 * 最坏情况下后端单次请求耗时可达 ~310s+。前端超时设为 360s，确保能收到
 * 后端结构化 `still_running=true` 结果而不是 axios 自己 timeout。
 */
const longPollClient = createApiClient({ timeout: 360_000, showErrorMessage: false })

/** copy/move 单条 item（与后端 `FileOperationItem` 一一对应） */
export interface FileOperationItem {
  /** 源文件路径，必须以 `/` 开头且非根 */
  path: string
  /** 目标父目录，必须以 `/` 开头 */
  dest: string
  /** 目标文件名（与源同名 = 直接复制/移动；不同 = 复制/移动并重命名） */
  newname: string
}

/** rename 单条 item（与后端 `RenameItem` 一一对应） */
export interface RenameItem {
  path: string
  newname: string
  /** fs_id（必须是数字，不能是字符串） */
  id: number
}

/** taskquery 完成时返回的单条结果 */
export interface FileOperationResultItem {
  from: string
  to: string
}

/**
 * 风控验证组件信息（与后端 `AuthWidget` 字段一致；全部字段均可缺失）
 *
 * 字段对齐 Rust 端 `Option<String>`：未返回时为 `null` 或 `undefined`。
 * 不要再进一步收紧成 `string`。
 */
export interface AuthWidget {
  saferand?: string | null
  safesign?: string | null
  safetpl?: string | null
  /** 透传未识别字段，前端不应依赖具体字段名 */
  [key: string]: unknown
}

/**
 * 文件管理操作的统一结果 DTO
 *
 * `kind` 决定使用哪个分支；`HTTP code` 始终为 200，业务成败仅由 `kind` 表达。
 */
export type FileOperationOutcomeDto =
    | {
  kind: 'success'
  taskid: number
  total: number
  list: FileOperationResultItem[]
}
    | {
  kind: 'failed'
  message: string
  taskid: number
  errno?: number | null
  task_errno?: number | null
  authwidget?: AuthWidget | null
  verify_scene?: number | null
  /** 60 次轮询仍未完成（仅 taskquery 阶段使用） */
  still_running: boolean
}

/**
 * 批量复制文件
 *
 * 路径必须以 `/` 开头；`newname` 由前端先经 `validateFilename` 校验。
 * 返回值的 `kind` 字段用于区分业务成功/失败；网络层错误才会 reject。
 */
export async function copyFiles(items: FileOperationItem[]): Promise<FileOperationOutcomeDto> {
  return longPollClient.post('/files/copy', { items })
}

/**
 * 批量移动文件
 */
export async function moveFiles(items: FileOperationItem[]): Promise<FileOperationOutcomeDto> {
  return longPollClient.post('/files/move', { items })
}

/**
 * 重命名单个文件 / 文件夹
 */
export async function renameFile(item: RenameItem): Promise<FileOperationOutcomeDto> {
  return longPollClient.post('/files/rename', { item })
}

/**
 * 文件名校验（前端版本，必须与后端 `validate_filename` 完全一致）
 *
 * 拒绝条件：
 * - 空字符串
 * - UTF-16 长度 > 255（与后端 `encode_utf16().count()` 对齐）
 * - `.` / `..`
 * - 含 `/` `\` `:` `*` `?` `"` `<` `>` `|` 任一字符
 * - 含 ASCII 控制字符（0x00-0x1F / 0x7F）
 * - 首/尾空格
 * - 尾部 `.`
 * - Windows 保留名（CON / PRN / AUX / NUL / COM1-9 / LPT1-9，含大小写、含扩展名）
 *
 * 返回 `null` 表示通过；返回 `string` 为错误描述。
 */
export function validateFilename(name: string): string | null {
  if (name.length === 0) return '名称不能为空'
  // String.length 即 UTF-16 code units 长度，与后端 encode_utf16().count() 一致
  if (name.length > 255) return '名称过长（最长 255 个字符）'
  if (name === '.' || name === '..') return `名称不能为 ${name}`

  const ILLEGAL = /[/\\:*?"<>|]/
  const m = name.match(ILLEGAL)
  if (m) return `名称含非法字符: ${m[0]}`

  for (let i = 0; i < name.length; i++) {
    const c = name.charCodeAt(i)
    if (c < 0x20 || c === 0x7f) return '名称含控制字符'
  }

  if (name.startsWith(' ') || name.endsWith(' ')) return '名称不能以空格开头或结尾'
  if (name.endsWith('.')) return '名称不能以 . 结尾'

  const stem = (name.split('.')[0] ?? name).toUpperCase()
  const RESERVED = new Set([
    'CON', 'PRN', 'AUX', 'NUL',
    'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
    'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9',
  ])
  if (RESERVED.has(stem)) return `名称不能使用系统保留词: ${stem}`

  return null
}

/**
 * 路径归一化：去尾斜杠（除根路径），保留 leading `/`
 */
export function normalizePath(p: string): string {
  if (!p) return ''
  if (p === '/') return '/'
  return p.replace(/\/+$/, '') || '/'
}

/**
 * 拼接父目录与文件名
 */
export function joinPath(parent: string, name: string): string {
  const base = normalizePath(parent)
  if (base === '/') return `/${name}`
  return `${base}/${name}`
}

/**
 * 取路径 leaf 名（最后一段）
 */
export function basename(p: string): string {
  const norm = normalizePath(p)
  const idx = norm.lastIndexOf('/')
  return idx === -1 ? norm : norm.slice(idx + 1)
}

/**
 * 取路径父目录
 */
export function dirname(p: string): string {
  const norm = normalizePath(p)
  const idx = norm.lastIndexOf('/')
  if (idx <= 0) return '/'
  return norm.slice(0, idx)
}
