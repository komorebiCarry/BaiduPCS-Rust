// 文件API封装

import axios from 'axios'
import { formatFileSize as sharedFormatFileSize, formatTimestamp } from './utils'

const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 10000,
})

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

// 重新导出共享工具函数，保持向后兼容
export const formatFileSize = sharedFormatFileSize
export const formatTime = formatTimestamp

