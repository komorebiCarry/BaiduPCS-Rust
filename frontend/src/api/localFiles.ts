import axios from 'axios'
import type { ApiResponse, FileEntry, ListDirectoryResponse, SortField, SortOrder } from './filesystem'

const WEB_AUTH_ACCESS_TOKEN_KEY = 'web_auth_access_token'

const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 30000,
})

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

export type { FileEntry, ListDirectoryResponse, SortField, SortOrder }

export interface DeleteLocalFilesData {
  deleted_count: number
  failed_paths: string[]
}

export async function listLocalFiles(
    path: string = '',
    page: number = 0,
    pageSize: number = 100,
    sortField: SortField = 'name',
    sortOrder: SortOrder = 'asc'
): Promise<ListDirectoryResponse> {
  const response = await apiClient.get<ApiResponse<ListDirectoryResponse>>('/local-files', {
    params: { path, page, page_size: pageSize, sort_field: sortField, sort_order: sortOrder }
  })

  if (response.data.code !== 0 || !response.data.data) {
    throw new Error(response.data.message || '获取本地文件列表失败')
  }

  return response.data.data
}

export async function deleteLocalFiles(paths: string[]): Promise<DeleteLocalFilesData> {
  const response = await apiClient.post<ApiResponse<DeleteLocalFilesData>>('/local-files/delete', {
    paths
  })

  if (response.data.code !== 0 || !response.data.data) {
    throw new Error(response.data.message || '删除本地文件失败')
  }

  return response.data.data
}
