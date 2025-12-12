import axios, { type AxiosInstance, type AxiosResponse } from 'axios'
import { ElMessage } from 'element-plus'

/**
 * 创建统一的 API 客户端
 * 避免在各个 API 模块中重复创建 axios 实例和拦截器
 */
export function createApiClient(options: { timeout?: number; showErrorMessage?: boolean } = {}): AxiosInstance {
  const { timeout = 30000, showErrorMessage = true } = options

  const client = axios.create({
    baseURL: '/api/v1',
    timeout,
  })

  // 响应拦截器
  client.interceptors.response.use(
    (response: AxiosResponse) => {
      const { code, message } = response.data
      if (code !== 0) {
        if (showErrorMessage) {
          ElMessage.error(message || '请求失败')
        }
        return Promise.reject(new Error(message || '请求失败'))
      }
      return response.data.data
    },
    (error) => {
      if (showErrorMessage) {
        ElMessage.error(error.response?.data?.message || error.message || '网络错误')
      }
      return Promise.reject(error)
    }
  )

  return client
}

/**
 * 创建支持业务错误码的 API 客户端（用于转存等需要处理特殊错误码的场景）
 */
export function createApiClientWithErrorCode(options: { timeout?: number } = {}): AxiosInstance {
  const { timeout = 30000 } = options

  const client = axios.create({
    baseURL: '/api/v1',
    timeout,
  })

  // 响应拦截器 - 返回完整错误信息让调用方处理
  client.interceptors.response.use(
    (response: AxiosResponse) => {
      const { code, message, data } = response.data
      if (code !== 0) {
        return Promise.reject({ code, message, data })
      }
      return response.data.data
    },
    (error) => {
      ElMessage.error(error.response?.data?.message || error.message || '网络错误')
      return Promise.reject(error)
    }
  )

  return client
}

// 默认 API 客户端实例
export const apiClient = createApiClient()

// 支持错误码的 API 客户端实例（用于转存模块）
export const apiClientWithErrorCode = createApiClientWithErrorCode()
