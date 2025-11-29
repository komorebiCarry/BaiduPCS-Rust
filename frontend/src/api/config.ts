import axios from 'axios'
import { ElMessage } from 'element-plus'

const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 10000,
})

// 响应拦截器
apiClient.interceptors.response.use(
    (response) => {
      const { code, message, data } = response.data
      if (code !== 0) {
        ElMessage.error(message || '请求失败')
        return Promise.reject(new Error(message || '请求失败'))
      }
      return data
    },
    (error) => {
      ElMessage.error(error.response?.data?.message || error.message || '网络错误')
      return Promise.reject(error)
    }
)

/// 服务器配置
export interface ServerConfig {
  host: string
  port: number
  cors_origins: string[]
}

/// 下载配置
export interface DownloadConfig {
  download_dir: string
  max_global_threads: number       // 全局最大线程数
  chunk_size_mb: number            // 分片大小
  max_concurrent_tasks: number     // 最大同时下载数
  max_retries: number              // 最大重试次数
}

/// 上传配置
export interface UploadConfig {
  max_global_threads: number       // 全局最大线程数
  chunk_size_mb: number            // 分片大小(4-32MB)
  max_concurrent_tasks: number     // 最大同时上传数
  max_retries: number              // 最大重试次数
  skip_hidden_files: boolean       // 上传文件夹时是否跳过隐藏文件
}

/// 应用配置
export interface AppConfig {
  server: ServerConfig
  download: DownloadConfig
  upload: UploadConfig
}

/// VIP 推荐配置
export interface VipRecommendedConfig {
  threads: number
  chunk_size: number
  max_tasks: number
  file_size_limit_gb: number
}

/// 推荐配置响应
export interface RecommendedConfigResponse {
  vip_type: number
  vip_name: string
  recommended: VipRecommendedConfig
  warnings: string[]
}

/**
 * 获取当前配置
 */
export async function getConfig(): Promise<AppConfig> {
  return apiClient.get('/config')
}

/**
 * 更新配置
 */
export async function updateConfig(config: AppConfig): Promise<string> {
  return apiClient.put('/config', config)
}

/**
 * 获取推荐配置
 */
export async function getRecommendedConfig(): Promise<RecommendedConfigResponse> {
  return apiClient.get('/config/recommended')
}

/**
 * 恢复为推荐配置
 */
export async function resetToRecommended(): Promise<string> {
  return apiClient.post('/config/reset')
}

