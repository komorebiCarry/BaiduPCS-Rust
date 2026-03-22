import axios from 'axios'
import { ElMessage } from 'element-plus'

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
  default_directory?: string       // 用户设置的默认下载目录
  recent_directory?: string        // 最近使用的下载目录
  ask_each_time: boolean           // 每次下载时是否询问保存位置
  max_global_threads: number       // 全局最大线程数
  chunk_size_mb: number            // 分片大小
  max_concurrent_tasks: number     // 最大同时下载数
  max_retries: number              // 最大重试次数
  cdn_refresh?: CdnRefreshConfig   // CDN 刷新配置
}

/// 上传配置
export interface UploadConfig {
  max_global_threads: number       // 全局最大线程数
  chunk_size_mb: number            // 分片大小(4-32MB)
  max_concurrent_tasks: number     // 最大同时上传数
  max_retries: number              // 最大重试次数
  skip_hidden_files: boolean       // 上传文件夹时是否跳过隐藏文件
  recent_directory?: string        // 最近使用的上传源目录
}

/// 转存配置
export interface TransferConfig {
  default_behavior: string      // 'transfer_only' | 'transfer_and_download'
  recent_save_fs_id?: number    // 最近使用的网盘目录 fs_id
  recent_save_path?: string     // 最近使用的网盘目录路径
}

/// 文件系统配置
export interface FilesystemConfig {
  allowed_paths?: string[]
  default_path?: string
  show_hidden?: boolean
  follow_symlinks?: boolean
}

/// 持久化配置
export interface PersistenceConfig {
  wal_dir?: string
  db_path?: string
  wal_flush_interval_ms?: number
  auto_recover_tasks?: boolean
  wal_retention_days?: number
  history_archive_hour?: number
  history_archive_minute?: number
  history_retention_days?: number
}

/// 日志配置
export interface LogConfig {
  enabled?: boolean
  log_dir?: string
  retention_days?: number
  level?: string
  max_file_size?: number
}

/// 上传备份触发配置
export interface UploadTriggerConfig {
  watch_enabled?: boolean
  watch_debounce_ms?: number
  watch_recursive?: boolean
  fallback_interval_enabled?: boolean
  fallback_interval_minutes?: number
  fallback_scheduled_enabled?: boolean
  fallback_scheduled_hour?: number
  fallback_scheduled_minute?: number
}

/// 下载备份触发配置
export interface DownloadTriggerConfig {
  poll_mode?: string
  poll_interval_minutes?: number
  poll_scheduled_hour?: number
  poll_scheduled_minute?: number
}

/// 自动备份配置
export interface AutoBackupConfig {
  enabled?: boolean
  max_concurrent_scans?: number
  max_concurrent_encrypts?: number
  max_concurrent_backup_tasks?: number
  change_aggregation_window_secs?: number
  temp_dir?: string
  config_path?: string
  upload_trigger?: UploadTriggerConfig
  download_trigger?: DownloadTriggerConfig
}

/// Web 访问认证配置
export interface WebAuthConfig {
  enabled?: boolean
  mode?: string
}

/// 分享直下配置
export interface ShareDirectDownloadConfig {
  temp_dir?: string
  auto_cleanup?: boolean
  cleanup_on_failure?: boolean
  cleanup_orphaned_on_startup?: boolean
}

/// CDN 刷新配置
export interface CdnRefreshConfig {
  enabled?: boolean
  refresh_interval_minutes?: number
  min_refresh_interval_secs?: number
  speed_drop_threshold_percent?: number
  speed_drop_duration_secs?: number
  baseline_establish_secs?: number
  stagnation_threshold_kbps?: number
  stagnation_ratio_percent?: number
  min_threads_for_detection?: number
  startup_delay_secs?: number
}

/// 代理类型
export type ProxyType = 'none' | 'http' | 'socks5'

/// 代理配置
export interface ProxyConfig {
  proxy_type: ProxyType
  host: string
  port: number
  username?: string
  password?: string
  allow_fallback?: boolean
}

/// 网络配置
export interface NetworkConfig {
  proxy: ProxyConfig
}

/// 冲突策略配置
export interface ConflictStrategyConfig {
  default_upload_strategy: 'smart_dedup' | 'auto_rename' | 'overwrite'
  default_download_strategy: 'overwrite' | 'skip' | 'auto_rename'
}

/// 应用配置
export interface AppConfig {
  server: ServerConfig
  download: DownloadConfig
  upload: UploadConfig
  transfer?: TransferConfig
  filesystem?: FilesystemConfig
  persistence?: PersistenceConfig
  log?: LogConfig
  autobackup?: AutoBackupConfig
  web_auth?: WebAuthConfig
  share_direct_download?: ShareDirectDownloadConfig
  network?: NetworkConfig
  conflict_strategy?: ConflictStrategyConfig
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

/**
 * 更新最近目录请求参数
 */
export interface UpdateRecentDirRequest {
  dir_type: 'download' | 'upload'
  path: string
}

/**
 * 更新最近使用的目录
 */
export async function updateRecentDir(req: UpdateRecentDirRequest): Promise<string> {
  return apiClient.post('/config/recent-dir', req)
}

// ============================================
// 防抖优化：最近目录更新
// ============================================

// 防抖定时器
let recentDirDebounceTimer: ReturnType<typeof setTimeout> | null = null

// 待处理的请求
let pendingRecentDirRequest: UpdateRecentDirRequest | null = null

// 防抖延迟（毫秒）
const RECENT_DIR_DEBOUNCE_DELAY = 1000 // 1秒

/**
 * 防抖版本的更新最近目录
 * 在指定延迟内的多次调用只会执行最后一次
 */
export function updateRecentDirDebounced(req: UpdateRecentDirRequest): void {
  // 保存最新的请求
  pendingRecentDirRequest = req

  // 清除之前的定时器
  if (recentDirDebounceTimer) {
    clearTimeout(recentDirDebounceTimer)
  }

  // 设置新的定时器
  recentDirDebounceTimer = setTimeout(async () => {
    if (pendingRecentDirRequest) {
      try {
        await updateRecentDir(pendingRecentDirRequest)
        console.log('最近目录已更新:', pendingRecentDirRequest.path)
      } catch (error) {
        console.error('更新最近目录失败:', error)
      }
      pendingRecentDirRequest = null
    }
    recentDirDebounceTimer = null
  }, RECENT_DIR_DEBOUNCE_DELAY)
}

/**
 * 立即执行待处理的最近目录更新（用于组件卸载前）
 */
export async function flushRecentDirUpdate(): Promise<void> {
  if (recentDirDebounceTimer) {
    clearTimeout(recentDirDebounceTimer)
    recentDirDebounceTimer = null
  }

  if (pendingRecentDirRequest) {
    try {
      await updateRecentDir(pendingRecentDirRequest)
    } catch (error) {
      console.error('更新最近目录失败:', error)
    }
    pendingRecentDirRequest = null
  }
}

/**
 * 设置默认下载目录请求参数
 */
export interface SetDefaultDirRequest {
  path: string
}

/**
 * 设置默认下载目录
 */
export async function setDefaultDownloadDir(req: SetDefaultDirRequest): Promise<string> {
  return apiClient.post('/config/default-download-dir', req)
}

// ============================================
// 转存配置 API
// ============================================

/**
 * 获取转存配置
 */
export async function getTransferConfig(): Promise<TransferConfig> {
  return apiClient.get('/config/transfer')
}

/**
 * 更新转存配置请求
 */
export interface UpdateTransferConfigRequest {
  default_behavior?: string
  recent_save_fs_id?: number
  recent_save_path?: string
}

/**
 * 更新转存配置
 */
export async function updateTransferConfig(req: UpdateTransferConfigRequest): Promise<string> {
  return apiClient.put('/config/transfer', req)
}


// ============================================
// 代理运行状态 API
// ============================================

/// 代理运行状态
export type ProxyRuntimeStatus = 'normal' | 'fallen_back_to_direct' | 'probing' | 'no_proxy'

/// 代理运行状态响应
export interface ProxyStatusResponse {
  status: ProxyRuntimeStatus
  flap_count: number
  next_probe_in_secs: number | null
}

/**
 * 获取代理运行状态
 */
export async function getProxyStatus(): Promise<ProxyStatusResponse> {
  return apiClient.get('/proxy/status')
}

/// 代理测试连接响应
export interface ProxyTestResponse {
  success: boolean
  latency_ms: number | null
  error: string | null
}

/**
 * 测试代理连接（不影响当前代理状态）
 */
export async function testProxyConnection(config: ProxyConfig): Promise<ProxyTestResponse> {
  return apiClient.post('/proxy/test', config, { timeout: 15000 })
}
