import axios, { type AxiosInstance, type AxiosResponse, type AxiosError, type InternalAxiosRequestConfig } from 'axios'
import { ElMessage } from 'element-plus'
import { backendHealth, isBackendDownError } from '../utils/backendHealth'

// 后端断开 / 恢复时的统一提示文案（避免轮询失败刷屏）
const BACKEND_DOWN_MESSAGE = '无法连接后端服务，正在尝试重连…'
const BACKEND_RECOVERED_MESSAGE = '后端服务已恢复连接'

/** 成功请求：刷新后端健康态；若刚从断开态恢复且允许提示，提示一次 */
function reportRequestSuccess(notify: boolean): void {
    const recovered = backendHealth.recordSuccess()
    if (recovered && notify) {
        ElMessage.success(BACKEND_RECOVERED_MESSAGE)
    }
}

/**
 * 失败请求：若为断连类错误（网络不可达 / 5xx），更新健康态并接管提示。
 * - 阈值内的偶发失败：返回 false，交由调用方照常提示一次
 * - 刚跨入断开态：用统一文案提示一次（notify 时），返回 true 抑制调用方
 * - 已处于断开态：返回 true 抑制重复刷屏
 * 非断连类（业务 4xx）错误返回 false，不影响健康态。
 */
function reportRequestFailureSuppressed(error: AxiosError, notify: boolean): boolean {
    if (!isBackendDownError(error)) {
        return false
    }
    const wasDown = backendHealth.getStatus() === 'down'
    const justWentDown = backendHealth.recordFailure()
    if (justWentDown) {
        if (notify) {
            ElMessage.error(BACKEND_DOWN_MESSAGE)
        }
        return true
    }
    return wasDown
}

// ============ Web 认证令牌管理 ============

// 本地存储键名（与 webAuth store 保持一致）
const WEB_AUTH_ACCESS_TOKEN_KEY = 'web_auth_access_token'
const WEB_AUTH_REFRESH_TOKEN_KEY = 'web_auth_refresh_token'

// Web 认证专用 HTTP 状态码（与后端保持一致）
// 419 表示 Web 认证令牌过期，区别于 401 百度账号认证失败
const WEB_AUTH_EXPIRED_STATUS = 419

// 令牌刷新状态
let isRefreshing = false
let refreshSubscribers: Array<(token: string) => void> = []

/**
 * 获取 Web 认证访问令牌
 */
function getWebAuthAccessToken(): string | null {
    return localStorage.getItem(WEB_AUTH_ACCESS_TOKEN_KEY)
}

/**
 * 获取 Web 认证刷新令牌
 */
function getWebAuthRefreshToken(): string | null {
    return localStorage.getItem(WEB_AUTH_REFRESH_TOKEN_KEY)
}

/**
 * 保存令牌到本地存储
 */
function saveTokens(accessToken: string, refreshToken: string, accessExpiresAt: number, refreshExpiresAt: number): void {
    localStorage.setItem(WEB_AUTH_ACCESS_TOKEN_KEY, accessToken)
    localStorage.setItem(WEB_AUTH_REFRESH_TOKEN_KEY, refreshToken)
    localStorage.setItem('web_auth_access_expires_at', accessExpiresAt.toString())
    localStorage.setItem('web_auth_refresh_expires_at', refreshExpiresAt.toString())
}

/**
 * 清除所有 Web 认证令牌
 */
function clearWebAuthTokens(): void {
    localStorage.removeItem(WEB_AUTH_ACCESS_TOKEN_KEY)
    localStorage.removeItem(WEB_AUTH_REFRESH_TOKEN_KEY)
    localStorage.removeItem('web_auth_access_expires_at')
    localStorage.removeItem('web_auth_refresh_expires_at')
}

/**
 * 跳转到 Web 登录页
 */
function redirectToWebLogin(): void {
    if (window.location.pathname !== '/web-login') {
        clearWebAuthTokens()
        window.location.href = '/web-login'
    }
}

/**
 * 刷新令牌
 */
async function refreshTokens(): Promise<string | null> {
    const refreshToken = getWebAuthRefreshToken()
    if (!refreshToken) {
        return null
    }

    try {
        const response = await axios.post('/api/v1/web-auth/refresh', {
            refresh_token: refreshToken
        })

        const { access_token, refresh_token, access_expires_at, refresh_expires_at } = response.data
        saveTokens(access_token, refresh_token, access_expires_at, refresh_expires_at)
        return access_token
    } catch (error) {
        console.error('令牌刷新失败:', error)
        return null
    }
}

/**
 * 通知所有等待刷新的请求
 */
function onRefreshed(token: string): void {
    refreshSubscribers.forEach(callback => callback(token))
    refreshSubscribers = []
}

/**
 * 添加等待刷新的请求
 */
function addRefreshSubscriber(callback: (token: string) => void): void {
    refreshSubscribers.push(callback)
}

/**
 * 为 axios 实例添加 Web 认证拦截器
 * 自动在请求头中添加 Authorization header
 * 处理 419 Web 认证失败，尝试刷新令牌后重试请求
 */
function addWebAuthInterceptor(client: AxiosInstance): void {
    // 请求拦截器：添加 Authorization header
    client.interceptors.request.use(
        (config) => {
            const token = getWebAuthAccessToken()
            if (token) {
                config.headers.Authorization = `Bearer ${token}`
            }
            return config
        },
        (error) => Promise.reject(error)
    )

    // 响应拦截器：处理 419 Web 认证失败
    client.interceptors.response.use(
        (response) => response,
        async (error: AxiosError) => {
            const originalRequest = error.config as InternalAxiosRequestConfig & { _retry?: boolean }

            // 如果是 419 错误且未重试过
            if (error.response?.status === WEB_AUTH_EXPIRED_STATUS && originalRequest && !originalRequest._retry) {
                // 如果正在刷新，等待刷新完成后重试
                if (isRefreshing) {
                    return new Promise((resolve) => {
                        addRefreshSubscriber((token: string) => {
                            originalRequest.headers.Authorization = `Bearer ${token}`
                            resolve(client(originalRequest))
                        })
                    })
                }

                originalRequest._retry = true
                isRefreshing = true

                try {
                    const newToken = await refreshTokens()

                    if (newToken) {
                        // 刷新成功，通知等待的请求并重试原始请求
                        onRefreshed(newToken)
                        originalRequest.headers.Authorization = `Bearer ${newToken}`
                        return client(originalRequest)
                    } else {
                        // 刷新失败（没有 refresh token 或刷新接口返回错误）
                        redirectToWebLogin()
                        return Promise.reject(error)
                    }
                } catch (refreshError) {
                    // 刷新过程出错
                    redirectToWebLogin()
                    return Promise.reject(refreshError)
                } finally {
                    isRefreshing = false
                }
            }

            return Promise.reject(error)
        }
    )
}

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

    // 添加 Web 认证拦截器（包含 401 处理）
    addWebAuthInterceptor(client)

    // 响应拦截器
    client.interceptors.response.use(
        (response: AxiosResponse) => {
            reportRequestSuccess(showErrorMessage)
            const { code, message } = response.data
            if (code !== 0) {
                if (showErrorMessage) {
                    ElMessage.error(message || '请求失败')
                }
                return Promise.reject(new Error(message || '请求失败'))
            }
            return response.data.data
        },
        (error: AxiosError) => {
            // 419 已在 addWebAuthInterceptor 中处理跳转
            if (error.response?.status === WEB_AUTH_EXPIRED_STATUS) {
                // 显示提示信息
                if (showErrorMessage) {
                    ElMessage.error('Web 认证令牌已过期，请重新登录')
                }
                return Promise.reject(new Error('Web 认证令牌已过期'))
            }

            // 后端断连类错误：集中节流提示，避免轮询失败刷屏
            const suppressed = reportRequestFailureSuppressed(error, showErrorMessage)

            // 优先使用后端返回的 message，避免显示原始 HTTP 错误信息
            const errorData = error.response?.data as { message?: string; error?: string } | undefined
            const errorMessage = errorData?.message
                || errorData?.error
                || (error.response?.status ? `请求失败 (${error.response.status})` : '网络错误')

            if (showErrorMessage && !suppressed) {
                ElMessage.error(errorMessage)
            }
            return Promise.reject(new Error(errorMessage))
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

    // 添加 Web 认证拦截器（包含 401 处理）
    addWebAuthInterceptor(client)

    // 响应拦截器 - 返回完整错误信息让调用方处理
    client.interceptors.response.use(
        (response: AxiosResponse) => {
            reportRequestSuccess(true)
            const { code, message, data } = response.data
            if (code !== 0) {
                return Promise.reject({ code, message, data })
            }
            return response.data.data
        },
        (error: AxiosError) => {
            // 419 已在 addWebAuthInterceptor 中处理跳转
            if (error.response?.status === WEB_AUTH_EXPIRED_STATUS) {
                ElMessage.error('Web 认证令牌已过期，请重新登录')
                return Promise.reject(new Error('Web 认证令牌已过期'))
            }

            // 后端断连类错误：集中节流提示，避免轮询失败刷屏
            const suppressed = reportRequestFailureSuppressed(error, true)

            // 优先使用后端返回的 message，避免显示原始 HTTP 错误信息
            const errorData = error.response?.data as { message?: string; error?: string } | undefined
            const errorMessage = errorData?.message
                || errorData?.error
                || (error.response?.status ? `请求失败 (${error.response.status})` : '网络错误')

            if (!suppressed) {
                ElMessage.error(errorMessage)
            }
            return Promise.reject(new Error(errorMessage))
        }
    )

    return client
}

// 默认 API 客户端实例
export const apiClient = createApiClient()

// 支持错误码的 API 客户端实例（用于转存模块）
export const apiClientWithErrorCode = createApiClientWithErrorCode()

/**
 * 创建原始 API 客户端（不处理响应格式，直接返回 axios 响应）
 * 用于自动备份等使用 { success, data, error } 格式的 API
 */
export function createRawApiClient(options: { timeout?: number } = {}): AxiosInstance {
    const { timeout = 30000 } = options

    const client = axios.create({
        baseURL: '/api/v1',
        timeout,
    })

    // 添加 Web 认证拦截器（包含 401 处理）
    addWebAuthInterceptor(client)

    // 只处理网络错误，不处理业务响应格式
    // 从 response.data 中提取错误信息，让调用方决定如何处理
    client.interceptors.response.use(
        (response: AxiosResponse) => {
            // 原始客户端不弹提示，仅维护健康态供轮询退避使用
            reportRequestSuccess(false)
            return response
        },
        (error: AxiosError) => {
            // 419 已在 addWebAuthInterceptor 中处理跳转
            if (error.response?.status === WEB_AUTH_EXPIRED_STATUS) {
                return Promise.reject(new Error('Web 认证令牌已过期'))
            }

            // 维护后端健康态（不弹提示，由调用方决定如何显示）
            reportRequestFailureSuppressed(error, false)

            // 从 response.data 中提取错误信息（支持 message 或 error 字段）
            const errorData = error.response?.data as { message?: string; error?: string } | undefined
            const errorMessage = errorData?.message
                || errorData?.error
                || error.message
                || '网络错误'

            // 抛出包含错误信息的 Error 对象，让调用方决定如何显示
            return Promise.reject(new Error(errorMessage))
        }
    )

    return client
}

// 原始 API 客户端实例（用于自动备份模块）
export const rawApiClient = createRawApiClient()
