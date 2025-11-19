// 认证API封装

import axios from 'axios'

const apiClient = axios.create({
  baseURL: '/api/v1',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json'
  }
})

// 响应拦截器
apiClient.interceptors.response.use(
  response => response.data,
  error => {
    console.error('API Error:', error)
    return Promise.reject(error)
  }
)

export interface ApiResponse<T> {
  code: number
  message: string
  data?: T
}

export interface QRCode {
  sign: string
  image_base64: string
  qrcode_url: string
  created_at: number
}

export interface UserAuth {
  uid: number
  username: string
  nickname?: string
  avatar_url?: string
  vip_type?: number
  total_space?: number
  used_space?: number
  bduss: string
  stoken?: string
  ptoken?: string
  cookies?: string
  login_time: number
}

export interface QRCodeStatus {
  status: 'waiting' | 'scanned' | 'success' | 'expired' | 'failed'
  user?: UserAuth
  token?: string
  reason?: string
}

/**
 * 生成登录二维码
 */
export async function generateQRCode(): Promise<QRCode> {
  const response = (await apiClient.post('/auth/qrcode/generate')) as ApiResponse<QRCode>
  if (response.code !== 0 || !response.data) {
    throw new Error(response.message || '生成二维码失败')
  }
  return response.data
}

/**
 * 查询扫码状态
 */
export async function getQRCodeStatus(sign: string): Promise<QRCodeStatus> {
  const response = (await apiClient.get(`/auth/qrcode/status?sign=${sign}`)) as ApiResponse<QRCodeStatus>
  if (response.code !== 0 || !response.data) {
    throw new Error(response.message || '查询状态失败')
  }
  return response.data
}

/**
 * 获取当前用户信息
 */
export async function getCurrentUser(): Promise<UserAuth> {
  const response = (await apiClient.get('/auth/user')) as ApiResponse<UserAuth>
  if (response.code === 0 && response.data) {
    return response.data
  }
  // 如果没有数据或者返回错误码，抛出异常
  throw new Error(response.message || '获取用户信息失败')
}

/**
 * 登出
 */
export async function logout(): Promise<void> {
  await apiClient.post('/auth/logout')
}
