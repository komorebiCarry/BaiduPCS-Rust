// 分享API封装

import { apiClient } from './client'

// =====================================================
// 接口定义
// =====================================================

/**
 * 创建分享参数
 */
export interface CreateShareParams {
  /** 文件路径列表 */
  paths: string[]
  /** 有效期（0=永久, 1=1天, 7=7天, 30=30天） */
  period: number
  /** 提取码（4位字符，可选，不提供则自动生成） */
  pwd?: string
}

/**
 * 分享创建结果
 */
export interface ShareResult {
  /** 分享链接 */
  link: string
  /** 提取码 */
  pwd: string
  /** 分享ID */
  shareid: number
}

/**
 * 分享记录
 */
export interface ShareRecord {
  /** 分享ID */
  shareId: number
  /** 文件ID列表 */
  fsIds: number[]
  /** 短链接 */
  shortlink: string
  /** 状态（0=正常, 其他=异常） */
  status: number
  /** 是否公开（0=私密, 1=公开） */
  public: number
  /** 文件类型 */
  typicalCategory: number
  /** 文件路径 */
  typicalPath: string
  /** 过期状态（-1=已过期, 0=有期限且未过期, 1=永久有效） */
  expiredType: number
  /** 过期时间点（Unix 秒级时间戳，0=永久或已过期） */
  expiredTime: number
  /** 浏览次数 */
  viewCount: number
  /** 下载次数（百度 dCnt；接口未返回时缺省，界面不显示） */
  downloadCount?: number
  /** 保存（转存）次数（百度 tCnt；接口未返回时缺省，界面不显示） */
  transferCount?: number
}

/**
 * 分享列表数据
 */
export interface ShareListData {
  /** 分享记录列表 */
  list: ShareRecord[]
  /** 总数 */
  total: number
  /** 当前页码 */
  page: number
}

/**
 * 分享详情数据
 */
export interface ShareDetailData {
  /** 提取码 */
  pwd: string
  /** 短链接 */
  shorturl: string
}

/**
 * 取消分享结果
 */
export interface CancelShareResult {
  /** 是否成功 */
  success: boolean
}

/**
 * 分享有效期选项（与网页端一致）：1 / 3 / 7 / 30 天，-1 = 永久有效
 */
export type ShareExpireDays = 1 | 3 | 7 | 30 | -1

export const SHARE_EXPIRE_OPTIONS: ReadonlyArray<{ days: ShareExpireDays; label: string }> = [
  { days: 1, label: '1天有效' },
  { days: 3, label: '3天有效' },
  { days: 7, label: '7天有效' },
  { days: 30, label: '30天有效' },
  { days: -1, label: '永久有效' },
]

/**
 * 单条分享修改有效期结果
 */
export interface ShareExpireItemResult {
  shareId: number
  success: boolean
  /** 新的过期状态（成功时返回，语义同 ShareRecord.expiredType） */
  expiredType?: number
  /** 新的过期时间点（Unix 秒，永久为 0；成功时返回） */
  expiredTime?: number
  /** 百度返回的过期时间文本（北京时间，可能缺省） */
  expireDate?: string
  /** 失败原因 */
  error?: string
}

/**
 * 修改分享有效期结果
 */
export interface ModifyShareExpireResult {
  results: ShareExpireItemResult[]
  successCount: number
  failedCount: number
}

// =====================================================
// API 函数
// =====================================================

/**
 * 创建分享
 * @param params 创建分享参数
 * @returns 分享结果
 */
export async function createShare(params: CreateShareParams): Promise<ShareResult> {
  return apiClient.post('/shares', params)
}

/**
 * 取消分享
 * @param shareIds 分享ID列表
 * @returns 取消结果
 */
export async function cancelShare(shareIds: number[]): Promise<CancelShareResult> {
  return apiClient.post('/shares/cancel', { share_ids: shareIds })
}

/**
 * 修改分享有效期（单条或批量）
 * @param shareIds 分享ID列表
 * @param days 1 / 3 / 7 / 30，-1 = 永久有效
 */
export async function modifyShareExpire(
  shareIds: number[],
  days: ShareExpireDays,
): Promise<ModifyShareExpireResult> {
  // 后端逐条调用百度接口：按条数放宽超时，避免批量时前端先超时而实际已修改
  return apiClient.post(
    '/shares/expire',
    { share_ids: shareIds, days },
    { timeout: 30000 + 5000 * shareIds.length },
  )
}

/**
 * 获取分享列表
 * @param page 页码（从1开始，默认1）
 * @returns 分享列表数据
 */
export async function getShareList(page: number = 1): Promise<ShareListData> {
  return apiClient.get('/shares', {
    params: { page }
  })
}

/**
 * 获取分享详情
 * @param shareId 分享ID
 * @returns 分享详情数据
 */
export async function getShareDetail(shareId: number): Promise<ShareDetailData> {
  return apiClient.get(`/shares/${shareId}`)
}

// =====================================================
// 工具函数
// =====================================================

/**
 * 生成随机提取码（4位字母数字）
 * @returns 4位随机提取码
 */
export function generatePwd(): string {
  const charset = 'abcdefghijklmnopqrstuvwxyz0123456789'
  let pwd = ''
  for (let i = 0; i < 4; i++) {
    const randomIndex = Math.floor(Math.random() * charset.length)
    pwd += charset[randomIndex]
  }
  return pwd
}
