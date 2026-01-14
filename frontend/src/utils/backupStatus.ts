/**
 * 备份状态展示工具模块
 * 参考文档 9.4.5 节
 */

import type { BackupTaskStatus } from '@/api/autobackup'

/** 备份状态展示配置 */
export interface BackupStatusDisplay {
  /** 状态文本 */
  text: string
  /** 状态颜色 */
  color: string
  /** 图标名称 */
  icon: string
  /** 是否显示加载动画 */
  loading: boolean
}

/** 备份子阶段展示配置 */
export interface BackupSubPhaseDisplay {
  /** 子阶段文本 */
  text: string
  /** 子阶段颜色 */
  color: string
}

/** 备份状态配置映射 */
const statusConfigMap: Record<BackupTaskStatus, BackupStatusDisplay> = {
  queued: {
    text: '等待中',
    color: '#909399',
    icon: 'Clock',
    loading: false,
  },
  preparing: {
    text: '准备中',
    color: '#E6A23C',
    icon: 'Loading',
    loading: true,
  },
  transferring: {
    text: '传输中',
    color: '#409EFF',
    icon: 'Loading',
    loading: true,
  },
  completed: {
    text: '已完成',
    color: '#67C23A',
    icon: 'CircleCheck',
    loading: false,
  },
  partially_completed: {
    text: '部分完成',
    color: '#E6A23C',
    icon: 'WarningFilled',
    loading: false,
  },
  failed: {
    text: '失败',
    color: '#F56C6C',
    icon: 'CircleClose',
    loading: false,
  },
  cancelled: {
    text: '已取消',
    color: '#909399',
    icon: 'Remove',
    loading: false,
  },
  paused: {
    text: '已暂停',
    color: '#E6A23C',
    icon: 'VideoPause',
    loading: false,
  },
}

/** 备份子阶段配置映射 */
const subPhaseConfigMap: Record<string, BackupSubPhaseDisplay> = {
  dedup_checking: {
    text: '去重检查中',
    color: '#909399',
  },
  waiting_slot: {
    text: '等待槽位',
    color: '#909399',
  },
  encrypting: {
    text: '加密中',
    color: '#E6A23C',
  },
  uploading: {
    text: '上传中',
    color: '#409EFF',
  },
  downloading: {
    text: '下载中',
    color: '#409EFF',
  },
  decrypting: {
    text: '解密中',
    color: '#E6A23C',
  },
  preempted: {
    text: '被抢占',
    color: '#F56C6C',
  },
}

/**
 * 获取备份状态展示配置
 */
export function getBackupStatusDisplay(status: BackupTaskStatus): BackupStatusDisplay {
  return statusConfigMap[status] || {
    text: status,
    color: '#909399',
    icon: 'Question',
    loading: false,
  }
}

/**
 * 获取备份状态文本
 */
export function getBackupStatusText(status: BackupTaskStatus): string {
  return getBackupStatusDisplay(status).text
}

/**
 * 获取备份状态颜色
 */
export function getBackupStatusColor(status: BackupTaskStatus): string {
  return getBackupStatusDisplay(status).color
}

/**
 * 获取备份子阶段展示配置
 */
export function getBackupSubPhaseDisplay(subPhase: string): BackupSubPhaseDisplay {
  return subPhaseConfigMap[subPhase] || {
    text: subPhase,
    color: '#909399',
  }
}

/**
 * 格式化速度显示
 * @param bytesPerSecond 每秒字节数
 * @returns 格式化后的速度字符串
 */
export function formatSpeed(bytesPerSecond: number): string {
  if (bytesPerSecond <= 0) return '0 B/s'

  const units = ['B/s', 'KB/s', 'MB/s', 'GB/s']
  let unitIndex = 0
  let speed = bytesPerSecond

  while (speed >= 1024 && unitIndex < units.length - 1) {
    speed /= 1024
    unitIndex++
  }

  return `${speed.toFixed(2)} ${units[unitIndex]}`
}

/**
 * 格式化文件大小
 * @param bytes 字节数
 * @returns 格式化后的大小字符串
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'

  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let unitIndex = 0
  let size = bytes

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex++
  }

  return `${size.toFixed(2)} ${units[unitIndex]}`
}

/**
 * 格式化进度百分比
 * @param current 当前值
 * @param total 总值
 * @returns 格式化后的百分比字符串
 */
export function formatProgress(current: number, total: number): string {
  if (total === 0) return '0%'
  const percentage = (current / total) * 100
  return `${percentage.toFixed(1)}%`
}

/**
 * 格式化剩余时间
 * @param bytesRemaining 剩余字节数
 * @param bytesPerSecond 每秒字节数
 * @returns 格式化后的时间字符串
 */
export function formatETA(bytesRemaining: number, bytesPerSecond: number): string {
  if (bytesPerSecond <= 0 || bytesRemaining <= 0) return '--'

  const seconds = bytesRemaining / bytesPerSecond

  if (seconds < 60) {
    return `${Math.ceil(seconds)} 秒`
  } else if (seconds < 3600) {
    const minutes = Math.floor(seconds / 60)
    const secs = Math.ceil(seconds % 60)
    return `${minutes} 分 ${secs} 秒`
  } else {
    const hours = Math.floor(seconds / 3600)
    const minutes = Math.floor((seconds % 3600) / 60)
    return `${hours} 小时 ${minutes} 分`
  }
}

/**
 * 判断状态是否为活跃状态（可操作）
 */
export function isActiveStatus(status: BackupTaskStatus): boolean {
  return ['queued', 'preparing', 'transferring'].includes(status)
}

/**
 * 判断状态是否可暂停
 */
export function canPause(status: BackupTaskStatus): boolean {
  return ['queued', 'preparing', 'transferring'].includes(status)
}

/**
 * 判断状态是否可恢复
 */
export function canResume(status: BackupTaskStatus): boolean {
  return status === 'paused'
}

/**
 * 判断状态是否可取消
 */
export function canCancel(status: BackupTaskStatus): boolean {
  return ['queued', 'preparing', 'transferring', 'paused'].includes(status)
}
