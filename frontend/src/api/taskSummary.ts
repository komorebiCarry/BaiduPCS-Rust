import { createApiClient } from './client'

// 菜单进度是后台轮询，失败时静默，避免断网时每隔几秒弹一次错误
const summaryClient = createApiClient({ timeout: 10_000, showErrorMessage: false })

/**
 * 单个方向（上传或下载）的进度摘要，口径见后端 `task_summary.rs`：
 * 按任务数统计，与列表页「全部 / 已完成 / 失败」一致（文件夹算 1 个任务）；
 * 字节进度和速度只看未结束的任务
 */
export interface TransferSummary {
  /** 列表里的任务总数 */
  total: number
  /** 已完成的任务数 */
  completed: number
  /** 失败的任务数 */
  failed: number
  /** 未结束的任务数（含暂停）；0 时菜单不显示进度 */
  remaining: number
  /** 未结束且未暂停的任务数；remaining > 0 而 active 为 0 表示全部暂停 */
  active: number
  /** 未结束任务的总字节 / 已完成字节 */
  total_bytes: number
  done_bytes: number
  /** 总速度（字节/秒） */
  speed: number
  /** 有文件夹仍在扫描，总数不确定 */
  scanning: boolean
}

export interface TasksSummary {
  download: TransferSummary
  upload: TransferSummary
}

export async function getTasksSummary(): Promise<TasksSummary> {
  return summaryClient.get('/tasks/summary')
}
