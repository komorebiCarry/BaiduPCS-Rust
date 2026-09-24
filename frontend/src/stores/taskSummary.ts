/**
 * 上传/下载菜单进度 Pinia Store
 *
 * 数据来源：轮询 `GET /api/v1/tasks/summary`（后端只遍历内存任务，开销很小）。
 * 不走 WebSocket：订阅是按名字存的 Set、没有引用计数，布局层再订阅一次
 * `upload:*` 会在上传页卸载时被一起取消掉。
 *
 * 轮询节奏：有进行中任务 3s，空闲 6s（让新建任务能尽快出现在菜单上）；
 * 页面不可见时暂停，重新可见时立即刷新一次。
 */

import { defineStore } from 'pinia'
import { computed, ref } from 'vue'
import { getTasksSummary, type TransferSummary } from '@/api/taskSummary'

const ACTIVE_INTERVAL_MS = 3000
const IDLE_INTERVAL_MS = 6000

/** 菜单上展示用的派生数据；没有未完成任务时为 null（菜单不显示进度） */
export interface TransferIndicator {
  completed: number
  total: number
  remaining: number
  failed: number
  speed: number
  /** 未完成的任务全部处于暂停状态 */
  paused: boolean
  /** 0-100；文件夹扫描中总量不确定时为 null */
  percent: number | null
}

export function toIndicator(s: TransferSummary | undefined): TransferIndicator | null {
  if (!s || s.remaining <= 0) return null
  const paused = s.active <= 0
  // 只有真正在扫描时总量才不确定；全部暂停时即使有扫描未完成的文件夹也显示已知进度
  const percent = s.scanning && !paused
    ? null
    : s.total_bytes > 0 ? Math.min(100, Math.floor((s.done_bytes / s.total_bytes) * 100)) : 0
  return {
    completed: s.completed,
    total: s.total,
    remaining: s.remaining,
    failed: s.failed,
    speed: s.speed,
    paused,
    percent,
  }
}

export const useTaskSummaryStore = defineStore('taskSummary', () => {
  const download = ref<TransferSummary>()
  const upload = ref<TransferSummary>()

  const downloadIndicator = computed(() => toIndicator(download.value))
  const uploadIndicator = computed(() => toIndicator(upload.value))

  let timer: number | undefined
  /** 轮询代次：stop/重排时递增，让还在 await 中的旧 tick 不再续排，避免出现两条轮询链 */
  let generation = 0
  let running = false

  async function refresh(): Promise<void> {
    try {
      const summary = await getTasksSummary()
      download.value = summary.download
      upload.value = summary.upload
    } catch {
      // 静默：下次轮询再试
    }
  }

  async function tick(gen: number): Promise<void> {
    timer = undefined
    if (!document.hidden) {
      await refresh()
    }
    if (!running || gen !== generation) return
    // 全部暂停时没有进度变化，按空闲节奏轮询即可
    const busy = (download.value?.active ?? 0) > 0 || (upload.value?.active ?? 0) > 0
    timer = window.setTimeout(() => tick(gen), busy ? ACTIVE_INTERVAL_MS : IDLE_INTERVAL_MS)
  }

  /** 立即刷新并从头开始计时 */
  function restart(): void {
    if (timer !== undefined) {
      window.clearTimeout(timer)
      timer = undefined
    }
    void tick(++generation)
  }

  function handleVisibilityChange(): void {
    if (running && !document.hidden) restart()
  }

  function start(): void {
    if (running) return
    running = true
    document.addEventListener('visibilitychange', handleVisibilityChange)
    restart()
  }

  function stop(): void {
    running = false
    generation++
    document.removeEventListener('visibilitychange', handleVisibilityChange)
    if (timer !== undefined) {
      window.clearTimeout(timer)
      timer = undefined
    }
  }

  return { download, upload, downloadIndicator, uploadIndicator, refresh, start, stop }
})
