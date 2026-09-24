import { describe, expect, it } from 'vitest'
import { toIndicator } from '../taskSummary'
import type { TransferSummary } from '@/api/taskSummary'

function summary(partial: Partial<TransferSummary>): TransferSummary {
  return {
    total: 0,
    completed: 0,
    failed: 0,
    remaining: 0,
    active: 0,
    total_bytes: 0,
    done_bytes: 0,
    speed: 0,
    scanning: false,
    ...partial,
  }
}

describe('toIndicator', () => {
  it('没有进行中的批次时不显示', () => {
    expect(toIndicator(undefined)).toBeNull()
    expect(toIndicator(summary({ total: 3, completed: 2, failed: 1 }))).toBeNull()
  })

  it('按字节计算百分比并向下取整', () => {
    const ind = toIndicator(summary({ total: 30, completed: 12, remaining: 18, active: 2, total_bytes: 1000, done_bytes: 999 }))
    expect(ind).toMatchObject({ completed: 12, total: 30, remaining: 18, percent: 99, paused: false })
  })

  it('只有正在扫描时进度才不确定', () => {
    expect(toIndicator(summary({ remaining: 1, active: 1, total_bytes: 100, done_bytes: 10, scanning: true }))!.percent).toBeNull()
    // 总字节未知但没在扫描：显示 0%，不播放动画
    expect(toIndicator(summary({ remaining: 1, active: 1 }))!.percent).toBe(0)
  })

  it('未完成任务全部暂停时标记为暂停，进度为已知值', () => {
    const ind = toIndicator(summary({ remaining: 5, active: 0, total_bytes: 200, done_bytes: 100, scanning: true }))
    expect(ind).toMatchObject({ paused: true, percent: 50 })
  })
})
