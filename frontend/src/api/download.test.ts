import { describe, expect, it } from 'vitest'
import { calculateETA, type DownloadTask } from './download'

function createTask(overrides: Partial<DownloadTask> = {}): DownloadTask {
  return {
    id: 'task-1',
    fs_id: 1,
    remote_path: '/file.bin',
    local_path: '/tmp/file.bin',
    total_size: 1000,
    downloaded_size: 200,
    status: 'downloading',
    speed: 100,
    created_at: 0,
    ...overrides,
  }
}

describe('calculateETA', () => {
  it('calculates ETA from remaining bytes and speed', () => {
    expect(calculateETA(createTask())).toBe(8)
  })

  it('rounds partial seconds up', () => {
    expect(calculateETA(createTask({ downloaded_size: 999, speed: 100 }))).toBe(1)
  })

  it('returns null when total size or speed is unknown', () => {
    expect(calculateETA(createTask({ total_size: 0 }))).toBeNull()
    expect(calculateETA(createTask({ speed: 0 }))).toBeNull()
    expect(calculateETA(createTask({ speed: Number.NaN }))).toBeNull()
  })

  it('returns zero after all bytes are downloaded', () => {
    expect(calculateETA(createTask({ downloaded_size: 1000, speed: 0 }))).toBe(0)
  })
})
