import { afterEach, describe, expect, it, vi } from 'vitest'

const post = vi.hoisted(() => vi.fn())
vi.mock('../client', () => ({ apiClient: { post, get: vi.fn() } }))

import { modifyShareExpire, SHARE_EXPIRE_OPTIONS } from '../share'

describe('modifyShareExpire', () => {
  afterEach(() => vi.clearAllMocks())

  it('按后端约定发送 share_ids / days，并按条数放宽超时', async () => {
    post.mockResolvedValue({ results: [], successCount: 0, failedCount: 0 })
    await modifyShareExpire([41865092248], 3)
    expect(post).toHaveBeenCalledWith(
      '/shares/expire',
      { share_ids: [41865092248], days: 3 },
      { timeout: 35000 },
    )
    await modifyShareExpire([1, 2, 3], -1)
    expect(post).toHaveBeenLastCalledWith(
      '/shares/expire',
      { share_ids: [1, 2, 3], days: -1 },
      { timeout: 45000 },
    )
  })

  it('选项与网页端一致（无自定义日期）', () => {
    expect(SHARE_EXPIRE_OPTIONS.map((o) => o.days)).toEqual([1, 3, 7, 30, -1])
    expect(SHARE_EXPIRE_OPTIONS.at(-1)!.label).toBe('永久有效')
  })
})
