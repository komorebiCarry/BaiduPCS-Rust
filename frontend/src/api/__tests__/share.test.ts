import { afterEach, describe, expect, it, vi } from 'vitest'

const post = vi.hoisted(() => vi.fn())
vi.mock('../client', () => ({ apiClient: { post, get: vi.fn() } }))

// 与后端共用的脱敏 fixture（vite ?raw 直接读源码树里的文件）
import fixtureRaw from '../../../../backend/src/netdisk/testdata/share_record_page1.json?raw'
import {
  deadShareKind,
  modifyShareExpire,
  SHARE_DELETED_MARKER,
  SHARE_EXPIRED_MARKER,
  SHARE_EXPIRE_OPTIONS,
  type ShareRecord,
} from '../share'

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

describe('deadShareKind（按 /share/record 真实返回判定失效分享）', () => {
  // 与后端共用的脱敏 fixture：timed / permanent / expired / deleted 各一条
  const fixture = JSON.parse(fixtureRaw) as { list: ShareRecord[]; count: number; nextpage: number }

  it('fixture 结构：没有 total，只有 count / nextpage', () => {
    expect(fixture).not.toHaveProperty('total')
    expect(fixture.count).toBe(4)
    expect(fixture.nextpage).toBe(0)
  })

  it('依次识别为 正常 / 正常 / 已过期 / 已删除', () => {
    expect(fixture.list.map(deadShareKind)).toEqual([null, null, 'expired', 'deleted'])
  })

  it('文件已删除优先于过期；expiredType -1 或「分享已过期」都算过期', () => {
    const base = fixture.list[0]
    expect(deadShareKind({ ...base, typicalPath: SHARE_DELETED_MARKER, expiredType: -1 })).toBe('deleted')
    expect(deadShareKind({ ...base, expiredType: -1 })).toBe('expired')
    expect(deadShareKind({ ...base, typicalPath: SHARE_EXPIRED_MARKER, expiredType: 0 })).toBe('expired')
    // 普通路径里含有标记字样不算
    expect(deadShareKind({ ...base, typicalPath: '/资料/分享已过期的说明.txt' })).toBeNull()
  })
})
