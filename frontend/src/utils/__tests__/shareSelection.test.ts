import { describe, expect, it } from 'vitest'
import {
  createShareSelection,
  deselectSharePath,
  isSharePathAncestor,
  normalizeSharePath,
  selectSharePath,
  shareSelectionCovers,
  shareSelectionPaths,
  shareSelectionReady,
  shareSelectionTriState,
  type ShareChildrenLoader,
  type ShareSelectionEntry,
} from '../shareSelection'

function entry(path: string, isDir = false): ShareSelectionEntry {
  return { key: normalizeSharePath(path), raw: path, isDir }
}

function loaderOf(tree: Record<string, ShareSelectionEntry[]>) {
  const calls: string[] = []
  const load: ShareChildrenLoader = async (dirKey: string) => {
    calls.push(dirKey)
    const entries = tree[dirKey]
    if (!entries) throw new Error(`目录加载失败: ${dirKey}`)
    return entries
  }
  return { load, calls }
}

describe('shareSelection 路径规范化', () => {
  it('合并重复斜杠并去掉首尾斜杠', () => {
    expect(normalizeSharePath('//剧集//S01//')).toBe('剧集/S01')
    expect(normalizeSharePath('/说明.txt')).toBe('说明.txt')
    expect(normalizeSharePath('/')).toBe('')
  })

  it('根分隔符之前的空白只算分隔符填充，文件名空白原样保留', () => {
    // 结尾的空格属于文件名，不能被裁掉
    expect(normalizeSharePath('  /说明.txt  ')).toBe('说明.txt  ')
    // 名字内部的空白同样保留
    expect(normalizeSharePath('/剧 集/第 1 集.mp4')).toBe('剧 集/第 1 集.mp4')
  })

  it('忽略合成的 sharelink 命名空间但不做大小写折叠', () => {
    expect(normalizeSharePath('/sharelink0-123/剧集/01.mp4')).toBe('剧集/01.mp4')
    expect(normalizeSharePath('/sharelink1234-5678/说明.txt')).toBe('说明.txt')
    // 非数字命名空间不是合成段，大小写也不折叠
    expect(normalizeSharePath('/sharelinkAB-12/说明.txt')).toBe('sharelinkAB-12/说明.txt')
    expect(normalizeSharePath('/ShareLink0-1/说明.txt')).toBe('ShareLink0-1/说明.txt')
  })

  it('保留文件名里的空白：结尾空格不与无空格同名', () => {
    expect(normalizeSharePath('/剧集/第1集 .mp4')).toBe('剧集/第1集 .mp4')
    expect(normalizeSharePath('/剧集/第1集.mp4 ')).toBe('剧集/第1集.mp4 ')
    expect(normalizeSharePath('/剧集/第1集.mp4')).not.toBe(
      normalizeSharePath('/剧集/第1集.mp4 '),
    )
    // 分隔符与合成命名空间照常规整，文件名空白不动
    expect(normalizeSharePath('//剧集//第1集.mp4 //')).toBe('剧集/第1集.mp4 ')
    expect(normalizeSharePath('/sharelink0-1/剧集/第1集.mp4 ')).toBe('剧集/第1集.mp4 ')
  })

  it('按路径段边界判定祖先关系', () => {
    expect(isSharePathAncestor('剧集', '剧集/S01')).toBe(true)
    expect(isSharePathAncestor('剧集', '剧集')).toBe(false)
    expect(isSharePathAncestor('剧', '剧集/S01')).toBe(false)
    expect(isSharePathAncestor('', '剧集/S01')).toBe(true)
    expect(isSharePathAncestor('', '')).toBe(false)
  })
})

describe('shareSelection 选择状态', () => {
  it('空初始路径且允许整体分享时进入整个分享哨兵', () => {
    const state = createShareSelection([], true)
    expect(state.wholeShare).toBe(true)
    expect(shareSelectionPaths(state)).toEqual([])
    expect(shareSelectionReady(state, true)).toBe(true)
  })

  it('有初始路径时按显式路径建立选择', () => {
    const state = createShareSelection(['/剧集', '/说明.txt'], true)
    expect(state.wholeShare).toBe(false)
    expect([...state.selected]).toEqual(['剧集', '说明.txt'])
  })

  it('显式目录为全选、被祖先覆盖的文件为全选', () => {
    const state = createShareSelection(['/剧集'], false)
    expect(shareSelectionTriState(state, '剧集', true)).toBe('checked')
    expect(shareSelectionTriState(state, '剧集/01.mp4', false)).toBe('checked')
    expect(shareSelectionTriState(state, '说明.txt', false)).toBe('unchecked')
  })

  it('已选后代的父目录为半选，完整子项全部覆盖时才判定为全选', () => {
    const state = createShareSelection(['/剧集/01.mp4'], false)
    expect(shareSelectionTriState(state, '剧集', true)).toBe('indeterminate')
    expect(shareSelectionTriState(state, '剧集', true, [entry('/剧集/01.mp4')])).toBe('checked')
    expect(shareSelectionTriState(state, '剧集', true, [entry('/剧集/01.mp4'), entry('/剧集/02.mp4')]))
      .toBe('indeterminate')
  })

  it('整个分享哨兵下所有条目都是全选', () => {
    const state = createShareSelection([], true)
    expect(shareSelectionTriState(state, '剧集', true)).toBe('checked')
    expect(shareSelectionTriState(state, '剧集/01.mp4', false)).toBe('checked')
  })

  it('未改动时原样回显初始路径（含别名写法与顺序）', () => {
    const initial = ['/sharelink0-1/剧集', '/说明.txt']
    const state = createShareSelection(initial, false)
    expect(shareSelectionPaths(state)).toEqual(initial)
  })

  it('改动后去掉被祖先覆盖的项，保留未改动项的原始字符串与未加载的已选路径', () => {
    const state = createShareSelection(['/剧集/01.mp4'], false)
    const next = selectSharePath(state, entry('/剧集', true))
    expect(shareSelectionPaths(next)).toEqual(['/剧集'])
  })

  it('结尾空格不同的文件名是两条独立选择', () => {
    const state = createShareSelection(['/剧集/第1集.mp4'], false)
    const next = selectSharePath(state, {
      key: '剧集/第1集.mp4 ',
      raw: '/剧集/第1集.mp4 ',
      isDir: false,
    })
    expect(shareSelectionPaths(next)).toEqual(['/剧集/第1集.mp4', '/剧集/第1集.mp4 '])
  })

  it('排除内容允许空选择，同步内容清空后不可确认', async () => {
    const exclude = createShareSelection([], false)
    expect(shareSelectionReady(exclude, false)).toBe(true)
    expect(shareSelectionPaths(exclude)).toEqual([])

    const include = createShareSelection(['/剧集'], true)
    expect(shareSelectionReady(include, true)).toBe(true)
    const outcome = await deselectSharePath(include, '剧集', async () => [])
    expect(outcome.ok).toBe(true)
    expect(shareSelectionPaths(outcome.state)).toEqual([])
    expect(shareSelectionReady(outcome.state, true)).toBe(false)
  })
})

describe('shareSelection 取消后代', () => {
  it('取消显式选中的目录只移除自身与后代，不请求目录内容', async () => {
    const { load, calls } = loaderOf({})
    const state = createShareSelection(['/剧集'], false)
    const outcome = await deselectSharePath(state, '剧集', load)
    expect(outcome.ok).toBe(true)
    expect(shareSelectionPaths(outcome.state)).toEqual([])
    expect(calls).toEqual([])
  })

  it('取消被祖先覆盖的子项时用剩余兄弟替换祖先', async () => {
    const { load, calls } = loaderOf({
      剧集: [entry('/剧集/01.mp4'), entry('/剧集/02.mp4')],
    })
    const state = createShareSelection(['/剧集'], false)
    const outcome = await deselectSharePath(state, '剧集/01.mp4', load)
    expect(outcome.ok).toBe(true)
    expect(shareSelectionPaths(outcome.state)).toEqual(['/剧集/02.mp4'])
    expect(calls).toEqual(['剧集'])
  })

  it('沿祖先链逐级展开，每级只加载该层直接子项', async () => {
    const { load, calls } = loaderOf({
      A: [entry('/A/B', true)],
      'A/B': [entry('/A/B/c.mp4'), entry('/A/B/d.mp4')],
    })
    const state = createShareSelection(['/A'], false)
    const outcome = await deselectSharePath(state, 'A/B/c.mp4', load)
    expect(outcome.ok).toBe(true)
    expect(shareSelectionPaths(outcome.state)).toEqual(['/A/B/d.mp4'])
    expect(calls).toEqual(['A', 'A/B'])
  })

  it('多个重叠显式祖先时从最外层展开，并清掉所有仍覆盖目标的条目', async () => {
    const { load, calls } = loaderOf({
      a: [entry('/a/b', true), entry('/a/c', true)],
      'a/b': [entry('/a/b/x'), entry('/a/b/y')],
    })
    const state = createShareSelection(['/a', '/a/b', '/z'], false)
    const outcome = await deselectSharePath(state, 'a/b/x', load)
    expect(outcome.ok).toBe(true)
    const paths = shareSelectionPaths(outcome.state)
    // 外层与内层覆盖者都不能留下：否则 /a/b/x 仍被覆盖
    expect(paths).not.toContain('/a')
    expect(paths).not.toContain('/a/b')
    expect(paths).not.toContain('/a/b/x')
    // 无关选择保留，其余兄弟补齐
    expect(paths).toContain('/z')
    expect(paths).toContain('/a/c')
    expect(paths).toContain('/a/b/y')
    expect(paths).toEqual(['/z', '/a/c', '/a/b/y'])
    // 从最外层开始逐级展开
    expect(calls).toEqual(['a', 'a/b'])
    // 取消后不能再有任何条目覆盖被取消的子项（reviewer 复现点）
    expect(shareSelectionCovers(outcome.state, 'a/b/x')).toBe(false)
  })

  it('重叠祖先展开中途失败时保留全部原选择', async () => {
    const load: ShareChildrenLoader = async (dirKey: string) => {
      if (dirKey === 'a') return [entry('/a/b', true), entry('/a/c', true)]
      throw new Error('网络中断')
    }
    const state = createShareSelection(['/a', '/a/b', '/z'], false)
    const outcome = await deselectSharePath(state, 'a/b/x', load)
    expect(outcome.ok).toBe(false)
    expect(shareSelectionPaths(outcome.state)).toEqual(['/a', '/a/b', '/z'])
  })

  it('整个分享哨兵下取消子项会解析成剩余兄弟的显式路径', async () => {
    const { load, calls } = loaderOf({
      '': [entry('/剧集', true), entry('/说明.txt')],
      剧集: [entry('/剧集/01.mp4')],
    })
    const state = createShareSelection([], true)
    const outcome = await deselectSharePath(state, '剧集/01.mp4', load)
    expect(outcome.ok).toBe(true)
    expect(shareSelectionPaths(outcome.state)).toEqual(['/说明.txt'])
    expect(calls).toEqual(['', '剧集'])
  })

  it('取消失败的文件时只保留同目录的兄弟路径', async () => {
    const { load } = loaderOf({
      剧集: [entry('/剧集/01.mp4'), entry('/剧集/02.mp4')],
    })
    const state = createShareSelection(['/剧集'], false)
    const outcome = await deselectSharePath(state, '剧集/01.mp4', load)
    expect(outcome.ok).toBe(true)
    expect(shareSelectionPaths(outcome.state)).toEqual(['/剧集/02.mp4'])
  })

  it('展开中途失败时整体放弃，不留下部分结果', async () => {
    const load: ShareChildrenLoader = async (dirKey: string) => {
      if (dirKey === 'A') return [entry('/A/B', true)]
      throw new Error('网络中断')
    }
    const state = createShareSelection(['/A'], false)
    const outcome = await deselectSharePath(state, 'A/B/c.mp4', load)
    expect(outcome.ok).toBe(false)
    expect(outcome.error).toContain('网络中断')
    expect(shareSelectionPaths(outcome.state)).toEqual(['/A'])
  })

  it('目标目录不存在时报错并保留原选择', async () => {
    const { load } = loaderOf({
      '': [entry('/说明.txt')],
    })
    const state = createShareSelection([], true)
    const outcome = await deselectSharePath(state, '不存在的目录', load)
    expect(outcome.ok).toBe(false)
    expect(outcome.state.wholeShare).toBe(true)
  })
})
