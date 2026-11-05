import { describe, expect, it } from 'vitest'
import {
  buildShareRootAliases,
  createShareSelection,
  deselectSharePath,
  formatSharePathForDisplay,
  isSharePathAncestor,
  normalizeShareKey,
  normalizeSharePath,
  rehydrateShareSelection,
  resolveShareKey,
  selectSharePath,
  shareSelectionCovers,
  shareSelectionCoversWhole,
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

describe('shareSelection 根目录上下文别名', () => {
  const aliases = buildShareRootAliases([
    { rawPath: '/apps/bypy/A', logicalName: 'A' },
    { rawPath: '/apps/bypy/B', logicalName: 'B' },
  ])

  it('只按实际根项做段边界前缀替换，不做后缀猜测', () => {
    expect(aliases.conflict).toBe('')
    expect(resolveShareKey('apps/bypy/A', aliases)).toBe('A')
    expect(resolveShareKey('apps/bypy/A/x.txt', aliases)).toBe('A/x.txt')
    expect(resolveShareKey('A/x.txt', aliases)).toBe('A/x.txt')
    expect(resolveShareKey('x/apps/bypy/A', aliases)).toBe('x/apps/bypy/A')
    expect(resolveShareKey('apps/bypy/AB/x', aliases)).toBe('apps/bypy/AB/x')
  })

  it('物理旧路径可按别名还原，且 no-op 仍回显原始数组', () => {
    const state = createShareSelection(['/apps/bypy/A/x.txt '], false, aliases)
    expect([...state.selected]).toEqual(['A/x.txt '])
    expect(shareSelectionPaths(state)).toEqual(['/apps/bypy/A/x.txt '])
  })

  it('别名晚到时迁移已改动选择，保留原始 raw 与未加载项', () => {
    const early = createShareSelection(['/apps/bypy/A/x.txt'], false)
    const changed = selectSharePath(early, { key: 'A/y.txt', raw: '/sharelink1-2/A/y.txt', isDir: false })
    const late = buildShareRootAliases([{ rawPath: '/apps/bypy/A', logicalName: 'A' }])
    const hydrated = rehydrateShareSelection(changed, late)
    expect(shareSelectionPaths(hydrated)).toEqual(['/apps/bypy/A/x.txt', '/sharelink1-2/A/y.txt'])
    expect(shareSelectionTriState(hydrated, 'A', true)).toBe('indeterminate')
  })

  it('逻辑 key 保留字面 sharelink 文件名，只有原始路径才剥离一次命名空间', () => {
    const literalAliases = buildShareRootAliases([
      { rawPath: '/apps/bypy/sharelink1-2', logicalName: 'sharelink1-2' },
    ])
    expect(literalAliases.conflict).toBe('')
    expect(literalAliases.byRaw.get('apps/bypy/sharelink1-2')).toBe('sharelink1-2')
    const state = createShareSelection(['/sharelink9-8/sharelink1-2/x.txt'], false, literalAliases)
    expect([...state.selected]).toEqual(['sharelink1-2/x.txt'])
    expect(shareSelectionPaths(state)).toEqual(['/sharelink9-8/sharelink1-2/x.txt'])

    expect(normalizeSharePath('/sharelink9-8/sharelink1-2/x.txt')).toBe('sharelink1-2/x.txt')
    expect(normalizeShareKey('sharelink1-2/x.txt')).toBe('sharelink1-2/x.txt')
    expect(normalizeShareKey('/A/sharelink3-4/x.txt')).toBe('A/sharelink3-4/x.txt')
    expect(formatSharePathForDisplay('/sharelink9-8/sharelink1-2/x.txt')).toBe('/sharelink1-2/x.txt')
  })

  it('raw 与 logical 的交叉重叠按段边界整体失败', () => {
    const rawUnderOtherLogical = buildShareRootAliases([
      { rawPath: '/apps/bypy/A', logicalName: 'A' },
      { rawPath: '/other/apps', logicalName: 'apps' },
    ])
    expect(rawUnderOtherLogical.conflict).not.toBe('')

    const rawAncestorOfOtherLogical = buildShareRootAliases([
      { rawPath: '/apps', logicalName: 'X' },
      { rawPath: '/other/y', logicalName: 'apps/bypy' },
    ])
    expect(rawAncestorOfOtherLogical.conflict).not.toBe('')

    const rawEqualsOtherLogical = buildShareRootAliases([
      { rawPath: '/A', logicalName: 'B' },
      { rawPath: '/apps/bypy/A', logicalName: 'A' },
    ])
    expect(rawEqualsOtherLogical.conflict).not.toBe('')

    const sameRootDescendant = buildShareRootAliases([
      { rawPath: '/A/A', logicalName: 'A' },
    ])
    expect(sameRootDescendant.conflict).not.toBe('')

    const sameRootAncestor = buildShareRootAliases([
      { rawPath: '/A', logicalName: 'A/B' },
    ])
    expect(sameRootAncestor.conflict).not.toBe('')

    expect(buildShareRootAliases([{ rawPath: '/A', logicalName: 'A' }]).conflict).toBe('')
    expect(buildShareRootAliases([
      { rawPath: '/A', logicalName: 'A' },
      { rawPath: '/B', logicalName: 'B' },
    ]).conflict).toBe('')
    expect(buildShareRootAliases([
      { rawPath: '/apps/bypy/A', logicalName: 'A' },
    ]).conflict).toBe('')
    expect(buildShareRootAliases([
      { rawPath: '/other/apps', logicalName: 'apps' },
      { rawPath: '/other/apps2', logicalName: 'apps2' },
    ]).conflict).toBe('')
  })

  it('明确的别名冲突整体失败，不合并不同内容', () => {
    const duplicateLogical = buildShareRootAliases([
      { rawPath: '/apps/bypy/A', logicalName: '同名' },
      { rawPath: '/other/同名', logicalName: '同名' },
    ])
    expect(duplicateLogical.conflict).not.toBe('')
    expect(duplicateLogical.byRaw.size).toBe(0)

    const overlapping = buildShareRootAliases([
      { rawPath: '/apps/bypy/A', logicalName: 'A' },
      { rawPath: '/apps/bypy/A/B', logicalName: 'B' },
    ])
    expect(overlapping.conflict).not.toBe('')

    const rawLogicalCollision = buildShareRootAliases([
      { rawPath: 'A', logicalName: 'B' },
      { rawPath: 'apps/bypy/A', logicalName: 'A' },
    ])
    expect(rawLogicalCollision.conflict).not.toBe('')

    const sameRawDifferentLogical = buildShareRootAliases([
      { rawPath: '/root/f0', logicalName: 'f0' },
      { rawPath: '/root/f0', logicalName: 'other' },
    ])
    expect(sameRawDifferentLogical.conflict).not.toBe('')
    expect(sameRawDifferentLogical.byRaw.size).toBe(0)

    const exactDuplicate = buildShareRootAliases([
      { rawPath: '/root/f0', logicalName: 'f0' },
      { rawPath: '/root/f0', logicalName: 'f0' },
    ])
    expect(exactDuplicate.conflict).toBe('')
    expect(exactDuplicate.byRaw.get('root/f0')).toBe('f0')
  })

  it('展示只去掉最前面合法的合成命名空间，保留其他路径与空白', () => {
    expect(formatSharePathForDisplay('/sharelink1-2/A/x.txt')).toBe('/A/x.txt')
    expect(formatSharePathForDisplay('sharelink1-2/A/x.txt')).toBe('A/x.txt')
    expect(formatSharePathForDisplay('/sharelink1-2/A/ 报告 .txt')).toBe('/A/ 报告 .txt')
    expect(formatSharePathForDisplay('/apps/bypy/A/x.txt')).toBe('/apps/bypy/A/x.txt')
    expect(formatSharePathForDisplay('/sharelinkAB-2/A/x.txt')).toBe('/sharelinkAB-2/A/x.txt')
    expect(formatSharePathForDisplay('/foo/sharelink1-2/A/x.txt')).toBe('/foo/sharelink1-2/A/x.txt')
    expect(formatSharePathForDisplay('/sharelink1-2')).toBe('/sharelink1-2')
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

describe('shareSelection 拆成逐项选择的目录（expandedDirs）', () => {
  it('未发生展开时不报告：取消显式选中项、取消未被覆盖的后代', async () => {
    const { load } = loaderOf({})
    const explicit = await deselectSharePath(createShareSelection(['/剧集', '/说明.txt'], false), '说明.txt', load)
    expect(explicit.ok).toBe(true)
    expect(explicit.expandedDirs).toBeUndefined()

    const partial = await deselectSharePath(createShareSelection(['/剧集/S01'], false), '剧集', load)
    expect(partial.ok).toBe(true)
    expect(partial.expandedDirs).toBeUndefined()
  })

  it('取消被目录覆盖的子项时报告该目录', async () => {
    const { load } = loaderOf({
      剧集: [entry('/剧集/01.mp4'), entry('/剧集/02.mp4')],
    })
    const outcome = await deselectSharePath(createShareSelection(['/剧集'], false), '剧集/01.mp4', load)
    expect(outcome.ok).toBe(true)
    expect(outcome.expandedDirs).toEqual(['剧集'])
  })

  it('沿祖先链展开时从外到内报告每一级目录', async () => {
    const { load } = loaderOf({
      A: [entry('/A/B', true), entry('/A/x.mp4')],
      'A/B': [entry('/A/B/c.mp4'), entry('/A/B/d.mp4')],
    })
    const outcome = await deselectSharePath(createShareSelection(['/A'], false), 'A/B/c.mp4', load)
    expect(outcome.ok).toBe(true)
    expect(outcome.expandedDirs).toEqual(['A', 'A/B'])
  })

  it('整个分享下取消一项时报告整个分享（空 key）', async () => {
    const { load } = loaderOf({
      '': [entry('/剧集', true), entry('/说明.txt')],
    })
    const outcome = await deselectSharePath(createShareSelection([], true), '说明.txt', load)
    expect(outcome.ok).toBe(true)
    expect(outcome.expandedDirs).toEqual([''])
  })

  it('展开失败时不报告', async () => {
    const { load } = loaderOf({})
    const outcome = await deselectSharePath(createShareSelection(['/剧集'], false), '剧集/01.mp4', load)
    expect(outcome.ok).toBe(false)
    expect(outcome.expandedDirs).toBeUndefined()
  })

  it('shareSelectionCoversWhole：整个分享只认哨兵，目录认自身或祖先', () => {
    expect(shareSelectionCoversWhole(createShareSelection([], true), '')).toBe(true)
    expect(shareSelectionCoversWhole(createShareSelection(['/剧集', '/说明.txt'], true), '')).toBe(false)
    const state = createShareSelection(['/A'], false)
    expect(shareSelectionCoversWhole(state, 'A')).toBe(true)
    expect(shareSelectionCoversWhole(state, 'A/B')).toBe(true)
    expect(shareSelectionCoversWhole(createShareSelection(['/A/B/d.mp4'], false), 'A/B')).toBe(false)
  })
})
