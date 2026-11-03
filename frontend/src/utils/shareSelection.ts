/**
 * 分享同步选择器的纯状态逻辑。
 *
 * 只负责路径规范化、覆盖判定、三态推导，以及「取消被祖先覆盖的后代」时的事务式
 * 展开：不依赖 Vue，也不发请求 —— 直接子项由调用方以 loader 注入。
 */

/** 分享内的一条路径（文件或目录） */
export interface ShareSelectionEntry {
  /** 规范化后的路径 key */
  key: string
  /** 服务端返回的原始路径 */
  raw: string
  /** 是否目录 */
  isDir: boolean
}

/** 直接子项加载器：传入目录 key（根为 ''），返回该目录的全部直接子项 */
export type ShareChildrenLoader = (dirKey: string) => Promise<ShareSelectionEntry[]>

/** 复选框三态 */
export type ShareTriState = 'checked' | 'indeterminate' | 'unchecked'

/** 选择状态 */
export interface ShareSelectionState {
  /** 显式选中的路径 key；目录代表整棵子树 */
  selected: Set<string>
  /** key → 原始路径字符串 */
  rawPaths: Map<string, string>
  /** 整个分享都被选中（include 空初始路径的内部哨兵） */
  wholeShare: boolean
  /** 打开时的原始路径数组 */
  initialRaw: string[]
  /** 打开时的显式选中 key */
  initialKeys: Set<string>
  /** 打开时是否处于整个分享哨兵 */
  initialWholeShare: boolean
}

/** 取消选择的结果 */
export interface ShareDeselectOutcome {
  /** 是否成功 */
  ok: boolean
  /** 成功时是新状态，失败时保持原状态 */
  state: ShareSelectionState
  /** 失败原因 */
  error?: string
}

/** 合成的 sharelink 命名空间段（与后端一致，忽略它但不做大小写折叠） */
const NAMESPACE_SEGMENT = /^sharelink\d+-\d+$/

/**
 * 规范化分享路径：合并重复斜杠、去掉首尾斜杠，并丢弃合成的 sharelink 命名空间段。
 *
 * 只规整分隔符与合成命名空间：**文件名里的空白一律原样保留**（含结尾空格），
 * 否则「报告. 」这类文件名会与「报告.」归一成同一个 key，导致还原与选择串档。
 */
export function normalizeSharePath(raw: string): string {
  if (!raw) return ''
  // 根分隔符之前的空白只属于分隔符，不属于任何文件名；分隔符之后的空白属于
  // 文件名本身，必须保留。
  const lead = /^\s*/.exec(raw)?.[0] ?? ''
  const rest = lead.length > 0 && raw.charAt(lead.length) === '/' ? raw.slice(lead.length) : raw
  const segments = rest.split('/').filter(segment => segment.length > 0)
  if (segments.length > 0 && NAMESPACE_SEGMENT.test(segments[0])) {
    segments.shift()
  }
  return segments.join('/')
}

/** ancestor 是否为 descendant 的祖先（按路径段边界判定） */
export function isSharePathAncestor(ancestorKey: string, descendantKey: string): boolean {
  if (ancestorKey === descendantKey) return false
  if (ancestorKey === '') return descendantKey !== ''
  return descendantKey.startsWith(`${ancestorKey}/`)
}

/** 建立初始选择状态 */
export function createShareSelection(
  initialPaths: string[] = [],
  emptySelectionMeansAll = false,
): ShareSelectionState {
  const initialRaw = [...initialPaths]
  const selected = new Set<string>()
  const rawPaths = new Map<string, string>()
  for (const path of initialRaw) {
    const key = normalizeSharePath(path)
    if (!key) continue
    selected.add(key)
    if (!rawPaths.has(key)) rawPaths.set(key, path)
  }
  const wholeShare = emptySelectionMeansAll && initialRaw.length === 0
  return {
    selected: wholeShare ? new Set<string>() : selected,
    rawPaths,
    wholeShare,
    initialRaw,
    initialKeys: selected,
    initialWholeShare: wholeShare,
  }
}

/** 复制选择状态 */
export function cloneShareSelection(state: ShareSelectionState): ShareSelectionState {
  return {
    selected: new Set(state.selected),
    rawPaths: new Map(state.rawPaths),
    wholeShare: state.wholeShare,
    initialRaw: state.initialRaw,
    initialKeys: state.initialKeys,
    initialWholeShare: state.initialWholeShare,
  }
}

/** 目标路径是否被某个显式选中的祖先覆盖 */
function coveredBySelectedAncestor(state: ShareSelectionState, key: string): boolean {
  for (const selected of state.selected) {
    if (isSharePathAncestor(selected, key)) return true
  }
  return false
}

/** 目标目录内部是否存在被选中的后代 */
export function hasSelectedDescendant(state: ShareSelectionState, key: string): boolean {
  for (const selected of state.selected) {
    if (isSharePathAncestor(key, selected)) return true
  }
  return false
}

/** 目标路径（含自身）是否被选中 */
export function shareSelectionCovers(state: ShareSelectionState, key: string): boolean {
  if (state.wholeShare) return true
  if (state.selected.has(key)) return true
  return coveredBySelectedAncestor(state, key)
}

/**
 * 推导某个条目的三态。
 *
 * completeChildren 必须是该目录的**完整**直接子项列表：只有完整覆盖时才能判定为
 * 全选，仅加载了一页时不得据此推断整棵子树已选中。
 */
export function shareSelectionTriState(
  state: ShareSelectionState,
  key: string,
  isDir: boolean,
  completeChildren?: ShareSelectionEntry[],
): ShareTriState {
  if (state.wholeShare) return 'checked'
  if (state.selected.has(key)) return 'checked'
  if (coveredBySelectedAncestor(state, key)) return 'checked'
  if (isDir) {
    if (
      completeChildren &&
      completeChildren.length > 0 &&
      completeChildren.every(child => shareSelectionCovers(state, child.key))
    ) {
      return 'checked'
    }
    if (hasSelectedDescendant(state, key)) return 'indeterminate'
  }
  return 'unchecked'
}

/** 当前选择是否与打开时完全一致 */
function isUnchanged(state: ShareSelectionState): boolean {
  if (state.wholeShare !== state.initialWholeShare) return false
  if (state.selected.size !== state.initialKeys.size) return false
  for (const key of state.selected) {
    if (!state.initialKeys.has(key)) return false
  }
  return true
}

/**
 * 输出对外路径数组。
 *
 * 未改动时原样返回打开时的原始数组（保序、保留别名写法）；改动后去掉被祖先覆盖的
 * 冗余项，未变动的 key 仍用原始字符串，尚未加载出来的已选路径也不会丢。
 */
export function shareSelectionPaths(state: ShareSelectionState): string[] {
  if (isUnchanged(state)) return [...state.initialRaw]
  const paths: string[] = []
  for (const key of state.selected) {
    let covered = false
    for (const other of state.selected) {
      if (other !== key && isSharePathAncestor(other, key)) {
        covered = true
        break
      }
    }
    if (covered) continue
    paths.push(state.rawPaths.get(key) ?? key)
  }
  return paths
}

/** 当前选择是否可以确认 */
export function shareSelectionReady(
  state: ShareSelectionState,
  emptySelectionMeansAll: boolean,
): boolean {
  if (!emptySelectionMeansAll) return true
  if (state.wholeShare) return true
  return state.selected.size > 0
}

/** 选中一个路径（目录代表整棵子树） */
export function selectSharePath(
  state: ShareSelectionState,
  entry: ShareSelectionEntry,
): ShareSelectionState {
  const next = cloneShareSelection(state)
  if (next.wholeShare) return next
  next.selected.add(entry.key)
  if (!next.rawPaths.has(entry.key)) next.rawPaths.set(entry.key, entry.raw)
  return next
}

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message) return error.message
  return '加载目录失败，请稍后重试'
}

/**
 * 取消选择一条路径。
 *
 * 目标被显式选中的祖先（或整个分享哨兵）覆盖时，必须把覆盖者替换成「剩余兄弟」：
 * 沿链路逐级加载每一层的**全部**直接子项，失败则整体放弃并保留原选择。
 */
export async function deselectSharePath(
  state: ShareSelectionState,
  targetKey: string,
  loadChildren: ShareChildrenLoader,
): Promise<ShareDeselectOutcome> {
  const target = normalizeSharePath(targetKey)
  if (!target) {
    return { ok: false, state, error: '无效路径' }
  }

  // 覆盖目标的显式祖先（不含目标自身）。多个显式祖先可能互相重叠（如 /a 与 /a/b），
  // 必须从**最外层**那个展开：只展开内层会让外层继续覆盖被取消的子项。
  const covering = state.wholeShare
    ? []
    : [...state.selected].filter((key) => isSharePathAncestor(key, target))
  const outermost = covering.length > 0
    ? covering.reduce((a, b) => (a.length <= b.length ? a : b))
    : null

  if (!state.wholeShare && outermost === null) {
    const next = cloneShareSelection(state)
    if (state.selected.has(target)) {
      for (const key of [...next.selected]) {
        if (key === target || isSharePathAncestor(target, key)) next.selected.delete(key)
      }
      return { ok: true, state: next }
    }
    // 目标自身未被选中：清掉其已选后代即可
    for (const key of [...next.selected]) {
      if (isSharePathAncestor(target, key)) next.selected.delete(key)
    }
    return { ok: true, state: next }
  }

  const entries: ShareSelectionEntry[] = []
  let levelKey = outermost ?? ''
  for (;;) {
    let children: ShareSelectionEntry[]
    try {
      children = await loadChildren(levelKey)
    } catch (error) {
      return { ok: false, state, error: errorMessage(error) }
    }
    const branch = children.find(
      child => child.key === target || isSharePathAncestor(child.key, target),
    )
    if (!branch) {
      return { ok: false, state, error: '无法定位所选路径对应的目录内容' }
    }
    for (const child of children) {
      if (child.key !== branch.key) entries.push(child)
    }
    if (branch.key === target) break
    levelKey = branch.key
  }

  const next = cloneShareSelection(state)
  next.wholeShare = false
  if (outermost) {
    // 去掉所有覆盖者与目标自身（含目标的后代）：不得再有条目覆盖被取消的子项，
    // 也不得把外层冗余祖先留在集合里继续覆盖它。
    for (const key of [...next.selected]) {
      if (
        key === target ||
        isSharePathAncestor(key, target) ||
        isSharePathAncestor(target, key)
      ) {
        next.selected.delete(key)
      }
    }
  } else {
    next.selected.clear()
  }
  for (const entry of entries) {
    next.selected.add(entry.key)
    if (!next.rawPaths.has(entry.key)) next.rawPaths.set(entry.key, entry.raw)
  }
  return { ok: true, state: next }
}
