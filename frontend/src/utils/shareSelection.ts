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
  /**
   * 成功且发生了「覆盖者 → 剩余兄弟」展开时，按从外到内的顺序列出被拆成逐项选择的
   * 目录 key（整个分享哨兵记为 ''）。这些目录不再整体选中，之后新增的内容不会被
   * 自动覆盖；未发生展开时缺省。
   */
  expandedDirs?: string[]
}

/** 根目录别名无法唯一对应时的提示 */
export const SHARE_ALIAS_CONFLICT_MESSAGE = '分享目录路径存在冲突，无法安全匹配已选内容，请重新打开选择器'

/** 根目录列表中的一项：原始路径 + 逻辑文件名 */
export interface ShareRootAliasItem {
  rawPath: string
  logicalName: string
}

/** 根目录上下文别名：原始根 key → 逻辑根 key */
export interface ShareRootAliases {
  byRaw: Map<string, string>
  conflict: string
}

/** 合成的 sharelink 命名空间段（与后端一致，忽略它但不做大小写折叠） */
const NAMESPACE_SEGMENT = /^sharelink\d+-\d+$/

/** 拆分路径段：只处理分隔符与分隔符前空白，文件名空白原样保留 */
function sharePathSegments(raw: string): string[] {
  if (!raw) return []
  // 根分隔符之前的空白只属于分隔符，不属于任何文件名；分隔符之后的空白属于
  // 文件名本身，必须保留。
  const lead = /^\s*/.exec(raw)?.[0] ?? ''
  const rest = lead.length > 0 && raw.charAt(lead.length) === '/' ? raw.slice(lead.length) : raw
  return rest.split('/').filter(segment => segment.length > 0)
}

/**
 * 规范化分享路径（原始 API / 已保存的完整路径）：合并重复斜杠、去掉首尾斜杠，
 * 并丢弃最前面合成的 sharelink 命名空间段。
 *
 * 只规整分隔符与合成命名空间：**文件名里的空白一律原样保留**（含结尾空格），
 * 否则「报告. 」这类文件名会与「报告.」归一成同一个 key，导致还原与选择串档。
 */
export function normalizeSharePath(raw: string): string {
  const segments = sharePathSegments(raw)
  if (segments.length > 0 && NAMESPACE_SEGMENT.test(segments[0])) {
    segments.shift()
  }
  return segments.join('/')
}

/**
 * 规范化逻辑 key（已去掉虚拟命名空间后的内容路径或 server_filename）：只处理
 * 分隔符，不剥离任何命名空间。整个文件名恰好形如 sharelink1-2 时必须保持字面值。
 */
export function normalizeShareKey(raw: string): string {
  return sharePathSegments(raw).join('/')
}

/**
 * 根据根目录实际列表建立「原始根路径 → 逻辑根名」别名。
 *
 * 只接受真实列举到的根项：不按后缀猜测，也不全局剥离 /apps/bypy。任何无法唯一
 * 对应的组合都整体失败（返回空表 + 冲突提示），避免把不同内容错并成一个 key。
 */
export function buildShareRootAliases(items: ShareRootAliasItem[]): ShareRootAliases {
  const byRaw = new Map<string, string>()
  const logicalOwners = new Map<string, string>()
  let conflict = ''

  for (const item of items) {
    const rawKey = normalizeSharePath(item.rawPath)
    const logicalKey = normalizeShareKey(item.logicalName)
    if (!rawKey || !logicalKey) continue
    const existing = byRaw.get(rawKey)
    if (existing !== undefined) {
      if (existing !== logicalKey) conflict = SHARE_ALIAS_CONFLICT_MESSAGE
      continue
    }
    const owner = logicalOwners.get(logicalKey)
    if (owner !== undefined && owner !== rawKey) {
      conflict = SHARE_ALIAS_CONFLICT_MESSAGE
      continue
    }
    byRaw.set(rawKey, logicalKey)
    logicalOwners.set(logicalKey, rawKey)
  }

  if (conflict) return { byRaw: new Map(), conflict }

  const raws = [...byRaw.keys()]
  for (let i = 0; i < raws.length; i++) {
    for (let j = i + 1; j < raws.length; j++) {
      if (isSharePathAncestor(raws[i], raws[j]) || isSharePathAncestor(raws[j], raws[i])) {
        conflict = SHARE_ALIAS_CONFLICT_MESSAGE
        break
      }
    }
    if (conflict) break
  }

  if (!conflict) {
    // raw 与 logical 也必须在段边界上互不包含：否则某个 logical 后代可能恰好以
    // 另一条 raw 为前缀，映射会把保存的逻辑路径改写到不同内容。自身 raw==logical
    // 的恒等项允许，但仅在不与任何其他根重叠时成立；非恒等的自身包含也失败。
    const mappings = [...byRaw].map(([raw, logical]) => ({ raw, logical }))
    const logicals = [...logicalOwners.keys()]
    for (const mapping of mappings) {
      for (const logical of logicals) {
        const identitySelf = mapping.raw === mapping.logical && logical === mapping.logical
        if (identitySelf) continue
        if (mapping.raw === logical) {
          conflict = SHARE_ALIAS_CONFLICT_MESSAGE
          break
        }
        if (
          isSharePathAncestor(mapping.raw, logical)
          || isSharePathAncestor(logical, mapping.raw)
        ) {
          conflict = SHARE_ALIAS_CONFLICT_MESSAGE
          break
        }
      }
      if (conflict) break
    }
  }

  if (conflict) return { byRaw: new Map(), conflict }
  return { byRaw, conflict: '' }
}

/** 用根目录别名解析任意 key：精确匹配或按路径段替换前缀，其余保持不变 */
export function resolveShareKey(key: string, aliases: ShareRootAliases): string {
  if (!key || aliases.byRaw.size === 0) return key
  const direct = aliases.byRaw.get(key)
  if (direct !== undefined) return direct
  let matchedRaw = ''
  for (const raw of aliases.byRaw.keys()) {
    if (key.startsWith(`${raw}/`) && raw.length > matchedRaw.length) matchedRaw = raw
  }
  if (!matchedRaw) return key
  const logical = aliases.byRaw.get(matchedRaw)
  return logical ? `${logical}/${key.slice(matchedRaw.length + 1)}` : key
}

/**
 * 展示用路径：仅去掉路径最前面合法的合成 sharelink<digit>-<digit> 命名空间段。
 * 不改模型值、请求值或规则输入，也不动文件名空白与其他相似写法。
 */
export function formatSharePathForDisplay(path: string): string {
  if (!path) return path
  const match = /^(\/?)(sharelink\d+-\d+)\//.exec(path)
  if (!match) return path
  const rest = path.slice(match[0].length)
  if (!rest) return path
  return `${match[1]}${rest}`
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
  aliases?: ShareRootAliases,
): ShareSelectionState {
  const initialRaw = [...initialPaths]
  const selected = new Set<string>()
  const rawPaths = new Map<string, string>()
  for (const path of initialRaw) {
    const key = resolveShareKey(normalizeSharePath(path), aliases ?? { byRaw: new Map(), conflict: '' })
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
export function isShareSelectionUnchanged(state: ShareSelectionState): boolean {
  if (state.wholeShare !== state.initialWholeShare) return false
  if (state.selected.size !== state.initialKeys.size) return false
  for (const key of state.selected) {
    if (!state.initialKeys.has(key)) return false
  }
  return true
}

/** 当前选择是否与打开时完全一致 */
function isUnchanged(state: ShareSelectionState): boolean {
  return isShareSelectionUnchanged(state)
}

/**
 * 根目录别名变化后用新别名重新解释同一份选择。
 *
 * 未改动时按初始原始数组重建，保证 no-op 仍逐字节回显初始写法；用户已改动时只
 * 迁移 key，保留原始 raw 与顺序，未加载出的选择不受影响。
 */
export function rehydrateShareSelection(
  state: ShareSelectionState,
  aliases: ShareRootAliases,
): ShareSelectionState {
  if (isUnchanged(state)) {
    return createShareSelection(state.initialRaw, state.initialWholeShare, aliases)
  }

  const selected = new Set<string>()
  const rawPaths = new Map<string, string>()
  for (const key of state.selected) {
    const nextKey = resolveShareKey(key, aliases)
    if (!nextKey) continue
    selected.add(nextKey)
    if (!rawPaths.has(nextKey)) rawPaths.set(nextKey, state.rawPaths.get(key) ?? key)
  }

  const initialKeys = new Set<string>()
  for (const raw of state.initialRaw) {
    const key = resolveShareKey(normalizeSharePath(raw), aliases)
    if (key) initialKeys.add(key)
  }

  return {
    selected,
    rawPaths,
    wholeShare: state.wholeShare,
    initialRaw: state.initialRaw,
    initialKeys,
    initialWholeShare: state.initialWholeShare,
  }
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
  const target = normalizeShareKey(targetKey)
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
  const expandedDirs: string[] = []
  let levelKey = outermost ?? ''
  for (;;) {
    expandedDirs.push(levelKey)
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
  return { ok: true, state: next, expandedDirs }
}

/**
 * 目录（或整个分享哨兵 ''）当前是否仍被整体选中。
 *
 * 用于判断此前被拆成逐项选择的目录是否已被用户重新整体勾选：整体选中时其后续新增
 * 内容会再次被自动覆盖。
 */
export function shareSelectionCoversWhole(state: ShareSelectionState, dirKey: string): boolean {
  if (dirKey === '') return state.wholeShare
  return shareSelectionCovers(state, dirKey)
}
