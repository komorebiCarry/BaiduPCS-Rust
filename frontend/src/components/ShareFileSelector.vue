<template>
  <div class="share-file-selector" :class="{ 'is-mobile': isMobile }" ref="rootRef">
    <!-- 加载状态 -->
    <div v-if="loading || navigating" class="loading-container">
      <el-icon class="is-loading"><Loading /></el-icon>
      <span>{{ navigating ? '加载目录中...' : '加载文件列表中...' }}</span>
    </div>

    <template v-else-if="currentFiles.length > 0">
      <!-- 顶部操作栏：面包屑 + 全选 + 计数 -->
      <div class="selector-header">
        <div class="header-top">
          <!-- 面包屑导航 -->
          <div v-if="pathStack.length > 0" class="breadcrumb">
            <el-button link type="primary" size="small" @click="navigateToRoot">
              根目录
            </el-button>
            <template v-for="(item, index) in pathStack" :key="index">
              <span class="breadcrumb-sep">/</span>
              <el-button
                  link
                  :type="index === pathStack.length - 1 ? 'default' : 'primary'"
                  size="small"
                  :disabled="index === pathStack.length - 1"
                  @click="navigateToLevel(index)"
              >
                {{ item.name }}
              </el-button>
            </template>
          </div>
        </div>
        <div class="header-bottom">
          <el-checkbox
              :key="selectionVisualRevision"
              :model-value="isAllCurrentSelected"
              :indeterminate="isCurrentIndeterminate"
              :disabled="pathMode && selectionBusy"
              @change="handleSelectAllCurrent"
          >
            全选
          </el-checkbox>
          <span class="select-info">
            <template v-if="pathMode">
              {{ pathSelectionSummary }}
            </template>
            <template v-else>
              已选 {{ totalSelectedCount }} 个{{ folderOnly ? '文件夹' : '文件' }}
              <span v-if="selectedTotalSize > 0" class="size-info">
                ({{ formatFileSize(selectedTotalSize) }})
              </span>
            </template>
          </span>
        </div>
        <div v-if="pathMode && (selectionBusy || selectionProblem)" class="selection-status">
          <span v-if="selectionBusy">正在展开目录…</span>
          <span v-else class="selection-error">{{ selectionProblem }}</span>
        </div>
      </div>

      <!-- 文件列表 -->
      <el-scrollbar ref="scrollbarRef" :max-height="isMobile ? '250px' : '300px'">
        <div class="file-list" ref="fileListRef">
          <div
              v-for="file in currentFiles"
              :key="file.fs_id"
              class="file-item"
              :data-fs-id="file.fs_id"
              @click="handleItemClick(file)"
          >
            <el-checkbox
                v-if="!folderOnly || file.is_dir"
                :key="`${file.fs_id}:${selectionVisualRevision}`"
                :model-value="rowChecked(file)"
                :indeterminate="rowIndeterminate(file)"
                :disabled="pathMode && selectionBusy"
                :aria-label="`选择 ${file.name}`"
                @change="(val: boolean) => handleCheckChange(file.fs_id, val, file)"
                @click.stop
            />
            <el-icon class="file-icon" :class="{ 'folder-icon': file.is_dir }">
              <Folder v-if="file.is_dir" />
              <Document v-else />
            </el-icon>
            <span class="file-name" :title="file.name">{{ file.name }}</span>
            <span v-if="file.is_dir" class="file-enter">
              <el-icon><ArrowRight /></el-icon>
            </span>
            <span class="file-size">{{ file.is_dir ? '文件夹' : formatFileSize(file.size) }}</span>
          </div>
          <!-- 加载更多提示 -->
          <div v-if="loadingMore" class="load-more-tip">
            <el-icon class="is-loading"><Loading /></el-icon>
            <span>加载更多...</span>
          </div>
          <div v-else-if="!hasMore" class="load-more-tip no-more">
          </div>
        </div>
      </el-scrollbar>
    </template>

    <!-- 空目录 -->
    <div v-else-if="!loading && !navigating" class="empty-container">
      <template v-if="pathStack.length > 0">
        <span>该目录为空</span>
        <el-button link type="primary" @click="navigateToRoot" style="margin-top: 8px">
          返回根目录
        </el-button>
      </template>
      <span v-else>暂无文件</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onMounted, onUpdated, onBeforeUnmount, nextTick } from 'vue'
import { Loading, Folder, Document, ArrowRight } from '@element-plus/icons-vue'
import { useIsMobile } from '@/utils/responsive'
import { formatFileSize } from '@/api/utils'
import { previewShareDir, previewShareFiles, type SharedFileInfo, type PreviewShareInfo } from '@/api/transfer'
import {
  buildShareRootAliases,
  cloneShareSelection,
  createShareSelection,
  deselectSharePath,
  normalizeShareKey,
  normalizeSharePath,
  rehydrateShareSelection,
  resolveShareKey,
  selectSharePath,
  SHARE_ALIAS_CONFLICT_MESSAGE,
  shareSelectionCovers,
  shareSelectionCoversWhole,
  shareSelectionPaths,
  shareSelectionReady,
  shareSelectionTriState,
  type ShareRootAliases,
  type ShareSelectionEntry,
  type ShareSelectionState,
  type ShareTriState,
} from '@/utils/shareSelection'

const isMobile = useIsMobile()

interface PathEntry {
  name: string
  dir: string
  /** 规范化后的目录 key */
  key: string
}

const props = withDefaults(defineProps<{
  files: SharedFileInfo[]
  loading: boolean
  shareInfo?: PreviewShareInfo | null
  /** 首次预览的分享链接（用于根目录分页加载） */
  shareUrl?: string
  /** 首次预览的密码 */
  sharePassword?: string
  /**
   * 仅允许选择文件夹。
   *
   * 文件不可勾选、点击文件不切换选中；进入子目录也不会把子文件自动选中 ——
   * 否则目录会被展开成一批文件路径，语义全乱。
   */
  folderOnly?: boolean
  /**
   * 启用路径选择模式（分享同步的同步内容 / 排除内容使用）。
   *
   * 该模式下按路径而不是 fs_id 维护选择，勾选目录代表整棵子树；未开启时保持原有
   * 的根目录自动全选与 fs_id 行为。
   */
  pathSelection?: boolean
  /** 路径选择模式的初始已选路径（原始字符串） */
  initialSelectedPaths?: string[]
  /** 空选择是否代表「整个分享」（仅同步内容使用） */
  emptySelectionMeansAll?: boolean
}>(), {
  pathSelection: false,
  initialSelectedPaths: () => [],
  emptySelectionMeansAll: false,
})

const emit = defineEmits<{
  'update:selectedFsIds': [fsIds: number[]]
  'update:selectedFiles': [files: SharedFileInfo[]]
  'update:selectedPaths': [paths: string[]]
  'update:selectionReady': [ready: boolean]
  /**
   * 因「取消整体选中目录里的个别项目」而被拆成逐项选择、且尚未被重新整体勾选的目录
   * （展示用路径，如 `/剧集/S01`；整个分享记为 `''`）。仅路径选择模式发出。
   */
  'update:followLostDirs': [dirs: string[]]
}>()

// 目录导航状态
const navigating = ref(false)
const pathStack = ref<PathEntry[]>([])
const dirFiles = ref<SharedFileInfo[]>([])

// 分页状态
const PAGE_SIZE = 100
const currentPage = ref(1)
const hasMore = ref(true)
const loadingMore = ref(false)

// 根目录分页状态独立保存：进入子目录会改写 currentPage/hasMore，返回根目录时按此恢复
const rootPage = ref(1)
const rootHasMore = ref(true)

// 分页请求所有权：旧请求（成功/失败/finally）不得影响更新的请求
let pagingRequestId = 0
let activePagingRequest: number | null = null

/** 导航、数据源重置、卸载时废弃进行中的分页请求，并立即释放加载态 */
function retirePagingRequest() {
  pagingRequestId++
  activePagingRequest = null
  loadingMore.value = false
}

// 根目录分页加载的额外文件（第2页及之后）
const rootExtraFiles = ref<SharedFileInfo[]>([])

// 滚动容器引用
const scrollbarRef = ref<any>(null)
const fileListRef = ref<HTMLElement | null>(null)

// 当前显示的文件列表（根目录用 props.files + rootExtraFiles，子目录用 dirFiles）
const currentFiles = computed(() =>
    pathStack.value.length > 0 ? dirFiles.value : [...props.files, ...rootExtraFiles.value]
)

// 全局选中状态（跨目录保持）
const checkedFsIds = ref<Set<number>>(new Set())

// 所有已知文件的 map（用于计算选中大小）
const allKnownFiles = ref<Map<number, SharedFileInfo>>(new Map())

// 文件夹 fs_id → 子文件 fs_id 列表（进入过的文件夹才有记录）
const folderChildren = ref<Map<number, number[]>>(new Map())

// ==================== 路径选择模式 ====================

const pathMode = computed(() => props.pathSelection === true)
const emptyMeansAll = computed(() => props.emptySelectionMeansAll === true)

/** 根目录上下文别名：物理根路径与虚拟命名空间后代归一到同一逻辑 key */
const rootAliases = ref<ShareRootAliases>(buildShareRootAliases([]))
const selectionState = ref<ShareSelectionState>(createShareSelection([], false))
const selectionBusy = ref(false)
const selectionError = ref('')
/** 事务内发现的根目录别名冲突：不落盘所选内容，但必须阻止确认 */
const contextConflict = ref('')

const aliasConflict = computed(() => rootAliases.value.conflict)
const selectionProblem = computed(() => aliasConflict.value || contextConflict.value || selectionError.value)

/**
 * ElementPlus 复选框会先更新内部勾选视觉再调用 change 回调；事务回滚时模型没有变化，
 * 只改 model-value 不会让组件重新同步，视觉会停在用户点过的状态。递增这个代次让
 * 全选框与行复选框重新挂载，按模型值重建视觉。
 */
const selectionVisualRevision = ref(0)
const rootRef = ref<HTMLElement | null>(null)

type SelectionFocusTarget = { kind: 'header' } | { kind: 'row'; fsId: number }

function focusedSelectionTarget(): SelectionFocusTarget | null {
  const root = rootRef.value
  const active = document.activeElement
  if (!root || !root.isConnected || !(active instanceof HTMLElement) || !root.contains(active)) return null
  const row = active.closest<HTMLElement>('.file-item')
  if (row) {
    const fsId = Number(row.dataset.fsId)
    return Number.isFinite(fsId) ? { kind: 'row', fsId } : null
  }
  return active.closest('.selector-header') ? { kind: 'header' } : null
}

function restoreSelectionFocus(target: SelectionFocusTarget) {
  const root = rootRef.value
  if (!root || !root.isConnected) return
  const active = document.activeElement
  // 事务期间用户把焦点移到别处（含导航控件）时不再抢回；重挂载后通常落在 body
  if (active instanceof HTMLElement && active !== document.body && active !== document.documentElement) return
  const input = target.kind === 'header'
    ? root.querySelector<HTMLInputElement>('.selector-header input[type="checkbox"]')
    : Array.from(root.querySelectorAll<HTMLElement>('.file-item'))
        .find(el => Number(el.dataset.fsId) === target.fsId)
        ?.querySelector<HTMLInputElement>('input[type="checkbox"]') ?? null
  if (!input || input.disabled) return
  input.focus({ preventScroll: true })
}

function resyncSelectionVisuals(
  focusTarget: SelectionFocusTarget | null = focusedSelectionTarget(),
) {
  selectionVisualRevision.value++
  if (!focusTarget) return
  nextTick(() => restoreSelectionFocus(focusTarget))
}

/** 目录 key → 已知直接子项（complete 为真时代表已拿到全部子项） */
const childrenCache = ref(new Map<string, { entries: ShareSelectionEntry[]; complete: boolean }>())

/** 目录 key → 服务端原始路径（拼接目录请求参数用） */
const pathIndex = new Map<string, string>()

/** 代次守卫：目录重载、快速切换或组件卸载后，进行中的展开结果一律丢弃 */
let selectionGeneration = 0

/** 导航代次：并发导航只有最后一次可以落盘与结算就绪状态 */
let navigationGeneration = 0

/**
 * 作废进行中的选择事务并释放它的 busy 所有权。
 *
 * 导航、关闭、数据源重置都会改变上下文：旧事务的异步结果既不能落盘到新上下文，
 * 也不能继续占着 busy（否则控件会永久停在「正在展开目录…」而确认按钮却被点亮）。
 * 这里原子地递增代次并清掉 busy，旧事务在 finally 里因代次不匹配不会再改状态。
 */
function cancelSelectionTransaction(restoreFocus = true) {
  selectionGeneration++
  contextConflict.value = ''
  if (!selectionBusy.value && !selectionError.value) return
  selectionBusy.value = false
  selectionError.value = ''
  resyncSelectionVisuals(restoreFocus ? focusedSelectionTarget() : null)
}

/** 路径模式的只读就绪状态：事务 / 导航 / 首次加载未结束前一律不就绪 */
function selectionReadiness(): boolean {
  return !aliasConflict.value
      && !contextConflict.value
      && !selectionBusy.value
      && !navigating.value
      && !props.loading
      && shareSelectionReady(selectionState.value, emptyMeansAll.value)
}

/** 统一就绪状态出口：任何路径都必须经过 busy / 加载判定，不直接透传模型结果 */
function emitSelectionReady() {
  if (!pathMode.value) return
  emit('update:selectionReady', selectionReadiness())
}

const initialPathsSignature = computed(() => (props.initialSelectedPaths ?? []).join('\u0001'))

function currentRootFiles(): SharedFileInfo[] {
  return [...props.files, ...rootExtraFiles.value]
}

/** 别名更新后同步已缓存条目与请求索引，避免同一内容留下两种 key */
function remapAliasedCaches(aliases: ShareRootAliases) {
  if (aliases.byRaw.size === 0) return
  const nextCache = new Map<string, { entries: ShareSelectionEntry[]; complete: boolean }>()
  for (const [dirKey, cached] of childrenCache.value) {
    const nextDirKey = resolveShareKey(dirKey, aliases)
    const entries = cached.entries.map((entry) => {
      const key = resolveShareKey(entry.key, aliases)
      return key === entry.key ? entry : { ...entry, key }
    })
    const existing = nextCache.get(nextDirKey)
    nextCache.set(nextDirKey, {
      entries: existing ? [...existing.entries, ...entries] : entries,
      complete: existing ? existing.complete && cached.complete : cached.complete,
    })
  }
  childrenCache.value = nextCache

  const nextIndex = new Map<string, string>()
  for (const [key, raw] of pathIndex) {
    nextIndex.set(resolveShareKey(key, aliases), raw)
  }
  pathIndex.clear()
  for (const [key, raw] of nextIndex) pathIndex.set(key, raw)
}

/** 从当前根列表重建别名；晚到的根分页只补映射，不重置用户已改动选择 */
function refreshRootAliases(files: SharedFileInfo[] = currentRootFiles()) {
  const aliases = buildShareRootAliases(
    files.map(file => ({ rawPath: file.path, logicalName: file.name })),
  )
  rootAliases.value = aliases
  if (!pathMode.value) return
  remapAliasedCaches(aliases)
  selectionState.value = rehydrateShareSelection(selectionState.value, aliases)
}

/**
 * 被拆成逐项选择的目录 key（从外到内、去重；整个分享为 ''）。
 *
 * 取消整体选中目录里的个别项目时，选择会把该目录替换成「剩余兄弟」的显式列表，
 * 目录之后新增的内容不再被自动覆盖。这里记下这些目录，供父组件提示用户；目录被
 * 重新整体勾选或其下已一项不选后自动从提示中消失。
 */
const followLostKeys = ref<string[]>([])

const activeFollowLostDirs = computed(() => {
  if (!pathMode.value) return []
  const state = selectionState.value
  const aliases = rootAliases.value
  const out: string[] = []
  const seen = new Set<string>()
  for (const raw of followLostKeys.value) {
    const key = raw === '' ? '' : resolveShareKey(raw, aliases)
    if (seen.has(key)) continue
    seen.add(key)
    if (shareSelectionCoversWhole(state, key)) continue
    // 目录下已一项不选：不存在「逐项选择」，也就无所谓自动跟随
    const nothingLeft = key === ''
      ? state.selected.size === 0
      : shareSelectionTriState(state, key, true) === 'unchecked'
    if (nothingLeft) continue
    out.push(key === '' ? '' : `/${key}`)
  }
  return out
})

watch(
  activeFollowLostDirs,
  (dirs, prev) => {
    if (!pathMode.value) return
    if (prev && dirs.length === prev.length && dirs.every((d, i) => d === prev[i])) return
    emit('update:followLostDirs', [...dirs])
  },
)

watch(
  [initialPathsSignature, emptyMeansAll, pathMode],
  () => {
    if (!pathMode.value) return
    cancelSelectionTransaction()
    childrenCache.value = new Map()
    pathIndex.clear()
    selectionState.value = createShareSelection(
      props.initialSelectedPaths ?? [],
      emptyMeansAll.value,
      rootAliases.value,
    )
    selectionError.value = ''
    selectionBusy.value = false
    followLostKeys.value = []
    emitSelection()
  },
  { immediate: true },
)

// 加载态变化同样要结算就绪：父组件据此启用 / 禁用确认
watch([() => props.loading, navigating], () => emitSelectionReady())

/** 当前所在目录的规范化 key（根目录为 ''） */
function currentDirKey(): string {
  if (pathStack.value.length === 0) return ''
  return pathStack.value[pathStack.value.length - 1].key
}

/** 条目自身的规范化 key（根目录先按实际列表别名归一，再参与所有选择计算） */
function pathKeyOf(file: SharedFileInfo): string {
  return resolveShareKey(normalizeSharePath(file.path), rootAliases.value)
}

/** 记录某个目录的直接子项，供三态推导复用 */
function recordDirectoryChildren(dirKey: string, files: SharedFileInfo[], complete: boolean) {
  const entries: ShareSelectionEntry[] = []
  for (const file of files) {
    const key = pathKeyOf(file)
    if (!key) continue
    entries.push({ key, raw: file.path, isDir: file.is_dir })
    pathIndex.set(key, file.path)
  }
  const next = new Map(childrenCache.value)
  next.set(dirKey, { entries, complete })
  childrenCache.value = next
}

/**
 * 记录一页目录响应。
 *
 * 目录完整性的唯一判定：该页不足 PAGE_SIZE 即已到末尾，此时累积列表才是完整列表；
 * 满页仍可能有后续分页，不能据此推断整目录覆盖。空页同样走这里，避免漏掉结束页。
 */
function recordListingPage(dirKey: string, files: SharedFileInfo[], pageLength: number) {
  recordDirectoryChildren(dirKey, files, pageLength < PAGE_SIZE)
}

/** 条目三态（仅路径模式使用） */
function rowTriState(file: SharedFileInfo): ShareTriState {
  const key = pathKeyOf(file)
  const cached = childrenCache.value.get(key)
  return shareSelectionTriState(
    selectionState.value,
    key,
    file.is_dir,
    cached?.complete ? cached.entries : undefined,
  )
}

function rowChecked(file: SharedFileInfo): boolean {
  if (!pathMode.value) return isFolderChecked(file)
  return rowTriState(file) === 'checked'
}

function rowIndeterminate(file: SharedFileInfo): boolean {
  if (!pathMode.value) return isFolderIndeterminate(file)
  return rowTriState(file) === 'indeterminate'
}

const pathSelectionSummary = computed(() => {
  const state = selectionState.value
  if (state.wholeShare) return '已选整个分享'
  return `已选 ${state.selected.size} 项`
})

/** 目录请求参数：企业版用服务端原样路径，个人版补 sharelink 命名空间 */
function dirRequestPath(dirKey: string): string {
  const raw = pathIndex.get(dirKey)
  const shareInfo = props.shareInfo
  if (shareInfo?.kind === 'apaas') {
    return raw ?? `/${dirKey}`
  }
  const prefix = shareInfo ? `/sharelink${shareInfo.uk}-${shareInfo.shareid}` : ''
  if (pathMode.value) {
    // 路径模式一律用逻辑 key 组合虚拟命名空间，不能把根目录的物理路径塞进 sharelink
    return `${prefix}/${dirKey}`
  }
  if (raw) {
    if (raw.startsWith('/sharelink')) return raw
    return `${prefix}/${raw.replace(/^\/+/, '')}`
  }
  return `${prefix}/${dirKey}`
}

/** 拉取某目录的一页直接子项 */
async function fetchChildrenPage(dirKey: string, page: number): Promise<SharedFileInfo[]> {
  if (dirKey === '') {
    if (!props.shareUrl) throw new Error('缺少分享链接，无法加载目录')
    const response = await previewShareFiles({
      share_url: props.shareUrl,
      password: props.sharePassword || undefined,
      page,
      num: PAGE_SIZE,
    })
    return response.files || []
  }
  if (!props.shareInfo) throw new Error('缺少分享信息，无法加载目录')
  const response = await previewShareDir({
    short_key: props.shareInfo.short_key,
    shareid: props.shareInfo.shareid,
    uk: props.shareInfo.uk,
    bdstoken: props.shareInfo.bdstoken,
    kind: props.shareInfo.kind,
    token: props.shareInfo.token,
    dir: dirRequestPath(dirKey),
    page,
    num: PAGE_SIZE,
  })
  return response.files || []
}

interface FetchedChildren {
  entries: ShareSelectionEntry[]
  /** 根目录事务内一并验证出的别名；仅在代次仍有效时才会落盘 */
  aliases?: ShareRootAliases
  cancelled?: boolean
}

/**
 * 拉取某目录的全部直接子项（分页到底，重复响应立即中止）。
 *
 * 根目录必须先把**所有页**的原始项收集齐，再按与展示列表相同的别名规则统一
 * 验证并生成 canonical key；任何一页出现别名冲突都整体失败，不能边翻页边合并。
 */
async function fetchAllChildren(
  dirKey: string,
  generation?: number,
): Promise<FetchedChildren> {
  const rawFiles: SharedFileInfo[] = []
  const seenEvidence = new Set<string>()
  const isCurrent = () => generation === undefined || generation === selectionGeneration
  for (let page = 1; ; page++) {
    // 事务已取消时不再发起下一页请求，而不是等整个目录拉完再丢弃
    if (!isCurrent()) return { entries: [], cancelled: true }
    const files = await fetchChildrenPage(dirKey, page)
    if (!isCurrent()) return { entries: [], cancelled: true }
    if (!files || files.length === 0) break
    let fresh = 0
    for (const file of files) {
      const rawKey = normalizeSharePath(file.path)
      if (!rawKey) continue
      // 根目录必须保留同名原始路径的全部名称证据：只有 raw+name 完全相同的普通
      // 重复分页才可去重；同一 raw 对应不同 server_filename 要留给别名校验报冲突。
      const logicalKey = normalizeShareKey(file.name)
      const evidenceKey = dirKey === '' ? `${rawKey}\u0000${logicalKey}` : rawKey
      if (seenEvidence.has(evidenceKey)) continue
      seenEvidence.add(evidenceKey)
      fresh++
      rawFiles.push(file)
    }
    if (fresh === 0) throw new Error('目录分页返回重复内容，已中止')
    if (files.length < PAGE_SIZE) break
  }
  if (!isCurrent()) return { entries: [], cancelled: true }

  const aliases = dirKey === ''
    ? buildShareRootAliases(rawFiles.map(file => ({ rawPath: file.path, logicalName: file.name })))
    : rootAliases.value
  if (dirKey === '' && aliases.conflict) throw new Error(aliases.conflict)

  const entries: ShareSelectionEntry[] = []
  const seenKeys = new Set<string>()
  for (const file of rawFiles) {
    const key = resolveShareKey(normalizeSharePath(file.path), aliases)
    if (!key || seenKeys.has(key)) continue
    seenKeys.add(key)
    entries.push({ key, raw: file.path, isDir: file.is_dir })
  }
  return { entries, aliases: dirKey === '' ? aliases : undefined }
}

/** 取得某目录的全部直接子项（已知完整则复用缓存） */
async function loadCompleteChildren(
  dirKey: string,
  generation?: number,
): Promise<ShareSelectionEntry[]> {
  const cached = childrenCache.value.get(dirKey)
  if (cached?.complete) return cached.entries
  const fetched = await fetchAllChildren(dirKey, generation)
  // 导航或新事务已作废旧代次：结果直接丢弃，不写缓存也不改路径索引
  if (fetched.cancelled) return fetched.entries
  if (generation !== undefined && generation !== selectionGeneration) return fetched.entries
  if (fetched.aliases) {
    // 所有根页已统一验证通过，才允许提交别名并迁移当前编辑中的选择
    rootAliases.value = fetched.aliases
    remapAliasedCaches(fetched.aliases)
    selectionState.value = rehydrateShareSelection(selectionState.value, fetched.aliases)
  }
  for (const entry of fetched.entries) {
    pathIndex.set(entry.key, entry.raw)
  }
  const next = new Map(childrenCache.value)
  next.set(dirKey, { entries: fetched.entries, complete: true })
  childrenCache.value = next
  return fetched.entries
}

/** 路径模式下应用一次选择变更并向外同步 */
function applySelection(next: ShareSelectionState) {
  selectionState.value = next
  emitSelection()
}

/** 路径模式：勾选/取消勾选某个条目 */
async function handlePathCheckChange(checked: boolean, file: SharedFileInfo) {
  const key = pathKeyOf(file)
  if (!key) return
  if (checked) {
    // 新的操作作废进行中的展开，并释放它占用的 busy，避免旧事务把控件卡住
    cancelSelectionTransaction()
    applySelection(selectSharePath(selectionState.value, { key, raw: file.path, isDir: file.is_dir }))
    return
  }
  await deselectPath(key)
}

/** 路径模式：取消一条路径（必要时展开祖先链） */
async function deselectPath(key: string) {
  if (selectionBusy.value) return
  const focusTarget = focusedSelectionTarget()
  const generation = ++selectionGeneration
  selectionBusy.value = true
  selectionError.value = ''
  contextConflict.value = ''
  emitSelectionReady()
  try {
    const outcome = await deselectSharePath(
      selectionState.value,
      key,
      (dirKey) => loadCompleteChildren(dirKey, generation),
    )
    if (generation !== selectionGeneration) return
    if (!outcome.ok) {
      const message = outcome.error || '取消选择失败，请稍后重试'
      if (message === SHARE_ALIAS_CONFLICT_MESSAGE) contextConflict.value = message
      else selectionError.value = message
      resyncSelectionVisuals(focusTarget)
      return
    }
    selectionState.value = rehydrateShareSelection(outcome.state, rootAliases.value)
    if (outcome.expandedDirs?.length) {
      const known = new Set(followLostKeys.value)
      const added = outcome.expandedDirs.filter((key) => !known.has(key))
      if (added.length > 0) followLostKeys.value = [...followLostKeys.value, ...added]
    }
  } catch (error) {
    if (generation !== selectionGeneration) return
    const message = error instanceof Error && error.message
      ? error.message
      : '取消选择失败，请稍后重试'
    if (message === SHARE_ALIAS_CONFLICT_MESSAGE) contextConflict.value = message
    else selectionError.value = message
    resyncSelectionVisuals(focusTarget)
  } finally {
    // 先释放 busy 再统一外发，就绪状态才不会在事务未结束时被点亮；
    // 代次不匹配说明已被导航 / 新事务接管，旧事务不得清掉新的 busy 或就绪状态
    if (generation === selectionGeneration) {
      selectionBusy.value = false
      emitSelection()
    }
  }
}

/** 路径模式：当前目录的全选 / 取消全选（作用于整个目录列表，含后续分页） */
async function handleSelectAllCurrentPath() {
  if (selectionBusy.value) return
  const dirKey = currentDirKey()
  if (isAllCurrentSelected.value) {
    if (dirKey === '') {
      cancelSelectionTransaction()
      const next = cloneShareSelection(selectionState.value)
      next.wholeShare = false
      next.selected.clear()
      applySelection(next)
      return
    }
    await deselectPath(dirKey)
    return
  }

  const focusTarget = focusedSelectionTarget()
  const generation = ++selectionGeneration
  selectionBusy.value = true
  selectionError.value = ''
  contextConflict.value = ''
  emitSelectionReady()
  try {
    const children = await loadCompleteChildren(dirKey, generation)
    if (generation !== selectionGeneration) return
    let next = selectionState.value
    for (const child of children) {
      next = selectSharePath(next, child)
    }
    selectionState.value = next
  } catch (error) {
    if (generation !== selectionGeneration) return
    const message = error instanceof Error && error.message
      ? error.message
      : '加载目录失败，请稍后重试'
    if (message === SHARE_ALIAS_CONFLICT_MESSAGE) contextConflict.value = message
    else selectionError.value = '加载目录失败，请稍后重试'
    resyncSelectionVisuals(focusTarget)
  } finally {
    if (generation === selectionGeneration) {
      selectionBusy.value = false
      emitSelection()
    }
  }
}

// 文件夹 checkbox 选中状态：
// - 未进入过的文件夹：直接看 checkedFsIds
// - 进入过的文件夹：看子文件是否全选
function isFolderChecked(file: SharedFileInfo): boolean {
  if (!file.is_dir) return checkedFsIds.value.has(file.fs_id)
  if (props.folderOnly) return checkedFsIds.value.has(file.fs_id)
  const children = folderChildren.value.get(file.fs_id)
  if (!children || children.length === 0) {
    // 没进入过，用文件夹自身的选中状态
    return checkedFsIds.value.has(file.fs_id)
  }
  // 进入过：全部子文件都选中 = checked
  return children.every(id => checkedFsIds.value.has(id))
}

// 文件夹半选状态：进入过且部分子文件选中
function isFolderIndeterminate(file: SharedFileInfo): boolean {
  if (!file.is_dir) return false
  if (props.folderOnly) return false
  const children = folderChildren.value.get(file.fs_id)
  if (!children || children.length === 0) return false
  const selectedCount = children.filter(id => checkedFsIds.value.has(id)).length
  return selectedCount > 0 && selectedCount < children.length
}

/**
 * 当前目录是否被**整体**覆盖。
 *
 * 显式选中当前目录、其任意祖先或整个分享哨兵都保证整棵子树已选；否则只有拿到该
 * 目录的**完整**列表且全部子项都被覆盖才算全选。仅加载了第一页时后续分页未知，
 * 只能算半选 —— 否则会把「第一页全选」误判成整目录已选，点一下就会清空所有选择。
 */
function currentDirFullyCovered(): boolean {
  const state = selectionState.value
  const dirKey = currentDirKey()
  if (shareSelectionCovers(state, dirKey)) return true
  const cached = childrenCache.value.get(dirKey)
  if (cached?.complete && cached.entries.length > 0) {
    return cached.entries.every(entry => shareSelectionCovers(state, entry.key))
  }
  return false
}

// 当前目录的全选状态（考虑文件夹的展开状态）
const isAllCurrentSelected = computed(() => {
  const targets = props.folderOnly
    ? currentFiles.value.filter(f => f.is_dir)
    : currentFiles.value
  if (targets.length === 0) return false
  if (pathMode.value) return currentDirFullyCovered()
  return targets.every(f => isFolderChecked(f))
})

const isCurrentIndeterminate = computed(() => {
  const targets = props.folderOnly
    ? currentFiles.value.filter(f => f.is_dir)
    : currentFiles.value
  if (targets.length === 0) return false
  if (pathMode.value) {
    if (currentDirFullyCovered()) return false
    return targets.some(f => rowTriState(f) !== 'unchecked')
  }
  const checkedCount = targets.filter(f => isFolderChecked(f)).length
  const indeterminateCount = targets.filter(f => isFolderIndeterminate(f)).length
  // 有半选的文件夹也算 indeterminate
  if (indeterminateCount > 0) return true
  return checkedCount > 0 && checkedCount < targets.length
})

const totalSelectedCount = computed(() => checkedFsIds.value.size)

const selectedTotalSize = computed(() => {
  let total = 0
  for (const fsId of checkedFsIds.value) {
    const file = allKnownFiles.value.get(fsId)
    if (file) total += file.size
  }
  return total
})

// 当根目录 files 变化时，重新全选并重置导航
watch(() => props.files, (newFiles) => {
  pathStack.value = []
  dirFiles.value = []
  rootExtraFiles.value = []
  allKnownFiles.value = new Map()
  checkedFsIds.value = new Set()
  folderChildren.value = new Map()

  // 重置分页状态；数据源变化同样废弃旧的根目录分页所有权
  retirePagingRequest()
  navigationGeneration++
  rootPage.value = 1
  rootHasMore.value = newFiles.length >= PAGE_SIZE
  currentPage.value = 1
  hasMore.value = rootHasMore.value

  for (const f of newFiles) {
    allKnownFiles.value.set(f.fs_id, f)
  }

  if (pathMode.value) {
    // 目录内容重载：作废进行中的展开操作与导航，目录缓存整体失效
    cancelSelectionTransaction()
    navigating.value = false
    childrenCache.value = new Map()
    pathIndex.clear()
    refreshRootAliases(newFiles)
    recordListingPage('', newFiles, newFiles.length)
    emitSelectionReady()
    return
  }

  for (const f of newFiles) {
    // 仅选文件夹模式必须逐个确认：不默认勾选。
    if (props.folderOnly) continue
    checkedFsIds.value.add(f.fs_id)
  }
  emitSelection()
}, { immediate: true })

// 全选/取消全选当前目录
function handleSelectAllCurrent(val: boolean | string | number) {
  if (pathMode.value) {
    void handleSelectAllCurrentPath()
    return
  }
  const newSet = new Set(checkedFsIds.value)
  for (const file of currentFiles.value) {
    // 仅选文件夹模式只操作文件夹自身，不展开成子文件。
    if (props.folderOnly) {
      if (!file.is_dir) continue
      if (val) {
        newSet.add(file.fs_id)
      } else {
        newSet.delete(file.fs_id)
      }
      continue
    }
    // 对于进入过的文件夹，操作其子文件
    const children = file.is_dir ? folderChildren.value.get(file.fs_id) : null
    if (children && children.length > 0) {
      for (const childId of children) {
        if (val) {
          newSet.add(childId)
        } else {
          newSet.delete(childId)
        }
      }
      // 文件夹自身不加入
      newSet.delete(file.fs_id)
    } else {
      if (val) {
        newSet.add(file.fs_id)
      } else {
        newSet.delete(file.fs_id)
      }
    }
  }
  checkedFsIds.value = newSet
  emitSelection()
}

// 单个文件选择变化
function handleCheckChange(fsId: number, checked: boolean | string | number, file?: SharedFileInfo) {
  if (pathMode.value) {
    if (!file) return
    void handlePathCheckChange(checked === true, file)
    return
  }
  const newSet = new Set(checkedFsIds.value)

  if (props.folderOnly) {
    if (!file?.is_dir) return
    if (checked) {
      newSet.add(fsId)
    } else {
      newSet.delete(fsId)
    }
    checkedFsIds.value = newSet
    emitSelection()
    return
  }

  // 如果是进入过的文件夹，勾选/取消勾选要联动子文件
  if (file?.is_dir) {
    const children = folderChildren.value.get(fsId)
    if (children && children.length > 0) {
      // 进入过的文件夹：操作子文件，不操作文件夹自身
      for (const childId of children) {
        if (checked) {
          newSet.add(childId)
        } else {
          newSet.delete(childId)
        }
      }
      // 文件夹自身不加入（因为转存时只需要子文件的 fs_id）
      newSet.delete(fsId)
    } else {
      // 没进入过的文件夹：直接操作文件夹自身
      if (checked) {
        newSet.add(fsId)
      } else {
        newSet.delete(fsId)
      }
    }
  } else {
    if (checked) {
      newSet.add(fsId)
    } else {
      newSet.delete(fsId)
    }
  }

  checkedFsIds.value = newSet
  emitSelection()
}

// 点击文件项：文件夹进入，文件切换选择
function handleItemClick(file: SharedFileInfo) {
  if (file.is_dir && props.shareInfo) {
    navigateIntoDir(file)
  } else {
    if (props.folderOnly) return
    if (pathMode.value) {
      void handlePathCheckChange(!rowChecked(file), file)
      return
    }
    // 切换选择
    const newSet = new Set(checkedFsIds.value)
    if (newSet.has(file.fs_id)) {
      newSet.delete(file.fs_id)
    } else {
      newSet.add(file.fs_id)
    }
    checkedFsIds.value = newSet
    emitSelection()
  }
}

// 构建子目录的 dir 参数
//
// 个人版：根目录进入子目录时需要拼接 /sharelink{uk}-{shareid}/{文件夹名}，
//        子目录返回的 path 已经是 sharelink 格式，可以直接用。
// 企业版：接口只认列表里原样的 path，带上 sharelink 前缀会被判成
//        「目录不在分享范围内」（errno=13042）。
function buildShareDir(folder: SharedFileInfo): string {
  // 企业版不做任何拼接，直接用服务端给的 path
  if (props.shareInfo?.kind === 'apaas') {
    return folder.path
  }
  if (pathMode.value) {
    // 路径模式用逻辑 key 组合：根目录物理路径不能直接拼进 sharelink 命名空间
    if (props.shareInfo) {
      return `/sharelink${props.shareInfo.uk}-${props.shareInfo.shareid}/${pathKeyOf(folder)}`
    }
    return folder.path
  }
  // 如果 path 已经是 sharelink 格式，直接用
  if (folder.path.startsWith('/sharelink')) {
    return folder.path
  }
  // 从根目录进入子目录，需要拼接
  if (props.shareInfo) {
    return `/sharelink${props.shareInfo.uk}-${props.shareInfo.shareid}/${folder.name}`
  }
  // fallback
  return folder.path
}

// 进入子目录
async function navigateIntoDir(folder: SharedFileInfo) {
  if (!props.shareInfo) return

  const dir = buildShareDir(folder)

  // 导航会作废进行中的目录展开：先原子取消旧事务（已选路径保持不变、busy 释放），
  // 再作废其它并发导航与分页请求，只有最后一次导航可以落盘并结算就绪状态
  if (pathMode.value) cancelSelectionTransaction(false)
  retirePagingRequest()
  const nav = ++navigationGeneration

  navigating.value = true
  emitSelectionReady()
  try {
    const response = await previewShareDir({
      short_key: props.shareInfo.short_key,
      shareid: props.shareInfo.shareid,
      uk: props.shareInfo.uk,
      bdstoken: props.shareInfo.bdstoken,
      // 企业版分享必须回传 kind/token，否则后端按个人版接口去列目录会失败
      kind: props.shareInfo.kind,
      token: props.shareInfo.token,
      dir,
      page: 1,
      num: PAGE_SIZE,
    })
    if (nav !== navigationGeneration) return

    // 记录路径，重置分页
    const dirKey = pathKeyOf(folder)
    pathStack.value.push({ name: folder.name, dir, key: dirKey })
    dirFiles.value = response.files
    currentPage.value = 1
    hasMore.value = response.files.length >= PAGE_SIZE

    // 记录文件夹→子文件映射
    folderChildren.value.set(folder.fs_id, response.files.map(f => f.fs_id))

    // 判断进入前文件夹是否被选中
    const wasFolderChecked = checkedFsIds.value.has(folder.fs_id)

    // 注册新发现的文件
    // 如果文件夹之前是选中的 → 子文件默认全选（展开文件夹）
    // 如果文件夹之前未选中 → 子文件默认不选
    const newSet = new Set(checkedFsIds.value)
    for (const f of response.files) {
      allKnownFiles.value.set(f.fs_id, f)
      if (props.folderOnly) continue
      if (pathMode.value) continue
      if (wasFolderChecked) {
        newSet.add(f.fs_id)
      }
    }
    if (!props.folderOnly && !pathMode.value) {
      newSet.delete(folder.fs_id) // 移除父文件夹（用子文件替代）
    }
    checkedFsIds.value = newSet
    if (pathMode.value) {
      // 路径模式只记录目录内容，不改变已选路径；就绪状态在导航结束后统一结算
      recordListingPage(dirKey, response.files, response.files.length)
    } else {
      emitSelection()
    }
  } catch (error: any) {
    console.error('加载子目录失败:', error)
  } finally {
    if (nav === navigationGeneration) {
      navigating.value = false
      emitSelectionReady()
    }
  }
}

// 导航到根目录
function navigateToRoot() {
  if (pathMode.value) cancelSelectionTransaction(false)
  retirePagingRequest()
  navigationGeneration++
  navigating.value = false
  pathStack.value = []
  dirFiles.value = []
  // 回到根目录时保留 rootExtraFiles，并恢复根目录自己的分页位置
  currentPage.value = rootPage.value
  hasMore.value = rootHasMore.value
  emitSelectionReady()
}

// 导航到指定层级
function navigateToLevel(index: number) {
  if (index === pathStack.value.length - 1) return // 已在当前层

  // 截断到目标层级，然后重新加载该目录
  const target = pathStack.value[index]
  pathStack.value = pathStack.value.slice(0, index + 1)

  // 导航会作废进行中的目录展开与分页请求，并释放其 busy 所有权
  if (pathMode.value) cancelSelectionTransaction(false)
  retirePagingRequest()
  const nav = ++navigationGeneration

  // 重新加载该目录内容
  if (props.shareInfo) {
    navigating.value = true
    emitSelectionReady()
    previewShareDir({
      short_key: props.shareInfo.short_key,
      shareid: props.shareInfo.shareid,
      uk: props.shareInfo.uk,
      bdstoken: props.shareInfo.bdstoken,
      // 企业版分享必须回传 kind/token，否则后端按个人版接口去列目录会失败
      kind: props.shareInfo.kind,
      token: props.shareInfo.token,
      dir: target.dir,
      page: 1,
      num: PAGE_SIZE,
    }).then(response => {
      if (nav !== navigationGeneration) return
      dirFiles.value = response.files
      currentPage.value = 1
      hasMore.value = response.files.length >= PAGE_SIZE
      if (pathMode.value) {
        recordListingPage(target.key, response.files, response.files.length)
      }
      for (const f of response.files) {
        allKnownFiles.value.set(f.fs_id, f)
      }
    }).catch(error => {
      console.error('加载目录失败:', error)
    }).finally(() => {
      if (nav !== navigationGeneration) return
      navigating.value = false
      emitSelectionReady()
    })
  }
}

// 滚动加载更多
async function loadMore() {
  if (loadingMore.value || !hasMore.value) return

  // 在 await 前固定请求上下文；返回后不得再读取 pathStack 决定落盘目标
  const requestId = ++pagingRequestId
  const navEpoch = navigationGeneration
  const subdir = pathStack.value.length > 0
  const dirKey = subdir ? currentDirKey() : ''
  const dirRequest = subdir ? pathStack.value[pathStack.value.length - 1].dir : null
  const nextPage = (subdir ? currentPage.value : rootPage.value) + 1
  const isCurrentRequest = () =>
    requestId === activePagingRequest && navEpoch === navigationGeneration

  loadingMore.value = true
  activePagingRequest = requestId

  try {
    let moreFiles: SharedFileInfo[] = []

    if (subdir && props.shareInfo && dirRequest !== null) {
      // 子目录分页
      const response = await previewShareDir({
        short_key: props.shareInfo.short_key,
        shareid: props.shareInfo.shareid,
        uk: props.shareInfo.uk,
        bdstoken: props.shareInfo.bdstoken,
        // 企业版分享必须回传 kind/token，否则后端按个人版接口去列目录会失败
        kind: props.shareInfo.kind,
        token: props.shareInfo.token,
        dir: dirRequest,
        page: nextPage,
        num: PAGE_SIZE,
      })
      moreFiles = response.files
    } else if (!subdir && props.shareUrl) {
      // 根目录分页
      const response = await previewShareFiles({
        share_url: props.shareUrl,
        password: props.sharePassword || undefined,
        page: nextPage,
        num: PAGE_SIZE,
      })
      moreFiles = response.files
    } else {
      return
    }

    // 导航、数据源重置或更新的分页请求已接管：结果、页码、缓存、错误一律不落盘
    if (!isCurrentRequest()) return

    if (moreFiles.length > 0) {
      if (subdir) {
        const seen = new Set(dirFiles.value.map(f => f.fs_id))
        dirFiles.value = [...dirFiles.value, ...moreFiles.filter(f => !seen.has(f.fs_id))]
        currentPage.value = nextPage
        hasMore.value = moreFiles.length >= PAGE_SIZE
      } else {
        const seen = new Set(rootExtraFiles.value.map(f => f.fs_id))
        rootExtraFiles.value = [
          ...rootExtraFiles.value,
          ...moreFiles.filter(f => !seen.has(f.fs_id)),
        ]
        rootPage.value = nextPage
        rootHasMore.value = moreFiles.length >= PAGE_SIZE
        currentPage.value = rootPage.value
        hasMore.value = rootHasMore.value
      }
    } else if (subdir) {
      hasMore.value = false
    } else {
      rootHasMore.value = false
      hasMore.value = false
    }

    if (pathMode.value && !subdir) {
      // 根目录晚到分页可能补齐物理路径别名；只补映射，不重置已改动选择
      refreshRootAliases()
    }

    if (pathMode.value) {
      // 路径模式只记录目录内容，不改变已选路径；空尾页同样提交完整列表。
      // 整层事务已确认的完整缓存不得被后续部分滚动页降级。
      const cacheKey = subdir ? dirKey : ''
      const cached = childrenCache.value.get(cacheKey)
      if (!cached?.complete) {
        const list = subdir ? dirFiles.value : [...props.files, ...rootExtraFiles.value]
        recordListingPage(cacheKey, list, moreFiles.length)
      }
    }

    if (moreFiles.length > 0) {
      // 注册并默认选中
      const newSet = new Set(checkedFsIds.value)
      for (const f of moreFiles) {
        allKnownFiles.value.set(f.fs_id, f)
        // 翻页不自动勾选：未经点击的目录不得成为排除项。
        if (props.folderOnly) continue
        if (pathMode.value) continue
        if (!checkedFsIds.value.has(f.fs_id)) {
          newSet.add(f.fs_id)
        }
      }
      checkedFsIds.value = newSet
      emitSelection()
    }
  } catch (error: any) {
    if (!isCurrentRequest()) return
    console.error('加载更多失败:', error)
  } finally {
    // 只释放自己的加载态，迟到的 finally 不得清掉新请求的 busy
    if (activePagingRequest === requestId) {
      activePagingRequest = null
      loadingMore.value = false
    }
  }
}

// 滚动事件处理
function handleScroll(e: Event) {
  const target = e.target as HTMLElement
  if (!target) return
  // 距离底部 50px 时触发加载
  if (target.scrollHeight - target.scrollTop - target.clientHeight < 50) {
    loadMore()
  }
}

// 挂载/卸载滚动监听
let scrollListenerElement: HTMLElement | null = null

/**
 * 绑定当前滚动容器。
 *
 * 导航时 loading 分支会卸载并重建 el-scrollbar，旧的 wrap 元素不再接收滚动事件；
 * 每次更新后重新对准当前的 wrap，子目录分页才能继续触发 loadMore。
 */
function bindScrollListener() {
  const wrapEl = scrollbarRef.value?.wrapRef ?? null
  if (wrapEl === scrollListenerElement) return
  scrollListenerElement?.removeEventListener('scroll', handleScroll)
  scrollListenerElement = wrapEl
  scrollListenerElement?.addEventListener('scroll', handleScroll)
}

onMounted(() => {
  nextTick(() => bindScrollListener())
})

onUpdated(() => {
  bindScrollListener()
})

onBeforeUnmount(() => {
  // 组件卸载后，进行中的目录展开 / 导航 / 分页结果一律丢弃，并释放 busy 所有权
  cancelSelectionTransaction()
  retirePagingRequest()
  navigationGeneration++
  scrollListenerElement?.removeEventListener('scroll', handleScroll)
  scrollListenerElement = null
})

// 发送选中的 fs_id 列表
// 核心逻辑：
// - 仅选文件夹模式：只发**被显式勾选**的目录 id，不做「子项全选 → 折叠成
//   父目录」的归并（否则勾选一个子目录会被提升成未勾选的父目录，
//   把排除范围悄悄放大到整个父目录）
// - 没进入过的文件夹：在 checkedFsIds 中就发文件夹 fs_id
// - 进入过的文件夹，子文件全选 → 发文件夹 fs_id（不发子文件）
// - 进入过的文件夹，部分选中 → 不发文件夹 fs_id，只发选中的子项（递归同理）
// - 普通文件 → 在 checkedFsIds 中就发
function emitSelection() {
  if (pathMode.value) {
    emit('update:selectedPaths', shareSelectionPaths(selectionState.value))
    emitSelectionReady()
    return
  }

  const result: number[] = []

  if (props.folderOnly) {
    for (const fsId of checkedFsIds.value) {
      const file = allKnownFiles.value.get(fsId)
      if (file && !file.is_dir) continue
      result.push(fsId)
    }
    emit('update:selectedFsIds', result)
    const selectedFiles: SharedFileInfo[] = result
        .map(fsId => allKnownFiles.value.get(fsId))
        .filter((f): f is SharedFileInfo => f !== undefined)
    emit('update:selectedFiles', selectedFiles)
    return
  }

  // 收集所有"被进入过的文件夹的子文件 id"，用于后续判断是否需要折叠
  const coveredByFolder = new Set<number>()

  // 先处理进入过的文件夹：判断全选还是部分选中
  for (const [folderId, children] of folderChildren.value) {
    if (children.length === 0) continue
    const allSelected = children.every(id => checkedFsIds.value.has(id))
    if (allSelected) {
      // 全选 → 发文件夹 id，子文件不单独发
      result.push(folderId)
      for (const id of children) {
        coveredByFolder.add(id)
      }
    }
    // 部分选中 → 不发文件夹 id，子文件各自判断（下面统一处理）
  }

  // 再处理 checkedFsIds 中剩余的项
  for (const fsId of checkedFsIds.value) {
    if (coveredByFolder.has(fsId)) continue // 已被全选文件夹覆盖
    result.push(fsId)
  }

  emit('update:selectedFsIds', result)

  // 同时发送选中文件的完整信息（用于后端获取文件元信息）
  const selectedFiles: SharedFileInfo[] = result
      .map(fsId => allKnownFiles.value.get(fsId))
      .filter((f): f is SharedFileInfo => f !== undefined)
  emit('update:selectedFiles', selectedFiles)
}
</script>

<style scoped lang="scss">
.share-file-selector {
  width: 100%;
}

.loading-container {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 24px 0;
  color: var(--el-text-color-secondary);
}

.empty-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  text-align: center;
  padding: 24px 0;
  color: var(--el-text-color-secondary);
}

.selector-header {
  padding: 4px 0 8px;
  border-bottom: 1px solid var(--el-border-color-lighter);
  margin-bottom: 4px;
}

.header-top {
  margin-bottom: 6px;
}

.header-bottom {
  display: flex;
  align-items: center;
  gap: 12px;
}

.breadcrumb {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 2px;
  font-size: 13px;
}

.breadcrumb-sep {
  color: var(--el-text-color-placeholder);
  margin: 0 2px;
}

.select-info {
  font-size: 13px;
  color: var(--el-text-color-secondary);
}

.selection-status {
  margin-top: 6px;
  font-size: 13px;
  color: var(--el-text-color-secondary);
}

.selection-error {
  color: var(--el-color-danger);
}

.size-info {
  color: var(--el-text-color-regular);
}

.file-list {
  padding: 4px 0;
}

.file-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 4px;
  border-radius: 4px;
  cursor: pointer;
  transition: background-color 0.15s;

  &:hover {
    background-color: var(--el-fill-color-light);
  }
}

.file-icon {
  font-size: 18px;
  color: var(--el-text-color-secondary);
  flex-shrink: 0;

  &.folder-icon {
    color: var(--el-color-warning);
  }
}

.file-name {
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 14px;
}

.file-enter {
  flex-shrink: 0;
  color: var(--el-text-color-placeholder);
  font-size: 14px;
}

.file-size {
  flex-shrink: 0;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  min-width: 60px;
  text-align: right;
}

/* 移动端适配 */
.is-mobile {
  .file-item {
    padding: 12px 4px;
    gap: 10px;
  }

  .file-icon {
    font-size: 20px;
  }

  .file-name {
    font-size: 15px;
  }

  .file-size {
    font-size: 13px;
  }

  :deep(.el-checkbox__inner) {
    width: 18px;
    height: 18px;

    &::after {
      height: 9px;
      left: 5px;
    }
  }

  .selector-header {
    padding: 6px 0 10px;
  }
}

.load-more-tip {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 6px;
  padding: 12px 0;
  color: var(--el-text-color-secondary);
  font-size: 13px;
}
</style>
