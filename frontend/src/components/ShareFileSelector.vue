<template>
  <div class="share-file-selector" :class="{ 'is-mobile': isMobile }">
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
            :model-value="isAllCurrentSelected"
            :indeterminate="isCurrentIndeterminate"
            @change="handleSelectAllCurrent"
          >
            全选
          </el-checkbox>
          <span class="select-info">
            已选 {{ totalSelectedCount }} 个文件
            <span v-if="selectedTotalSize > 0" class="size-info">
              ({{ formatFileSize(selectedTotalSize) }})
            </span>
          </span>
        </div>
      </div>

      <!-- 文件列表 -->
      <el-scrollbar ref="scrollbarRef" :max-height="isMobile ? '250px' : '300px'">
        <div class="file-list" ref="fileListRef">
          <div
            v-for="file in currentFiles"
            :key="file.fs_id"
            class="file-item"
            @click="handleItemClick(file)"
          >
            <el-checkbox
              :model-value="isFolderChecked(file)"
              :indeterminate="isFolderIndeterminate(file)"
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
import { ref, computed, watch, onMounted, onBeforeUnmount, nextTick } from 'vue'
import { Loading, Folder, Document, ArrowRight } from '@element-plus/icons-vue'
import { useIsMobile } from '@/utils/responsive'
import { formatFileSize } from '@/api/utils'
import { previewShareDir, previewShareFiles, type SharedFileInfo, type PreviewShareInfo, type PreviewShareRequest } from '@/api/transfer'

const isMobile = useIsMobile()

interface PathEntry {
  name: string
  dir: string
}

const props = defineProps<{
  files: SharedFileInfo[]
  loading: boolean
  shareInfo?: PreviewShareInfo | null
  /** 首次预览的分享链接（用于根目录分页加载） */
  shareUrl?: string
  /** 首次预览的密码 */
  sharePassword?: string
}>()

const emit = defineEmits<{
  'update:selectedFsIds': [fsIds: number[]]
  'update:selectedFiles': [files: SharedFileInfo[]]
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

// 文件夹 checkbox 选中状态：
// - 未进入过的文件夹：直接看 checkedFsIds
// - 进入过的文件夹：看子文件是否全选
function isFolderChecked(file: SharedFileInfo): boolean {
  if (!file.is_dir) return checkedFsIds.value.has(file.fs_id)
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
  const children = folderChildren.value.get(file.fs_id)
  if (!children || children.length === 0) return false
  const selectedCount = children.filter(id => checkedFsIds.value.has(id)).length
  return selectedCount > 0 && selectedCount < children.length
}

// 当前目录的全选状态（考虑文件夹的展开状态）
const isAllCurrentSelected = computed(() => {
  if (currentFiles.value.length === 0) return false
  return currentFiles.value.every(f => isFolderChecked(f))
})

const isCurrentIndeterminate = computed(() => {
  if (currentFiles.value.length === 0) return false
  const checkedCount = currentFiles.value.filter(f => isFolderChecked(f)).length
  const indeterminateCount = currentFiles.value.filter(f => isFolderIndeterminate(f)).length
  // 有半选的文件夹也算 indeterminate
  if (indeterminateCount > 0) return true
  return checkedCount > 0 && checkedCount < currentFiles.value.length
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

  // 重置分页状态
  currentPage.value = 1
  hasMore.value = newFiles.length >= PAGE_SIZE

  for (const f of newFiles) {
    allKnownFiles.value.set(f.fs_id, f)
    checkedFsIds.value.add(f.fs_id)
  }
  emitSelection()
}, { immediate: true })

// 全选/取消全选当前目录
function handleSelectAllCurrent(val: boolean | string | number) {
  const newSet = new Set(checkedFsIds.value)
  for (const file of currentFiles.value) {
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
  const newSet = new Set(checkedFsIds.value)
  
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
// 根目录进入子目录时需要拼接 /sharelink{uk}-{shareid}/{文件夹名}
// 子目录返回的 path 已经是 sharelink 格式，可以直接用
function buildShareDir(folder: SharedFileInfo): string {
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

  navigating.value = true
  try {
    const response = await previewShareDir({
      short_key: props.shareInfo.short_key,
      shareid: props.shareInfo.shareid,
      uk: props.shareInfo.uk,
      bdstoken: props.shareInfo.bdstoken,
      dir,
      page: 1,
      num: PAGE_SIZE,
    })

    // 记录路径，重置分页
    pathStack.value.push({ name: folder.name, dir })
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
    newSet.delete(folder.fs_id) // 移除父文件夹（用子文件替代）
    for (const f of response.files) {
      allKnownFiles.value.set(f.fs_id, f)
      if (wasFolderChecked) {
        newSet.add(f.fs_id)
      }
    }
    checkedFsIds.value = newSet
    emitSelection()
  } catch (error: any) {
    console.error('加载子目录失败:', error)
  } finally {
    navigating.value = false
  }
}

// 导航到根目录
function navigateToRoot() {
  pathStack.value = []
  dirFiles.value = []
  // 回到根目录时保留 rootExtraFiles 和分页状态，不重新加载
}

// 导航到指定层级
function navigateToLevel(index: number) {
  if (index === pathStack.value.length - 1) return // 已在当前层

  // 截断到目标层级，然后重新加载该目录
  const target = pathStack.value[index]
  pathStack.value = pathStack.value.slice(0, index + 1)

  // 重新加载该目录内容
  if (props.shareInfo) {
    navigating.value = true
    previewShareDir({
      short_key: props.shareInfo.short_key,
      shareid: props.shareInfo.shareid,
      uk: props.shareInfo.uk,
      bdstoken: props.shareInfo.bdstoken,
      dir: target.dir,
      page: 1,
      num: PAGE_SIZE,
    }).then(response => {
      dirFiles.value = response.files
      currentPage.value = 1
      hasMore.value = response.files.length >= PAGE_SIZE
      for (const f of response.files) {
        allKnownFiles.value.set(f.fs_id, f)
      }
    }).catch(error => {
      console.error('加载目录失败:', error)
    }).finally(() => {
      navigating.value = false
    })
  }
}

// 滚动加载更多
async function loadMore() {
  if (loadingMore.value || !hasMore.value) return
  loadingMore.value = true

  const nextPage = currentPage.value + 1

  try {
    let moreFiles: SharedFileInfo[] = []

    if (pathStack.value.length > 0 && props.shareInfo) {
      // 子目录分页
      const currentDir = pathStack.value[pathStack.value.length - 1].dir
      const response = await previewShareDir({
        short_key: props.shareInfo.short_key,
        shareid: props.shareInfo.shareid,
        uk: props.shareInfo.uk,
        bdstoken: props.shareInfo.bdstoken,
        dir: currentDir,
        page: nextPage,
        num: PAGE_SIZE,
      })
      moreFiles = response.files
    } else if (props.shareUrl) {
      // 根目录分页
      const response = await previewShareFiles({
        share_url: props.shareUrl,
        password: props.sharePassword || undefined,
        page: nextPage,
        num: PAGE_SIZE,
      })
      moreFiles = response.files
    }

    if (moreFiles.length > 0) {
      currentPage.value = nextPage
      hasMore.value = moreFiles.length >= PAGE_SIZE

      // 追加到对应列表
      if (pathStack.value.length > 0) {
        dirFiles.value = [...dirFiles.value, ...moreFiles]
      } else {
        // 根目录分页：追加到 rootExtraFiles
        rootExtraFiles.value = [...rootExtraFiles.value, ...moreFiles]
      }
      // 注册并默认选中
      const newSet = new Set(checkedFsIds.value)
      for (const f of moreFiles) {
        allKnownFiles.value.set(f.fs_id, f)
        if (!checkedFsIds.value.has(f.fs_id)) {
          newSet.add(f.fs_id)
        }
      }
      checkedFsIds.value = newSet
      emitSelection()
    } else {
      hasMore.value = false
    }
  } catch (error: any) {
    console.error('加载更多失败:', error)
  } finally {
    loadingMore.value = false
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
onMounted(() => {
  nextTick(() => {
    const wrapEl = scrollbarRef.value?.wrapRef
    if (wrapEl) {
      wrapEl.addEventListener('scroll', handleScroll)
    }
  })
})

onBeforeUnmount(() => {
  const wrapEl = scrollbarRef.value?.wrapRef
  if (wrapEl) {
    wrapEl.removeEventListener('scroll', handleScroll)
  }
})

// 发送选中的 fs_id 列表
// 核心逻辑：
// - 没进入过的文件夹：在 checkedFsIds 中就发文件夹 fs_id
// - 进入过的文件夹，子文件全选 → 发文件夹 fs_id（不发子文件）
// - 进入过的文件夹，部分选中 → 不发文件夹 fs_id，只发选中的子项（递归同理）
// - 普通文件 → 在 checkedFsIds 中就发
function emitSelection() {
  const result: number[] = []
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
