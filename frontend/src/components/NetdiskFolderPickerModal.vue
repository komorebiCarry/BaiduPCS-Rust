<template>
  <el-dialog
      v-model="visible"
      :title="title"
      width="640px"
      :close-on-click-modal="false"
      append-to-body
      @close="handleClose"
  >
    <div class="folder-picker">
      <!-- 路径面包屑 -->
      <div class="breadcrumb">
        <el-breadcrumb separator="/">
          <el-breadcrumb-item @click="navigateTo('/')">
            <el-icon><HomeFilled /></el-icon>
            <span>根目录</span>
          </el-breadcrumb-item>
          <el-breadcrumb-item
              v-for="(part, index) in pathParts"
              :key="index"
              @click="navigateTo(getPathUpTo(index))"
          >{{ part }}</el-breadcrumb-item>
        </el-breadcrumb>
        <el-button
            size="small"
            :icon="Refresh"
            :loading="loading"
            text
            @click="reload"
        >刷新</el-button>
      </div>

      <!-- 当前路径展示 -->
      <div class="current-path">
        <span class="label">将选择到</span>
        <code class="value">{{ currentPath }}</code>
      </div>

      <!-- 文件夹列表 -->
      <div v-loading="loading" class="folder-list">
        <div v-if="!loading && folders.length === 0" class="empty">
          <el-icon><FolderOpened /></el-icon>
          <span>该目录下没有子文件夹</span>
        </div>
        <div
            v-for="folder in folders"
            :key="folder.fs_id"
            class="folder-item"
            :class="{ disabled: isFolderDisabled(folder) }"
            @click="handleFolderClick(folder)"
        >
          <el-icon class="icon"><Folder /></el-icon>
          <span class="name">{{ folder.server_filename }}</span>
          <el-icon class="arrow"><ArrowRight /></el-icon>
        </div>
        <el-button
            v-if="hasMore && !loading"
            text
            class="load-more"
            @click="loadMore"
        >加载更多</el-button>
      </div>

      <!-- 新建文件夹 -->
      <div class="create-folder">
        <el-input
            v-if="creatingFolder"
            v-model="newFolderName"
            placeholder="输入新文件夹名"
            :maxlength="100"
            size="small"
            @keydown.enter="confirmCreateFolder"
            @keydown.esc="cancelCreateFolder"
        >
          <template #append>
            <el-button :icon="Check" @click="confirmCreateFolder" />
            <el-button :icon="Close" @click="cancelCreateFolder" />
          </template>
        </el-input>
        <el-button
            v-else
            size="small"
            :icon="FolderAdd"
            text
            @click="creatingFolder = true"
        >新建文件夹</el-button>
      </div>
    </div>

    <template #footer>
      <el-button @click="handleClose">取消</el-button>
      <el-button
          type="primary"
          :disabled="!canConfirm"
          @click="handleConfirm"
      >选择此文件夹</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  HomeFilled,
  Folder,
  FolderOpened,
  FolderAdd,
  Refresh,
  ArrowRight,
  Check,
  Close,
} from '@element-plus/icons-vue'
import {
  getFileList,
  createFolder,
  joinPath,
  normalizePath,
  validateFilename,
  type FileItem,
} from '@/api/file'

interface Props {
  /** v-model 控制显示 */
  modelValue: boolean
  /** 标题 */
  title?: string
  /** 起始路径，默认为根 */
  initialPath?: string
  /**
   * 子树禁选：自身及其所有子目录都不允许选择。
   * 例如 move 操作时禁止把目录移到自身或自身子目录。
   */
  blockedPaths?: string[]
  /**
   * 精确禁选：仅自身禁选，不影响子目录。
   * 例如 move 操作时禁止"目标父目录 == 源父目录"，但目标的子目录是合法的。
   */
  blockedExactPaths?: string[]
}

const props = withDefaults(defineProps<Props>(), {
  title: '选择目标文件夹',
  initialPath: '/',
  blockedPaths: () => [],
  blockedExactPaths: () => [],
})

const emit = defineEmits<{
  (e: 'update:modelValue', v: boolean): void
  (e: 'confirm', path: string): void
}>()

const visible = computed({
  get: () => props.modelValue,
  set: (v: boolean) => emit('update:modelValue', v),
})

const currentPath = ref<string>('/')
const folders = ref<FileItem[]>([])
const loading = ref(false)
const page = ref(1)
const hasMore = ref(false)
const PAGE_SIZE = 200

const creatingFolder = ref(false)
const newFolderName = ref('')

// 进入面板时初始化路径
watch(
    () => props.modelValue,
    (v) => {
      if (v) {
        currentPath.value = normalizePath(props.initialPath || '/')
        creatingFolder.value = false
        newFolderName.value = ''
        loadFolders()
      }
    },
    { immediate: true }
)

const pathParts = computed(() => {
  if (currentPath.value === '/') return [] as string[]
  return currentPath.value.split('/').filter(Boolean)
})

function getPathUpTo(index: number): string {
  return '/' + pathParts.value.slice(0, index + 1).join('/')
}

const canConfirm = computed(() => {
  // 当前路径可否作为目标选中：exact + subtree 任一命中都不可确认
  return !isBlockedForSelection(currentPath.value)
})

/**
 * 是否不可作为目标选中（不可确认）
 *   - 精确禁选（blockedExactPaths）命中自身 → true
 *   - 子树禁选（blockedPaths）命中自身或任一祖先 → true
 */
function isBlockedForSelection(path: string): boolean {
  const target = normalizePath(path)
  if (props.blockedExactPaths.some((b) => normalizePath(b) === target)) {
    return true
  }
  return props.blockedPaths.some((blocked) => {
    const b = normalizePath(blocked)
    return target === b || target.startsWith(b + '/')
  })
}

/**
 * 是否不可进入（文件夹项 disabled）
 *   - 仅 subtree 命中时禁止进入（进去也全部不可选）
 *   - exact 命中但非 subtree：允许进入，因为子目录仍是合法目标
 *     例：move /a/x.txt 时，/a 是 exact 禁选，但用户需要进入 /a 才能选 /a/sub
 */
function isBlockedForEntry(path: string): boolean {
  const target = normalizePath(path)
  return props.blockedPaths.some((blocked) => {
    const b = normalizePath(blocked)
    return target === b || target.startsWith(b + '/')
  })
}

function isFolderDisabled(folder: FileItem): boolean {
  return isBlockedForEntry(folder.path)
}

async function loadFolders(append = false) {
  loading.value = true
  try {
    if (!append) {
      page.value = 1
      folders.value = []
    }
    const data = await getFileList(currentPath.value, page.value, PAGE_SIZE)
    const onlyFolders = data.list.filter((f) => f.isdir === 1)
    folders.value = append ? folders.value.concat(onlyFolders) : onlyFolders
    hasMore.value = data.has_more
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : '加载文件夹失败'
    ElMessage.error(msg)
  } finally {
    loading.value = false
  }
}

async function loadMore() {
  if (loading.value || !hasMore.value) return
  page.value += 1
  await loadFolders(true)
}

function reload() {
  loadFolders(false)
}

function navigateTo(path: string) {
  if (loading.value) return
  currentPath.value = normalizePath(path)
  loadFolders(false)
}

function handleFolderClick(folder: FileItem) {
  if (isFolderDisabled(folder)) {
    ElMessage.warning('该路径不允许选择（与源路径冲突）')
    return
  }
  navigateTo(folder.path)
}

async function confirmCreateFolder() {
  const name = newFolderName.value.trim()
  const err = validateFilename(name)
  if (err) {
    ElMessage.error(err)
    return
  }
  const target = joinPath(currentPath.value, name)
  try {
    await createFolder(target)
    ElMessage.success('创建成功')
    creatingFolder.value = false
    newFolderName.value = ''
    await loadFolders(false)
  } catch (e: unknown) {
    const msg = e instanceof Error ? e.message : '创建文件夹失败'
    ElMessage.error(msg)
  }
}

function cancelCreateFolder() {
  creatingFolder.value = false
  newFolderName.value = ''
}

async function handleConfirm() {
  if (!canConfirm.value) {
    ElMessage.warning('请选择一个有效的目标路径')
    return
  }
  emit('confirm', currentPath.value)
  emit('update:modelValue', false)
}

function handleClose() {
  emit('update:modelValue', false)
}

// 暴露：allow caller to override
defineExpose({ reload })
</script>

<style scoped>
.folder-picker {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.breadcrumb {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 8px 12px;
  background: var(--el-fill-color-light);
  border-radius: 6px;
  flex-wrap: wrap;
  gap: 8px;
}

.breadcrumb :deep(.el-breadcrumb__item) {
  cursor: pointer;
}

.current-path {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 13px;
}

.current-path .label {
  color: var(--el-text-color-secondary);
}

.current-path .value {
  background: var(--el-fill-color-darker);
  padding: 2px 8px;
  border-radius: 4px;
  font-family: monospace;
  word-break: break-all;
}

.folder-list {
  min-height: 240px;
  max-height: 360px;
  overflow-y: auto;
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;
  padding: 4px;
}

.folder-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  border-radius: 4px;
  cursor: pointer;
  transition: background 0.15s;
}

.folder-item:hover {
  background: var(--el-fill-color);
}

.folder-item.disabled {
  opacity: 0.4;
  cursor: not-allowed;
}

.folder-item .icon {
  color: #e6a23c;
  font-size: 18px;
}

.folder-item .name {
  flex: 1;
  word-break: break-all;
}

.folder-item .arrow {
  color: var(--el-text-color-placeholder);
}

.empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 36px;
  color: var(--el-text-color-secondary);
  gap: 8px;
}

.empty .el-icon {
  font-size: 36px;
}

.load-more {
  width: 100%;
  margin-top: 6px;
}

.create-folder {
  display: flex;
  align-items: center;
  gap: 8px;
}
</style>
