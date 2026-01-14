<template>
  <el-dialog
      v-model="visible"
      :title="title"
      :width="isMobile ? '95%' : '800px'"
      :close-on-click-modal="false"
      @open="handleOpen"
      @close="handleClose"
      :class="{ 'is-mobile': isMobile }"
  >
    <!-- 导航栏 -->
    <NavigatorBar
        :current-path="store.currentPath"
        :can-go-back="store.canGoBack"
        :can-go-forward="store.canGoForward"
        :can-go-up="!store.isRoot"
        @navigate="handleNavigate"
        @back="store.goBack"
        @forward="store.goForward"
        @up="store.goToParent"
        @refresh="store.refresh"
    />

    <!-- 内容区 -->
    <div class="content-area" v-loading="store.loading">
      <ErrorState
          v-if="store.error"
          :message="store.error"
          @retry="store.refresh"
      />
      <EmptyState
          v-else-if="!store.loading && store.entries.length === 0"
      />
      <FileList
          v-else
          :entries="store.entries"
          :selection="store.selection"
          :multi-selection="store.multiSelection"
          :select-type="effectiveSelectType"
          :multiple="isMultiSelectMode"
          @select="handleSelect"
          @open="handleOpen2"
          @toggle-select="handleToggleSelect"
          @select-all="handleSelectAll"
          @clear-selection="handleClearSelection"
      />
    </div>

    <!-- 分页加载更多 -->
    <div v-if="store.hasMore" class="load-more">
      <el-button
          text
          :loading="store.loading"
          @click="store.loadMore"
      >
        加载更多 ({{ store.entries.length }}/{{ store.total }})
      </el-button>
    </div>

    <!-- 底部操作栏 -->
    <template #footer>
      <div class="footer-bar">
        <!-- 上传模式 -->
        <template v-if="mode === 'upload'">
          <div class="upload-info">
            <span class="selected-info">
              <template v-if="isMultiSelectMode && store.multiSelection.length > 0">
                已选择: {{ store.multiSelection.length }} 个文件/文件夹
              </template>
              <template v-else-if="store.selection">
                已选择: {{ store.selection.name }}
              </template>
              <template v-else>
                未选择
              </template>
            </span>
            <div v-if="showEncryption" class="encrypt-switch">
              <el-icon><Lock /></el-icon>
              <span>加密上传</span>
              <el-switch v-model="encryptEnabled" size="small" />
            </div>
          </div>
          <div class="actions">
            <el-button @click="handleClose">取消</el-button>
            <el-button
                type="primary"
                :disabled="!canConfirmUpload"
                @click="handleConfirm"
            >
              {{ confirmText }}{{ isMultiSelectMode && store.multiSelection.length > 1 ? ` (${store.multiSelection.length})` : '' }}
            </el-button>
          </div>
        </template>

        <!-- 纯目录选择模式 -->
        <template v-else-if="mode === 'select-directory'">
          <div class="download-info">
            <div class="download-path">
              <span class="label">已选择:</span>
              <span class="path" :title="currentDownloadPath">{{ currentDownloadPath || '请选择目录' }}</span>
            </div>
          </div>
          <div class="actions">
            <el-button @click="handleClose">取消</el-button>
            <el-button
                type="primary"
                :disabled="!canConfirm"
                @click="handleConfirm"
            >
              {{ confirmText }}
            </el-button>
          </div>
        </template>

        <!-- 下载模式 -->
        <template v-else>
          <div class="download-info">
            <div class="download-path">
              <span class="label">下载到:</span>
              <span class="path" :title="currentDownloadPath">{{ currentDownloadPath || '请选择目录' }}</span>
            </div>
            <el-checkbox v-model="setAsDefault" class="set-default-checkbox">
              设为默认下载目录
            </el-checkbox>
          </div>
          <div class="actions">
            <el-button @click="handleClose">取消</el-button>
            <el-button
                v-if="showUseDefaultButton"
                @click="handleUseDefault"
            >
              默认路径下载
            </el-button>
            <el-button
                type="primary"
                :disabled="!canConfirm"
                @click="handleConfirm"
            >
              下载
            </el-button>
          </div>
        </template>
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, watch, ref } from 'vue'
import { useFilePickerStore } from '@/stores/filepicker'
import { useIsMobile } from '@/utils/responsive'
import type { FileEntry } from '@/api/filesystem'
import NavigatorBar from './NavigatorBar.vue'
import FileList from './FileList.vue'
import EmptyState from './EmptyState.vue'
import ErrorState from './ErrorState.vue'
import { Lock } from '@element-plus/icons-vue'

// 响应式检测
const isMobile = useIsMobile()

const props = withDefaults(defineProps<{
  modelValue: boolean
  selectType?: 'file' | 'directory' | 'both'
  title?: string
  confirmText?: string
  mode?: 'upload' | 'download' | 'select-directory'
  initialPath?: string
  defaultDownloadDir?: string
  multiple?: boolean  // 是否支持多选（上传模式默认 true）
  showEncryption?: boolean  // 是否显示加密选项（上传模式）
}>(), {
  selectType: 'both',
  title: '选择文件',
  confirmText: '确定',
  mode: 'upload',
  multiple: true,
  showEncryption: false,
})

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'select': [entry: FileEntry, encrypt: boolean]
  'select-multiple': [entries: FileEntry[], encrypt: boolean]  // 多选确认事件
  'confirm-download': [payload: { path: string, setAsDefault: boolean }]
  'confirm': [path: string]  // 纯目录选择模式
  'use-default': []
}>()

const store = useFilePickerStore()

// 下载模式状态
const setAsDefault = ref(false)

// 加密状态（上传模式）
const encryptEnabled = ref(false)

// 是否启用多选模式（上传模式 + multiple 为 true）
const isMultiSelectMode = computed(() => {
  return props.mode === 'upload' && props.multiple
})

// 是否为目录选择模式（下载或纯目录选择）
const isDirectoryMode = computed(() => {
  return props.mode === 'download' || props.mode === 'select-directory'
})

// 下载模式下实际使用的 selectType（强制为 directory）
const effectiveSelectType = computed(() => {
  return isDirectoryMode.value ? 'directory' : props.selectType
})

// 下载模式下的当前选中路径（用于底部显示）
const currentDownloadPath = computed(() => {
  // 优先使用选中的目录
  if (store.selection && store.selection.entryType === 'directory') {
    return store.selection.path
  }
  // 否则使用当前浏览的目录
  return store.currentPath
})

// 是否显示默认路径下载按钮
const showUseDefaultButton = computed(() => {
  return props.mode === 'download' && props.defaultDownloadDir
})

// 对话框可见性
const visible = computed({
  get: () => props.modelValue,
  set: (val) => emit('update:modelValue', val),
})

// 是否可确认（下载/目录选择模式）
const canConfirm = computed(() => {
  if (isDirectoryMode.value) {
    // 目录选择模式：只要有当前路径就可以确认
    return !!currentDownloadPath.value
  }
  return false
})

// 是否可确认上传（上传模式）
const canConfirmUpload = computed(() => {
  if (isMultiSelectMode.value) {
    // 多选模式：有选中的文件/文件夹
    return store.multiSelection.length > 0
  }

  // 单选模式：原有逻辑
  if (!store.selection) return false

  if (props.selectType === 'file' && store.selection.entryType !== 'file') {
    return false
  }
  if (props.selectType === 'directory' && store.selection.entryType !== 'directory') {
    return false
  }

  return true
})

// 对话框打开
function handleOpen() {
  store.reset()
  setAsDefault.value = false
  encryptEnabled.value = false

  // 根据模式确定初始路径
  if (props.mode === 'download') {
    // 下载模式：优先使用 initialPath，其次 defaultDownloadDir
    const initialPath = props.initialPath || props.defaultDownloadDir || ''
    if (initialPath) {
      store.jumpToPath(initialPath)
    } else {
      store.loadDirectory('')
    }
  } else {
    // 上传模式：使用 initialPath 或空路径
    if (props.initialPath) {
      store.jumpToPath(props.initialPath)
    } else {
      store.loadDirectory('')
    }
  }
}

// 对话框关闭
function handleClose() {
  visible.value = false
}

// 导航到路径
function handleNavigate(path: string) {
  // 空路径（Windows 的"计算机"根目录）直接使用 navigateTo，会调用 getRoots() 获取驱动器列表
  // 避免调用 gotoPath 接口导致路径解析问题
  if (!path) {
    store.navigateTo('')
  } else {
    store.jumpToPath(path)
  }
}

// 选择条目（单击）
function handleSelect(entry: FileEntry) {
  // 单击总是选中条目（用于视觉反馈）
  // 只是在确认时检查类型是否匹配
  store.selectEntry(entry)
}

// 双击打开
function handleOpen2(entry: FileEntry) {
  if (entry.entryType === 'directory') {
    // 双击目录 → 进入目录
    store.openEntry(entry)
  } else if (props.selectType !== 'directory') {
    // 双击文件 → 选中并确认（如果允许选择文件）
    store.selectEntry(entry)
    handleConfirm()
  }
}

// 确认选择
function handleConfirm() {
  if (props.mode === 'download') {
    // 下载模式：发射 confirm-download 事件
    const downloadPath = currentDownloadPath.value
    if (downloadPath) {
      emit('confirm-download', { path: downloadPath, setAsDefault: setAsDefault.value })
      visible.value = false
    }
  } else if (props.mode === 'select-directory') {
    // 纯目录选择模式：发射 confirm 事件
    const selectedPath = currentDownloadPath.value
    if (selectedPath) {
      emit('confirm', selectedPath)
      visible.value = false
    }
  } else {
    // 上传模式
    if (isMultiSelectMode.value && store.multiSelection.length > 0) {
      // 多选模式：发射 select-multiple 事件
      emit('select-multiple', [...store.multiSelection], encryptEnabled.value)
      visible.value = false
    } else if (store.selection && canConfirmUpload.value) {
      // 单选模式：原有逻辑
      emit('select', store.selection, encryptEnabled.value)
      visible.value = false
    }
  }
}

// 使用默认路径下载
function handleUseDefault() {
  emit('use-default')
  visible.value = false
}

// 多选：切换选中
function handleToggleSelect(entry: FileEntry) {
  store.toggleMultiSelect(entry)
}

// 多选：全选
function handleSelectAll() {
  store.selectAll(effectiveSelectType.value)
}

// 多选：清除选择
function handleClearSelection() {
  store.clearMultiSelection()
}

// 监听 selectType 变化，清除不合适的选择
watch(() => props.selectType, () => {
  if (store.selection) {
    if (props.selectType === 'file' && store.selection.entryType !== 'file') {
      store.selectEntry(null)
    }
    if (props.selectType === 'directory' && store.selection.entryType !== 'directory') {
      store.selectEntry(null)
    }
  }
})
</script>

<style scoped>
.content-area {
  height: 400px;
  overflow-y: auto;
  border: 1px solid var(--el-border-color);
  border-radius: 4px;
  margin-top: 12px;
}

/* 移动端内容区域自适应高度 */
.is-mobile .content-area {
  height: 50vh;
  max-height: 60vh;
}

.load-more {
  text-align: center;
  padding: 8px;
}

.footer-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;
}

/* 上传模式信息区 */
.upload-info {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-width: 450px;
}

.selected-info {
  color: var(--el-text-color-secondary);
  font-size: 14px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 400px;
}

.encrypt-switch {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: var(--el-text-color-regular);
}

.actions {
  display: flex;
  gap: 8px;
}

/* 下载模式样式 */
.download-info {
  display: flex;
  flex-direction: column;
  gap: 8px;
  max-width: 450px;
}

.download-path {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 14px;
}

.download-path .label {
  color: var(--el-text-color-secondary);
  flex-shrink: 0;
}

.download-path .path {
  color: var(--el-text-color-primary);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.set-default-checkbox {
  font-size: 13px;
}

/* =====================
   移动端样式适配
   ===================== */
@media (max-width: 767px) {
  .footer-bar {
    flex-direction: column;
    gap: 12px;
    align-items: stretch;
  }

  .download-info {
    max-width: 100%;
  }

  .actions {
    width: 100%;
    flex-direction: column;
    gap: 8px; /* 确保按钮间距一致 */
    display: flex; /* 确保是 flex 容器 */
    align-items: stretch; /* 按钮拉伸到全宽 */

    .el-button {
      width: 100%;
      margin: 0; /* 移除可能的 margin */
      flex-shrink: 0; /* 防止按钮被压缩 */
    }
  }

  .selected-info {
    max-width: 100%;
    text-align: center;
  }

  .download-path {
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }

  .download-path .path {
    word-break: break-all;
    white-space: normal;
  }
}
</style>