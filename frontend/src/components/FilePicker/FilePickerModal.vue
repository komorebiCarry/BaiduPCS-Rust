<template>
  <el-dialog
    v-model="visible"
    :title="title"
    width="800px"
    :close-on-click-modal="false"
    @open="handleOpen"
    @close="handleClose"
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
        :select-type="selectType"
        @select="handleSelect"
        @open="handleOpen2"
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
        <span class="selected-info">
          <template v-if="store.selection">
            已选择: {{ store.selection.name }}
          </template>
          <template v-else>
            未选择
          </template>
        </span>
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
      </div>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, watch } from 'vue'
import { useFilePickerStore } from '@/stores/filepicker'
import type { FileEntry } from '@/api/filesystem'
import NavigatorBar from './NavigatorBar.vue'
import FileList from './FileList.vue'
import EmptyState from './EmptyState.vue'
import ErrorState from './ErrorState.vue'

const props = withDefaults(defineProps<{
  modelValue: boolean
  selectType?: 'file' | 'directory' | 'both'
  title?: string
  confirmText?: string
}>(), {
  selectType: 'both',
  title: '选择文件',
  confirmText: '确定',
})

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'select': [entry: FileEntry]
}>()

const store = useFilePickerStore()

// 对话框可见性
const visible = computed({
  get: () => props.modelValue,
  set: (val) => emit('update:modelValue', val),
})

// 是否可确认
const canConfirm = computed(() => {
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
  store.loadDirectory('')
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
  if (store.selection && canConfirm.value) {
    emit('select', store.selection)
    visible.value = false
  }
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

.selected-info {
  color: var(--el-text-color-secondary);
  font-size: 14px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  max-width: 400px;
}

.actions {
  display: flex;
  gap: 8px;
}
</style>