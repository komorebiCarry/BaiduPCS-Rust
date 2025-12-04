<template>
  <div class="netdisk-path-selector">
    <!-- 当前路径显示 -->
    <div class="path-display" @click="openDialog">
      <el-icon class="folder-icon"><Folder /></el-icon>
      <span class="path-text" :title="displayPath">{{ displayPath || '选择目录' }}</span>
      <el-icon class="arrow-icon"><ArrowDown /></el-icon>
    </div>

    <!-- 目录选择对话框 -->
    <el-dialog
      v-model="dialogVisible"
      title="选择保存位置"
      width="600px"
      :close-on-click-modal="false"
      @open="handleDialogOpen"
    >
      <!-- 面包屑导航 -->
      <div class="breadcrumb-nav">
        <el-breadcrumb separator="/">
          <el-breadcrumb-item @click="navigateTo('/')">
            <el-icon><HomeFilled /></el-icon>
            根目录
          </el-breadcrumb-item>
          <el-breadcrumb-item
            v-for="(part, index) in pathParts"
            :key="index"
            @click="navigateTo(getPathUpTo(index))"
          >
            {{ part }}
          </el-breadcrumb-item>
        </el-breadcrumb>
      </div>

      <!-- 目录列表 -->
      <div class="folder-list" v-loading="loading && folders.length === 0">
        <el-scrollbar height="300px" ref="scrollbarRef" @scroll="handleScroll">
          <!-- 空状态 -->
          <el-empty v-if="!loading && folders.length === 0" description="当前目录为空" />

          <!-- 目录列表 -->
          <div
            v-for="folder in folders"
            :key="folder.fs_id"
            class="folder-item"
            :class="{ selected: selectedFsId === folder.fs_id }"
            @click="selectFolder(folder)"
            @dblclick="enterFolder(folder)"
          >
            <el-icon class="folder-icon"><Folder /></el-icon>
            <span class="folder-name">{{ folder.server_filename }}</span>
          </div>

          <!-- 加载更多提示 -->
          <div v-if="loadingMore" class="loading-more">
            <el-icon class="is-loading"><Loading /></el-icon>
            <span>加载中...</span>
          </div>
          <div v-else-if="!hasMore && folders.length > 0" class="no-more">
            没有更多了
          </div>
        </el-scrollbar>
      </div>

      <!-- 新建文件夹 -->
      <div class="create-folder">
        <el-input
          v-model="newFolderName"
          placeholder="新建文件夹"
          size="small"
          @keyup.enter="createNewFolder"
        >
          <template #append>
            <el-button @click="createNewFolder" :loading="creating">
              <el-icon><Plus /></el-icon>
            </el-button>
          </template>
        </el-input>
      </div>

      <template #footer>
        <div class="dialog-footer">
          <span class="selected-path">
            保存到: {{ selectedPath || currentPath }}
          </span>
          <div class="actions">
            <el-button @click="dialogVisible = false">取消</el-button>
            <el-button type="primary" @click="confirmSelection">确定</el-button>
          </div>
        </div>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { ElMessage } from 'element-plus'
import { Folder, ArrowDown, HomeFilled, Plus, Loading } from '@element-plus/icons-vue'
import { getFileList, createFolder, type FileItem } from '@/api/file'

const props = defineProps<{
  modelValue: string  // 路径
  fsId: number        // 目录 fs_id
}>()

const emit = defineEmits<{
  'update:modelValue': [value: string]
  'update:fsId': [value: number]
}>()

// 状态
const dialogVisible = ref(false)
const loading = ref(false)
const loadingMore = ref(false)
const creating = ref(false)
const currentPath = ref('/')
const currentFsId = ref<number>(0)  // 当前浏览目录的 fs_id
const folders = ref<FileItem[]>([])
const selectedFsId = ref<number | null>(null)
const selectedPath = ref<string>('')
const newFolderName = ref('')
const scrollbarRef = ref<any>(null)

// 分页状态
const currentPage = ref(1)
const pageSize = 50
const hasMore = ref(true)

// 显示路径
const displayPath = computed(() => {
  return props.modelValue || '/'
})

// 路径分割
const pathParts = computed(() => {
  if (currentPath.value === '/') return []
  return currentPath.value.split('/').filter(p => p)
})

// 获取指定深度的路径
function getPathUpTo(index: number): string {
  const parts = pathParts.value.slice(0, index + 1)
  return '/' + parts.join('/')
}

// 打开对话框
function openDialog() {
  dialogVisible.value = true
}

// 对话框打开时初始化
async function handleDialogOpen() {
  // 初始化为当前选中的路径
  const initPath = props.modelValue || '/'
  const initFsId = props.fsId || 0
  selectedFsId.value = null  // 初始时没有选中子文件夹
  selectedPath.value = ''
  currentFsId.value = initFsId  // 设置当前目录的 fs_id
  await loadFolders(initPath)
}

// 加载目录列表
async function loadFolders(path: string, append: boolean = false) {
  if (append) {
    loadingMore.value = true
  } else {
    loading.value = true
    currentPage.value = 1
    hasMore.value = true
    folders.value = []
  }
  currentPath.value = path

  try {
    const page = append ? currentPage.value : 1
    const data = await getFileList(path, page, pageSize)
    // 只显示文件夹
    const newFolders = data.list.filter(item => item.isdir === 1)

    if (append) {
      folders.value = [...folders.value, ...newFolders]
    } else {
      folders.value = newFolders
    }

    // 判断是否还有更多数据
    hasMore.value = data.has_more
    currentPage.value = data.page
  } catch (error: any) {
    ElMessage.error(error.message || '加载目录失败')
    if (!append) {
      folders.value = []
    }
  } finally {
    loading.value = false
    loadingMore.value = false
  }
}

// 加载下一页
async function loadNextPage() {
  if (loadingMore.value || !hasMore.value) return
  currentPage.value++
  await loadFolders(currentPath.value, true)
}

// 滚动事件处理
function handleScroll(_event: { scrollTop: number; scrollLeft: number }) {
  const scrollbar = scrollbarRef.value
  if (!scrollbar) return

  // 获取滚动容器
  const wrapRef = scrollbar.wrapRef
  if (!wrapRef) return

  const { scrollTop, scrollHeight, clientHeight } = wrapRef

  // 当滚动到距离底部 50px 时加载更多
  if (scrollHeight - scrollTop - clientHeight < 50) {
    loadNextPage()
  }
}

// 导航到路径（通过面包屑）
function navigateTo(path: string) {
  // 清除选中状态
  selectedFsId.value = null
  selectedPath.value = ''
  // 导航到根目录时 fs_id 为 0，其他情况需要从文件夹信息获取
  if (path === '/') {
    currentFsId.value = 0
  }
  loadFolders(path)
}

// 选中文件夹
function selectFolder(folder: FileItem) {
  selectedFsId.value = folder.fs_id
  selectedPath.value = folder.path
}

// 进入文件夹（双击）
function enterFolder(folder: FileItem) {
  selectedFsId.value = null
  selectedPath.value = ''
  currentFsId.value = folder.fs_id  // 更新当前目录的 fs_id
  loadFolders(folder.path)
}

// 创建新文件夹
async function createNewFolder() {
  const name = newFolderName.value.trim()
  if (!name) {
    ElMessage.warning('请输入文件夹名称')
    return
  }

  // 验证文件夹名
  if (/[<>:"/\\|?*]/.test(name)) {
    ElMessage.warning('文件夹名称不能包含特殊字符')
    return
  }

  creating.value = true

  try {
    const fullPath = currentPath.value === '/'
      ? `/${name}`
      : `${currentPath.value}/${name}`

    await createFolder(fullPath)
    ElMessage.success('文件夹创建成功')
    newFolderName.value = ''

    // 刷新列表
    await loadFolders(currentPath.value)
  } catch (error: any) {
    ElMessage.error(error.message || '创建文件夹失败')
  } finally {
    creating.value = false
  }
}

// 确认选择
function confirmSelection() {
  // 如果选中了某个子文件夹，使用该文件夹的信息
  // 否则使用当前浏览目录的信息
  const path = selectedPath.value || currentPath.value
  const fsId = selectedFsId.value ?? currentFsId.value

  emit('update:modelValue', path)
  emit('update:fsId', fsId)
  dialogVisible.value = false
}

// 监听外部值变化
watch(() => props.modelValue, (newVal) => {
  if (newVal && newVal !== selectedPath.value) {
    selectedPath.value = newVal
  }
})
</script>

<style scoped lang="scss">
.netdisk-path-selector {
  width: 100%;
}

.path-display {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border: 1px solid var(--el-border-color);
  border-radius: 4px;
  cursor: pointer;
  transition: all 0.2s;

  &:hover {
    border-color: var(--el-color-primary);
  }

  .folder-icon {
    color: #e6a23c;
    flex-shrink: 0;
  }

  .path-text {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    color: var(--el-text-color-primary);
  }

  .arrow-icon {
    color: var(--el-text-color-secondary);
    flex-shrink: 0;
  }
}

.breadcrumb-nav {
  padding: 12px 0;
  border-bottom: 1px solid var(--el-border-color-lighter);
  margin-bottom: 12px;

  :deep(.el-breadcrumb__item) {
    cursor: pointer;

    &:hover {
      color: var(--el-color-primary);
    }
  }
}

.folder-list {
  min-height: 300px;
}

.loading-more {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 8px;
  padding: 12px;
  color: #909399;
  font-size: 13px;
}

.no-more {
  text-align: center;
  padding: 12px;
  color: #c0c4cc;
  font-size: 13px;
}

.folder-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 12px;
  cursor: pointer;
  border-radius: 4px;
  transition: all 0.2s;

  &:hover {
    background-color: var(--el-fill-color-light);
  }

  &.selected {
    background-color: var(--el-color-primary-light-9);
    color: var(--el-color-primary);
  }

  .folder-icon {
    color: #e6a23c;
    font-size: 20px;
  }

  .folder-name {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

.create-folder {
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid var(--el-border-color-lighter);
}

.dialog-footer {
  display: flex;
  justify-content: space-between;
  align-items: center;
  width: 100%;

  .selected-path {
    color: var(--el-text-color-secondary);
    font-size: 13px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    max-width: 300px;
  }

  .actions {
    display: flex;
    gap: 8px;
  }
}
</style>
