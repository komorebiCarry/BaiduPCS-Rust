<template>
  <div class="files-container" :class="{ 'is-mobile': isMobile }">
    <!-- 面包屑导航 -->
    <div class="breadcrumb-bar">
      <el-breadcrumb separator="/">
        <el-breadcrumb-item @click="navigateToDir('')">
          <el-icon>
            <HomeFilled/>
          </el-icon>
          <span v-if="!isMobile">下载目录</span>
        </el-breadcrumb-item>
        <el-breadcrumb-item
            v-for="(part, index) in pathParts"
            :key="index"
            @click="navigateToDir(getPathUpTo(index))"
        >
          {{ part }}
        </el-breadcrumb-item>
      </el-breadcrumb>

      <!-- PC端工具栏 -->
      <div v-if="!isMobile" class="toolbar-buttons">
        <el-button
            v-if="selectedFiles.length > 0"
            type="danger"
            :loading="batchDeleting"
            @click="handleBatchDelete"
        >
          <el-icon><Delete /></el-icon>
          删除 ({{ selectedFiles.length }})
        </el-button>
        <el-button type="primary" @click="refreshFileList">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </div>

      <!-- 移动端工具栏 -->
      <div v-else class="toolbar-buttons-mobile">
        <el-button
            v-if="selectedFiles.length > 0"
            type="danger"
            circle
            :loading="batchDeleting"
            @click="handleBatchDelete"
        >
          <el-icon><Delete /></el-icon>
        </el-button>
        <el-button type="primary" circle @click="refreshFileList">
          <el-icon><Refresh /></el-icon>
        </el-button>
      </div>
    </div>

    <!-- 文件列表 -->
    <div class="file-list" ref="fileListRef" @scroll="handleScroll">
      <!-- PC端表格视图 -->
      <el-table
          v-if="!isMobile"
          v-loading="loading"
          :data="fileList"
          style="width: 100%"
          @row-click="handleRowClick"
          @selection-change="handleSelectionChange"
          :row-class-name="getRowClassName"
      >
        <el-table-column type="selection" width="55" />
        <el-table-column label="文件名" min-width="400">
          <template #default="{ row }">
            <div class="file-name">
              <el-icon :size="20" class="file-icon">
                <Folder v-if="row.entryType === 'directory'"/>
                <Document v-else/>
              </el-icon>
              <span>{{ row.name }}</span>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="大小" width="120">
          <template #default="{ row }">
            <span v-if="row.entryType === 'file'">{{ formatFileSize(row.size) }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>

        <el-table-column label="修改时间" width="180">
          <template #default="{ row }">
            {{ formatTime(row.updatedAt) }}
          </template>
        </el-table-column>
      </el-table>

      <!-- 移动端卡片视图 -->
      <div v-else class="mobile-file-list" v-loading="loading">
        <div
            v-for="item in fileList"
            :key="item.id"
            class="mobile-file-card"
            :class="{ 'is-folder': item.entryType === 'directory' }"
            @click="handleRowClick(item)"
        >
          <div class="file-card-main">
            <el-icon :size="36" class="file-card-icon" :color="item.entryType === 'directory' ? '#e6a23c' : '#409eff'">
              <Folder v-if="item.entryType === 'directory'"/>
              <Document v-else/>
            </el-icon>
            <div class="file-card-info">
              <div class="file-card-name">{{ item.name }}</div>
              <div class="file-card-meta">
                <span v-if="item.entryType === 'file'">{{ formatFileSize(item.size) }}</span>
                <span v-else>文件夹</span>
                <span class="meta-divider">·</span>
                <span>{{ formatTime(item.updatedAt) }}</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- 加载更多提示 -->
      <div v-if="loadingMore" class="loading-more">
        <el-icon class="is-loading"><Loading /></el-icon>
        <span>加载中...</span>
      </div>
      <div v-else-if="!hasMore && fileList.length > 0" class="no-more">
        没有更多了
      </div>

      <!-- 空状态 -->
      <el-empty v-if="!loading && fileList.length === 0" description="当前目录为空"/>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, computed } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { listLocalFiles, deleteLocalFiles, type FileEntry } from '@/api/localFiles'
import { formatFileSize, formatTime } from '@/api/filesystem'
import { useIsMobile } from '@/utils/responsive'

const isMobile = useIsMobile()

const loading = ref(false)
const loadingMore = ref(false)
const fileList = ref<FileEntry[]>([])
const currentPath = ref('')
const rootPath = ref('')
const currentPage = ref(0)
const hasMore = ref(true)
const fileListRef = ref<HTMLElement | null>(null)
const selectedFiles = ref<FileEntry[]>([])
const batchDeleting = ref(false)

const pathParts = computed(() => {
  if (!currentPath.value || currentPath.value === rootPath.value) return []
  const relative = currentPath.value.startsWith(rootPath.value)
      ? currentPath.value.slice(rootPath.value.length)
      : currentPath.value
  return relative.split('/').filter(p => p)
})

function getPathUpTo(index: number): string {
  const parts = pathParts.value.slice(0, index + 1)
  return rootPath.value + '/' + parts.join('/')
}

async function loadFiles(path: string, append: boolean = false) {
  if (append) {
    loadingMore.value = true
  } else {
    loading.value = true
    currentPage.value = 0
    hasMore.value = true
  }

  try {
    const page = append ? currentPage.value : 0
    const data = await listLocalFiles(path, page, 100)

    if (append) {
      fileList.value = [...fileList.value, ...data.entries]
    } else {
      fileList.value = data.entries
      currentPath.value = data.currentPath
      if (!rootPath.value) {
        rootPath.value = data.currentPath
      }
    }

    hasMore.value = data.hasMore
    currentPage.value = data.page
  } catch (error: any) {
    ElMessage.error(error.message || '加载本地文件列表失败')
  } finally {
    loading.value = false
    loadingMore.value = false
  }
}

async function loadNextPage() {
  if (loadingMore.value || !hasMore.value) return
  currentPage.value++
  await loadFiles(currentPath.value, true)
}

function handleScroll(event: Event) {
  const target = event.target as HTMLElement
  const { scrollTop, scrollHeight, clientHeight } = target
  if (scrollHeight - scrollTop - clientHeight < 100) {
    loadNextPage()
  }
}

function navigateToDir(path: string) {
  loadFiles(path || '')
}

function refreshFileList() {
  loadFiles(currentPath.value || '')
}

function handleRowClick(row: FileEntry) {
  if (row.entryType === 'directory') {
    navigateToDir(row.path)
  }
}

function getRowClassName({ row }: { row: FileEntry }) {
  return row.entryType === 'directory' ? 'directory-row' : ''
}

function handleSelectionChange(selection: FileEntry[]) {
  selectedFiles.value = selection
}

async function handleBatchDelete() {
  if (selectedFiles.value.length === 0) return
  const count = selectedFiles.value.length
  const paths = selectedFiles.value.map(f => f.path)
  try {
    await ElMessageBox.confirm(
        `确定要删除选中的 ${count} 个文件/文件夹吗？此操作不可恢复！`,
        '确认删除',
        {
          confirmButtonText: '删除',
          cancelButtonText: '取消',
          type: 'warning',
          beforeClose: async (action, instance, done) => {
            if (action !== 'confirm') { done(); return }
            instance.confirmButtonLoading = true
            instance.confirmButtonText = '删除中...'
            try {
              const result = await deleteLocalFiles(paths)
              done()
              if (result.failed_paths.length > 0) {
                ElMessage.warning(`成功删除 ${result.deleted_count} 个，失败 ${result.failed_paths.length} 个`)
              } else {
                ElMessage.success(`成功删除 ${result.deleted_count} 个文件/文件夹`)
              }
              selectedFiles.value = []
              await refreshFileList()
            } catch (error: any) {
              done()
              ElMessage.error(error.message || '删除失败')
            } finally {
              instance.confirmButtonLoading = false
            }
          }
        }
    )
  } catch {
    // 用户取消
  }
}

onMounted(() => {
  loadFiles('')
})
</script>

<script lang="ts">
export { Folder, Document, Refresh, HomeFilled, Loading, Delete } from '@element-plus/icons-vue'
</script>

<style scoped lang="scss">
.files-container {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  background: white;
}

.breadcrumb-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  border-bottom: 1px solid #e0e0e0;
  background: white;
  gap: 12px;

  .toolbar-buttons {
    display: flex;
    gap: 12px;
    flex-shrink: 0;
  }

  .toolbar-buttons-mobile {
    display: flex;
    gap: 8px;
    flex-shrink: 0;
  }
}

.file-list {
  flex: 1;
  padding: 20px;
  overflow: auto;
}

.loading-more {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 8px;
  padding: 16px;
  color: #909399;
  font-size: 14px;
}

.no-more {
  text-align: center;
  padding: 16px;
  color: #c0c4cc;
  font-size: 14px;
}

.file-name {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;

  .file-icon {
    flex-shrink: 0;
  }

  &:hover {
    color: #409eff;
  }
}

:deep(.directory-row) {
  cursor: pointer;

  &:hover {
    background-color: #f5f7fa;
  }
}

:deep(.el-table__row) {
  &:hover .file-name {
    color: #409eff;
  }
}

.is-mobile {
  height: calc(100vh - 60px - 56px);

  .breadcrumb-bar {
    padding: 12px 16px;
    flex-wrap: wrap;
  }

  .file-list {
    padding: 12px;
  }
}

.mobile-file-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.mobile-file-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  background: #f9f9f9;
  border-radius: 12px;
  cursor: pointer;
  transition: all 0.2s;

  &:active {
    background: #f0f0f0;
    transform: scale(0.98);
  }

  &.is-folder {
    background: #fffbf0;

    &:active {
      background: #fff3d9;
    }
  }

  .file-card-main {
    display: flex;
    align-items: center;
    gap: 12px;
    flex: 1;
    min-width: 0;
  }

  .file-card-icon {
    flex-shrink: 0;
  }

  .file-card-info {
    flex: 1;
    min-width: 0;
  }

  .file-card-name {
    font-size: 15px;
    font-weight: 500;
    color: #333;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    margin-bottom: 4px;
  }

  .file-card-meta {
    font-size: 12px;
    color: #909399;
    display: flex;
    align-items: center;
    gap: 4px;

    .meta-divider {
      color: #dcdfe6;
    }
  }
}

@media (max-width: 767px) {
  :deep(.el-dialog) {
    width: 92% !important;
    margin: 5vh auto !important;
  }
}
</style>
