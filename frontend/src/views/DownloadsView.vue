<template>
  <div class="downloads-container">
    <!-- 顶部工具栏 -->
    <div class="toolbar">
      <div class="header-left">
        <h2>下载管理</h2>
        <el-tag :type="activeCountType" size="large">
          {{ activeCount }} 个任务进行中
        </el-tag>
      </div>
      <div class="header-right">
        <el-button @click="refreshTasks">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
        <el-button @click="handleClearCompleted" :disabled="completedCount === 0">
          清除已完成 ({{ completedCount }})
        </el-button>
        <el-button @click="handleClearFailed" :disabled="failedCount === 0" type="danger" plain>
          清除失败 ({{ failedCount }})
        </el-button>
      </div>
    </div>

    <!-- 下载任务列表 -->
    <div class="task-container">
      <el-empty v-if="!loading && downloadItems.length === 0" description="暂无下载任务" />

      <div v-else class="task-list">
        <el-card
            v-for="item in downloadItems"
            :key="item.id"
            class="task-card"
            :class="{
              'task-active': item.status === 'downloading' || item.status === 'scanning',
              'is-folder': item.type === 'folder'
            }"
            shadow="hover"
        >
          <!-- 任务信息 -->
          <div class="task-header">
            <div class="task-info">
              <div class="task-title">
                <el-icon :size="20" class="file-icon">
                  <Folder v-if="item.type === 'folder'" />
                  <Document v-else />
                </el-icon>
                <span class="filename">
                    {{ item.type === 'folder' ? item.name : getFilename(item.local_path || '') }}
                  </span>
                <el-tag
                    :type="item.type === 'folder' ? getFolderStatusType(item.status as FolderStatus) : getStatusType(item.status as TaskStatus)"
                    size="small"
                >
                  {{ item.type === 'folder' ? getFolderStatusText(item.status as FolderStatus) : getStatusText(item.status as TaskStatus) }}
                </el-tag>
                <span v-if="item.type === 'folder' && item.status === 'scanning'" class="scanning-hint">
                    (已发现 {{ item.total_files }} 个文件)
                  </span>
              </div>
              <div class="task-path">
                {{ item.type === 'folder' ? item.remote_root : item.remote_path }}
              </div>
            </div>

            <!-- 操作按钮 -->
            <div class="task-actions">
              <el-button
                  v-if="item.type === 'folder'"
                  size="small"
                  @click="showFolderDetail(item)"
              >
                <el-icon><List /></el-icon>
                详情
              </el-button>
              <el-button
                  v-if="item.status === 'downloading' || item.status === 'scanning'"
                  size="small"
                  @click="handlePause(item)"
              >
                <el-icon><VideoPause /></el-icon>
                暂停
              </el-button>
              <el-button
                  v-if="item.status === 'paused'"
                  size="small"
                  type="primary"
                  @click="handleResume(item)"
              >
                <el-icon><VideoPlay /></el-icon>
                继续
              </el-button>
              <el-button
                  v-if="item.status === 'completed'"
                  size="small"
                  type="success"
                  @click="openLocalFile(item.type === 'folder' ? (item.local_root || '') : (item.local_path || ''))"
              >
                <el-icon><FolderOpened /></el-icon>
                打开文件夹
              </el-button>
              <el-button
                  size="small"
                  type="danger"
                  :disabled="deletingIds.has(item.id!)"
                  :loading="deletingIds.has(item.id!)"
                  @click="handleDelete(item)"
              >
                <el-icon><Delete /></el-icon>
                {{ deletingIds.has(item.id!) ? '删除中...' : '删除' }}
              </el-button>
            </div>
          </div>

          <!-- 进度条 -->
          <div class="task-progress">
            <el-progress
                :percentage="((item.downloaded_size || 0) / (item.total_size || 1) * 100)"
                :status="getProgressStatus(item.status!)"
                :stroke-width="8"
            >
              <template #default="{ percentage }">
                <span class="progress-text">{{ percentage.toFixed(1) }}%</span>
              </template>
            </el-progress>
          </div>

          <!-- 下载统计 -->
          <div class="task-stats">
            <!-- 文件夹特有统计 -->
            <div v-if="item.type === 'folder'" class="stat-item">
              <span class="stat-label">进度:</span>
              <span class="stat-value">{{ item.completed_files }}/{{ item.total_files }} 个文件</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">已下载:</span>
              <span class="stat-value">{{ formatFileSize(item.downloaded_size || 0) }}</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">总大小:</span>
              <span class="stat-value">{{ formatFileSize(item.total_size || 0) }}</span>
            </div>
            <div class="stat-item" v-if="item.status === 'downloading' || item.status === 'scanning'">
              <span class="stat-label">速度:</span>
              <span class="stat-value speed">{{ formatSpeed(item.speed || 0) }}</span>
            </div>
            <div class="stat-item" v-if="item.status === 'downloading' && item.type === 'file'">
              <span class="stat-label">剩余时间:</span>
              <span class="stat-value">{{ formatETA(calculateETA({
                total_size: item.total_size || 0,
                downloaded_size: item.downloaded_size || 0,
                speed: item.speed || 0
              } as any)) }}</span>
            </div>
            <div class="stat-item" v-if="item.error">
              <span class="stat-label error">错误:</span>
              <span class="stat-value error">{{ item.error }}</span>
            </div>
          </div>
        </el-card>
      </div>
    </div>

    <!-- 文件夹详情弹窗 -->
    <el-dialog
        v-model="folderDetailDialog.visible"
        :title="`文件夹详情: ${folderDetailDialog.folderName}`"
        width="900px"
        top="5vh"
        @close="stopFolderDetailTimer"
    >
      <div class="folder-detail">
        <!-- 文件夹统计信息 -->
        <div class="folder-stats">
          <div class="stat-card">
            <div class="stat-label">总文件数</div>
            <div class="stat-value">{{ folderDetailDialog.totalFiles }}</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">已完成</div>
            <div class="stat-value success">{{ folderDetailDialog.completedFiles }}</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">下载中</div>
            <div class="stat-value primary">{{ folderDetailDialog.downloadingFiles }}</div>
          </div>
          <div class="stat-card">
            <div class="stat-label">待处理</div>
            <div class="stat-value info">{{ folderDetailDialog.pendingFiles }}</div>
          </div>
        </div>

        <!-- 子任务列表 -->
        <div class="subtasks-container">
          <div class="subtasks-header">
            <span>子任务列表 ({{ folderDetailDialog.tasks.length }} 个)</span>
            <el-input
                v-model="folderDetailDialog.searchText"
                placeholder="搜索文件名"
                clearable
                style="width: 250px"
                size="small"
            >
              <template #prefix>
                <el-icon><Search /></el-icon>
              </template>
            </el-input>
          </div>

          <el-table
              :data="filteredSubtasks"
              stripe
              height="450"
              :default-sort="{ prop: 'status', order: 'ascending' }"
          >
            <el-table-column label="文件名" min-width="300" show-overflow-tooltip>
              <template #default="{ row }">
                <div class="file-name-cell">
                  <el-icon :size="16"><Document /></el-icon>
                  <span>{{ getFileName(row) }}</span>
                </div>
              </template>
            </el-table-column>

            <el-table-column label="状态" width="100" sortable prop="status">
              <template #default="{ row }">
                <el-tag :type="getStatusType(row.status)" size="small">
                  {{ getStatusText(row.status) }}
                </el-tag>
              </template>
            </el-table-column>

            <el-table-column label="大小" width="120" sortable prop="total_size">
              <template #default="{ row }">
                {{ formatFileSize(row.total_size) }}
              </template>
            </el-table-column>

            <el-table-column label="进度" width="180">
              <template #default="{ row }">
                <el-progress
                    :percentage="((row.downloaded_size / row.total_size) * 100)"
                    :status="getProgressStatus(row.status)"
                    :stroke-width="6"
                    :text-inside="false"
                    :show-text="true"
                >
                  <template #default="{ percentage }">
                    <span style="font-size: 12px">{{ percentage.toFixed(1) }}%</span>
                  </template>
                </el-progress>
              </template>
            </el-table-column>

            <el-table-column label="速度" width="120">
              <template #default="{ row }">
                <span v-if="row.status === 'downloading'" class="speed-text">
                  {{ formatSpeed(row.speed) }}
                </span>
                <span v-else class="placeholder-text">-</span>
              </template>
            </el-table-column>
          </el-table>
        </div>
      </div>

      <template #footer>
        <el-button @click="folderDetailDialog.visible = false">关闭</el-button>
        <el-button type="primary" @click="refreshFolderDetail">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  getAllDownloadsMixed,
  getAllDownloads,
  pauseDownload,
  resumeDownload,
  deleteDownload,
  pauseFolderDownload,
  resumeFolderDownload,
  cancelFolderDownload,
  clearCompleted,
  clearFailed,
  calculateETA,
  formatFileSize,
  formatSpeed,
  formatETA,
  getStatusText,
  getStatusType,
  getFolderStatusText,
  getFolderStatusType,
  type DownloadItemFromBackend,
  type DownloadTask,
  type TaskStatus,
  type FolderStatus,
} from '@/api/download'
import {
  Refresh,
  Document,
  Folder,
  VideoPause,
  VideoPlay,
  Delete,
  FolderOpened,
  List,
  Search,
} from '@element-plus/icons-vue'

// 状态
const loading = ref(false)
const downloadItems = ref<DownloadItemFromBackend[]>([])
const deletingIds = ref<Set<string>>(new Set()) // 正在删除的任务ID集合

// 文件夹详情弹窗
const folderDetailDialog = ref({
  visible: false,
  folderId: '',
  folderName: '',
  totalFiles: 0,
  completedFiles: 0,
  downloadingFiles: 0,
  pendingFiles: 0,
  tasks: [] as DownloadTask[],
  searchText: '',
})

// 自动刷新定时器
let refreshTimer: number | null = null
// 文件夹详情弹窗刷新定时器
let folderDetailTimer: number | null = null

// 是否有活跃任务（需要实时刷新）
const hasActiveTasks = computed(() => {
  return downloadItems.value.some(item => {
    const status = item.status
    return status === 'downloading' || status === 'scanning' || status === 'paused' || status === 'pending'
  })
})

// 计算属性
const activeCount = computed(() => {
  return downloadItems.value.filter(item =>
      item.status === 'downloading' || item.status === 'scanning'
  ).length
})

const completedCount = computed(() => {
  return downloadItems.value.filter(item => item.status === 'completed').length
})

const failedCount = computed(() => {
  return downloadItems.value.filter(item => item.status === 'failed').length
})

const activeCountType = computed(() => {
  if (activeCount.value === 0) return 'info'
  if (activeCount.value <= 3) return 'success'
  return 'warning'
})

// 过滤后的子任务（用于弹窗搜索）
const filteredSubtasks = computed(() => {
  const searchText = folderDetailDialog.value.searchText.toLowerCase().trim()
  if (!searchText) {
    return folderDetailDialog.value.tasks
  }
  return folderDetailDialog.value.tasks.filter((task) => {
    const filename = getFileName(task).toLowerCase()
    return filename.includes(searchText)
  })
})

// 获取文件名
function getFilename(path: string): string {
  const parts = path.replace(/\\/g, '/').split('/')
  return parts[parts.length - 1] || path
}

// 获取文件名（用于子任务表格）
function getFileName(task: DownloadTask): string {
  return task.relative_path || getFilename(task.remote_path)
}

// 获取进度条状态
function getProgressStatus(status: TaskStatus | FolderStatus): 'success' | 'exception' | 'warning' | undefined {
  if (status === 'completed') return 'success'
  if (status === 'failed') return 'exception'
  if (status === 'paused') return 'warning'
  return undefined
}

// 刷新任务列表
async function refreshTasks() {
  // 如果正在加载中，跳过本次请求，避免并发请求
  if (loading.value) {
    return
  }

  loading.value = true
  try {
    downloadItems.value = await getAllDownloadsMixed()
  } catch (error: any) {
    console.error('刷新任务列表失败:', error)
    // 请求失败时，清空任务列表，避免显示过时数据
    downloadItems.value = []
  } finally {
    loading.value = false
    // 无论成功还是失败，都要检查并更新自动刷新状态
    updateAutoRefresh()
  }
}

// 更新自动刷新状态
function updateAutoRefresh() {
  // 如果有活跃任务，启动或保持定时刷新
  if (hasActiveTasks.value) {
    if (!refreshTimer) {
      console.log('启动自动刷新定时器，活跃任务数:', activeCount.value)
      refreshTimer = window.setInterval(() => {
        refreshTasks()
      }, 100)
    }
  } else {
    // 没有活跃任务时，停止定时刷新
    if (refreshTimer) {
      console.log('停止自动刷新定时器，当前任务数:', downloadItems.value.length)
      clearInterval(refreshTimer)
      refreshTimer = null
    }
  }
}

// 暂停任务（文件或文件夹）
async function handlePause(item: DownloadItemFromBackend) {
  try {
    if (item.type === 'folder') {
      await pauseFolderDownload(item.id!)
    } else {
      await pauseDownload(item.id!)
    }
    ElMessage.success('任务已暂停')
    refreshTasks()
  } catch (error: any) {
    console.error('暂停任务失败:', error)
  }
}

// 恢复任务（文件或文件夹）
async function handleResume(item: DownloadItemFromBackend) {
  try {
    if (item.type === 'folder') {
      await resumeFolderDownload(item.id!)
    } else {
      await resumeDownload(item.id!)
    }
    ElMessage.success('任务已继续')
    refreshTasks()
  } catch (error: any) {
    console.error('恢复任务失败:', error)
  }
}

// 删除任务（文件或文件夹）
async function handleDelete(item: DownloadItemFromBackend) {
  const status = item.status!
  const hasLocalFile = status === 'completed' || status === 'paused' || status === 'downloading'

  try {
    let deleteFiles = false

    if (hasLocalFile) {
      // 询问用户是否删除本地文件
      const action = await ElMessageBox.confirm(
          '是否同时删除本地已下载的文件？',
          '删除确认',
          {
            confirmButtonText: '删除文件',
            cancelButtonText: '仅删除任务',
            distinguishCancelAndClose: true,
            type: 'warning',
          }
      ).catch((action: string) => action)

      if (action === 'close') {
        return // 用户关闭对话框，取消操作
      }
      deleteFiles = action === 'confirm'
    } else {
      // 没有本地文件，直接确认删除任务
      await ElMessageBox.confirm(
          '确定要删除此任务吗？',
          '删除确认',
          {
            confirmButtonText: '确定',
            cancelButtonText: '取消',
            type: 'warning',
          }
      )
    }

    // 标记为正在删除
    deletingIds.value.add(item.id!)

    // 文件夹删除需要显示加载提示（因为需要等待所有分片停止）
    let loadingInstance: any = null
    if (item.type === 'folder') {
      loadingInstance = ElMessage({
        message: '正在安全停止所有下载任务，请稍候...',
        type: 'info',
        duration: 0, // 不自动关闭
        showClose: false,
      })
    }

    try {
      if (item.type === 'folder') {
        await cancelFolderDownload(item.id!, deleteFiles)
      } else {
        await deleteDownload(item.id!, deleteFiles)
      }

      ElMessage.success(deleteFiles ? '任务和文件已删除' : '任务已删除')
    } finally {
      // 关闭加载提示
      if (loadingInstance) {
        loadingInstance.close()
      }
      // 移除删除状态
      deletingIds.value.delete(item.id!)
    }

    refreshTasks()
  } catch (error: any) {
    // 移除删除状态
    deletingIds.value.delete(item.id!)

    if (error !== 'cancel' && error !== 'close') {
      console.error('删除任务失败:', error)
      ElMessage.error('删除任务失败: ' + (error.message || error))
    }
  }
}

// 清除已完成
async function handleClearCompleted() {
  try {
    await ElMessageBox.confirm(
        `确定要清除所有已完成的任务吗？（共${completedCount.value}个）`,
        '批量清除',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )
    const count = await clearCompleted()
    ElMessage.success(`已清除 ${count} 个任务`)
    refreshTasks()
  } catch (error: any) {
    if (error !== 'cancel') {
      console.error('清除已完成任务失败:', error)
    }
  }
}

// 清除失败
async function handleClearFailed() {
  try {
    await ElMessageBox.confirm(
        `确定要清除所有失败的任务吗？（共${failedCount.value}个）`,
        '批量清除',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )
    const count = await clearFailed()
    ElMessage.success(`已清除 ${count} 个任务`)
    refreshTasks()
  } catch (error: any) {
    if (error !== 'cancel') {
      console.error('清除失败任务失败:', error)
    }
  }
}

// 打开本地文件夹
function openLocalFile(path: string) {
  ElMessage.info(`文件位置: ${path}`)
  // TODO: 实现打开本地文件夹功能
  // 可以使用Electron或Tauri等桌面框架的API
}

// 显示文件夹详情弹窗
async function showFolderDetail(item: DownloadItemFromBackend) {
  if (!item.id) return

  folderDetailDialog.value.visible = true
  folderDetailDialog.value.folderId = item.id
  folderDetailDialog.value.folderName = item.name || '未知文件夹'
  folderDetailDialog.value.searchText = ''

  await refreshFolderDetail()

  // 启动文件夹详情自动刷新定时器
  startFolderDetailTimer()
}

// 启动文件夹详情定时器
function startFolderDetailTimer() {
  stopFolderDetailTimer()
  folderDetailTimer = window.setInterval(() => {
    if (folderDetailDialog.value.visible) {
      refreshFolderDetail()
    } else {
      stopFolderDetailTimer()
    }
  }, 500)
}

// 停止文件夹详情定时器
function stopFolderDetailTimer() {
  if (folderDetailTimer) {
    clearInterval(folderDetailTimer)
    folderDetailTimer = null
  }
}

// 刷新文件夹详情
async function refreshFolderDetail() {
  const folderId = folderDetailDialog.value.folderId
  if (!folderId) return

  try {
    // 获取所有任务，然后过滤出属于该文件夹的任务
    const allTasks = await getAllDownloads()
    const folderTasks = allTasks.filter((task) => task.group_id === folderId)

    // 计算统计数据
    const completedFiles = folderTasks.filter((t) => t.status === 'completed').length
    const downloadingFiles = folderTasks.filter((t) => t.status === 'downloading').length
    const pendingFiles = folderTasks.filter((t) => t.status === 'pending').length

    folderDetailDialog.value.tasks = folderTasks
    folderDetailDialog.value.totalFiles = folderTasks.length
    folderDetailDialog.value.completedFiles = completedFiles
    folderDetailDialog.value.downloadingFiles = downloadingFiles
    folderDetailDialog.value.pendingFiles = pendingFiles

    // 同时获取文件夹的 total_files（包括 pending_files 中的）
    const folderItem = downloadItems.value.find((i) => i.id === folderId && i.type === 'folder')
    if (folderItem && folderItem.total_files) {
      const notCreatedYet = (folderItem.total_files || 0) - folderTasks.length
      if (notCreatedYet > 0) {
        folderDetailDialog.value.pendingFiles += notCreatedYet
        folderDetailDialog.value.totalFiles = folderItem.total_files
      }
    }
  } catch (error: any) {
    console.error('获取文件夹子任务失败:', error)
    ElMessage.error('获取文件夹子任务失败')
  }
}

// 组件挂载时加载任务列表
onMounted(() => {
  refreshTasks()
  // updateAutoRefresh 会在 refreshTasks 完成后根据任务状态自动启动定时器
})

// 组件卸载时清除定时器
onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  stopFolderDetailTimer()
})
</script>

<style scoped lang="scss">
.downloads-container {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  background: #f5f5f5;
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: white;
  border-bottom: 1px solid #e0e0e0;
  padding: 16px 20px;

  .header-left {
    display: flex;
    align-items: center;
    gap: 20px;

    h2 {
      margin: 0;
      font-size: 18px;
      color: #333;
    }
  }

  .header-right {
    display: flex;
    gap: 10px;
  }
}

.task-container {
  flex: 1;
  padding: 20px;
  overflow: auto;
}

.task-list {
  display: flex;
  flex-direction: column;
  gap: 15px;
}

.task-card {
  transition: all 0.3s;

  &.task-active {
    border-color: #409eff;
    box-shadow: 0 2px 12px rgba(64, 158, 255, 0.2);
  }

  &.is-folder {
    border-left: 4px solid #67c23a;
  }

  &:hover {
    transform: translateY(-2px);
  }
}

.scanning-hint {
  color: #909399;
  font-size: 12px;
  margin-left: 8px;
}

.task-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 15px;
}

.task-info {
  flex: 1;
  min-width: 0;
}

.task-title {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;

  .file-icon {
    flex-shrink: 0;
    color: #409eff;
  }

  .filename {
    font-size: 16px;
    font-weight: 500;
    color: #333;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
}

.task-path {
  font-size: 12px;
  color: #999;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding-left: 30px;
}

.task-actions {
  display: flex;
  gap: 8px;
  flex-shrink: 0;
  margin-left: 20px;
}

.task-progress {
  margin-bottom: 15px;

  .progress-text {
    font-size: 12px;
    font-weight: 500;
  }
}

.task-stats {
  display: flex;
  gap: 20px;
  flex-wrap: wrap;

  .stat-item {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;

    .stat-label {
      color: #666;

      &.error {
        color: #f56c6c;
      }
    }

    .stat-value {
      color: #333;
      font-weight: 500;

      &.speed {
        color: #67c23a;
        font-weight: 600;
      }

      &.error {
        color: #f56c6c;
      }
    }
  }
}

:deep(.el-progress__text) {
  font-size: 12px !important;
}

// 文件夹详情弹窗样式
.folder-detail {
  .folder-stats {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 16px;
    margin-bottom: 20px;

    .stat-card {
      background: #f5f7fa;
      border-radius: 8px;
      padding: 16px;
      text-align: center;

      .stat-label {
        font-size: 12px;
        color: #909399;
        margin-bottom: 8px;
      }

      .stat-value {
        font-size: 24px;
        font-weight: 600;
        color: #303133;

        &.success {
          color: #67c23a;
        }

        &.primary {
          color: #409eff;
        }

        &.info {
          color: #909399;
        }
      }
    }
  }

  .subtasks-container {
    .subtasks-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
      font-size: 14px;
      font-weight: 500;
      color: #606266;
    }

    .file-name-cell {
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .speed-text {
      color: #67c23a;
      font-weight: 500;
    }

    .placeholder-text {
      color: #c0c4cc;
    }
  }
}
</style>
