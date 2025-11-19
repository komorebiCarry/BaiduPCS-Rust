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
        <el-empty v-if="!loading && tasks.length === 0" description="暂无下载任务" />

        <div v-else class="task-list">
          <el-card
            v-for="task in tasks"
            :key="task.id"
            class="task-card"
            :class="{ 'task-active': task.status === 'downloading' }"
            shadow="hover"
          >
            <!-- 任务信息 -->
            <div class="task-header">
              <div class="task-info">
                <div class="task-title">
                  <el-icon :size="20" class="file-icon">
                    <Document />
                  </el-icon>
                  <span class="filename">{{ getFilename(task.local_path) }}</span>
                  <el-tag :type="getStatusType(task.status)" size="small">
                    {{ getStatusText(task.status) }}
                  </el-tag>
                </div>
                <div class="task-path">{{ task.remote_path }}</div>
              </div>

              <!-- 操作按钮 -->
              <div class="task-actions">
                <el-button
                  v-if="task.status === 'downloading'"
                  size="small"
                  @click="handlePause(task.id)"
                >
                  <el-icon><VideoPause /></el-icon>
                  暂停
                </el-button>
                <el-button
                  v-if="task.status === 'paused'"
                  size="small"
                  type="primary"
                  @click="handleResume(task.id)"
                >
                  <el-icon><VideoPlay /></el-icon>
                  继续
                </el-button>
                <el-button
                  v-if="task.status === 'completed'"
                  size="small"
                  type="success"
                  @click="openLocalFile(task.local_path)"
                >
                  <el-icon><FolderOpened /></el-icon>
                  打开文件夹
                </el-button>
                <el-button
                  size="small"
                  type="danger"
                  @click="handleDelete(task.id, task.status)"
                >
                  <el-icon><Delete /></el-icon>
                  删除
                </el-button>
              </div>
            </div>

            <!-- 进度条 -->
            <div class="task-progress">
              <el-progress
                :percentage="calculateProgress(task)"
                :status="getProgressStatus(task.status)"
                :stroke-width="8"
              >
                <template #default="{ percentage }">
                  <span class="progress-text">{{ percentage.toFixed(1) }}%</span>
                </template>
              </el-progress>
            </div>

            <!-- 下载统计 -->
            <div class="task-stats">
              <div class="stat-item">
                <span class="stat-label">已下载:</span>
                <span class="stat-value">{{ formatFileSize(task.downloaded_size) }}</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">总大小:</span>
                <span class="stat-value">{{ formatFileSize(task.total_size) }}</span>
              </div>
              <div class="stat-item" v-if="task.status === 'downloading'">
                <span class="stat-label">速度:</span>
                <span class="stat-value speed">{{ formatSpeed(task.speed) }}</span>
              </div>
              <div class="stat-item" v-if="task.status === 'downloading'">
                <span class="stat-label">剩余时间:</span>
                <span class="stat-value">{{ formatETA(calculateETA(task)) }}</span>
              </div>
              <div class="stat-item" v-if="task.error">
                <span class="stat-label error">错误:</span>
                <span class="stat-value error">{{ task.error }}</span>
              </div>
            </div>
          </el-card>
        </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  getAllDownloads,
  pauseDownload,
  resumeDownload,
  deleteDownload,
  clearCompleted,
  clearFailed,
  calculateProgress,
  calculateETA,
  formatFileSize,
  formatSpeed,
  formatETA,
  getStatusText,
  getStatusType,
  type DownloadTask,
  type TaskStatus,
} from '@/api/download'
import {
  Refresh,
  Document,
  VideoPause,
  VideoPlay,
  Delete,
  FolderOpened,
} from '@element-plus/icons-vue'

// 状态
const loading = ref(false)
const tasks = ref<DownloadTask[]>([])

// 自动刷新定时器
let refreshTimer: number | null = null

// 是否有活跃任务（需要实时刷新）
// 包括：downloading（正在下载）、paused（暂停，可能需要恢复）、pending（等待中，可能很快开始）
const hasActiveTasks = computed(() => {
  return tasks.value.some(t => 
    t.status === 'downloading' || 
    t.status === 'paused' || 
    t.status === 'pending'
  )
})

// 计算属性
const activeCount = computed(() => {
  return tasks.value.filter(t => t.status === 'downloading').length
})

const completedCount = computed(() => {
  return tasks.value.filter(t => t.status === 'completed').length
})

const failedCount = computed(() => {
  return tasks.value.filter(t => t.status === 'failed').length
})

const activeCountType = computed(() => {
  if (activeCount.value === 0) return 'info'
  if (activeCount.value <= 3) return 'success'
  return 'warning'
})

// 获取文件名
function getFilename(path: string): string {
  const parts = path.replace(/\\/g, '/').split('/')
  return parts[parts.length - 1] || path
}

// 获取进度条状态
function getProgressStatus(status: TaskStatus): 'success' | 'exception' | 'warning' | undefined {
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
    tasks.value = await getAllDownloads()
  } catch (error: any) {
    console.error('刷新任务列表失败:', error)
    // 请求失败时，清空任务列表，避免显示过时数据
    tasks.value = []
  } finally {
    loading.value = false
    // 无论成功还是失败，都要检查并更新自动刷新状态
    // 如果请求失败或没有活跃任务，应该停止定时器
    updateAutoRefresh()
  }
}

// 更新自动刷新状态
function updateAutoRefresh() {
  // 如果有活跃任务（正在下载、暂停或等待中），启动或保持定时刷新
  if (hasActiveTasks.value) {
    if (!refreshTimer) {
      console.log('启动自动刷新定时器，活跃任务数:', tasks.value.filter(t => 
        t.status === 'downloading' || t.status === 'paused' || t.status === 'pending'
      ).length)
      refreshTimer = window.setInterval(() => {
        refreshTasks()
      }, 100)
    }
  } else {
    // 没有活跃任务时，停止定时刷新
    if (refreshTimer) {
      console.log('停止自动刷新定时器，当前任务数:', tasks.value.length)
      clearInterval(refreshTimer)
      refreshTimer = null
    }
  }
}

// 暂停任务
async function handlePause(taskId: string) {
  try {
    await pauseDownload(taskId)
    ElMessage.success('任务已暂停')
    refreshTasks()
  } catch (error: any) {
    console.error('暂停任务失败:', error)
  }
}

// 恢复任务
async function handleResume(taskId: string) {
  try {
    await resumeDownload(taskId)
    ElMessage.success('任务已继续')
    refreshTasks()
  } catch (error: any) {
    console.error('恢复任务失败:', error)
  }
}

// 删除任务
async function handleDelete(taskId: string, status: TaskStatus) {
  const deleteFile = status === 'completed' || status === 'paused'

  try {
    if (deleteFile) {
      await ElMessageBox.confirm(
        '确定要删除此任务吗？',
        '删除确认',
        {
          confirmButtonText: '仅删除任务',
          cancelButtonText: '取消',
          distinguishCancelAndClose: true,
          type: 'warning',
          showClose: true,
          closeOnClickModal: false,
          closeOnPressEscape: false,
          customClass: 'delete-confirm-box',
          beforeClose: async (action, _instance, done) => {
            if (action === 'confirm') {
              // 仅删除任务
              await deleteDownload(taskId, false)
              ElMessage.success('任务已删除')
              refreshTasks()
              done()
            } else if (action === 'cancel') {
              done()
            }
          },
        }
      )
    } else {
      await ElMessageBox.confirm(
        '确定要删除此任务吗？',
        '删除确认',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
      )
      await deleteDownload(taskId, false)
      ElMessage.success('任务已删除')
      refreshTasks()
    }
  } catch (error: any) {
    if (error !== 'cancel' && error !== 'close') {
      console.error('删除任务失败:', error)
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

  &:hover {
    transform: translateY(-2px);
  }
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
</style>
