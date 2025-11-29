<template>
  <div class="uploads-container">
    <!-- 顶部工具栏 -->
    <div class="toolbar">
        <div class="header-left">
          <h2>上传管理</h2>
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

    <!-- 上传任务列表 -->
    <div class="task-container">
        <el-empty v-if="!loading && uploadItems.length === 0" description="暂无上传任务">
          <template #image>
            <el-icon :size="80" color="#909399"><Upload /></el-icon>
          </template>
          <template #description>
            <p>暂无上传任务</p>
            <p style="font-size: 12px; color: #909399;">
              前往「文件管理」页面点击"上传"按钮
            </p>
          </template>
        </el-empty>

        <div v-else class="task-list">
          <el-card
            v-for="item in uploadItems"
            :key="item.id"
            class="task-card"
            :class="{ 'task-active': item.status === 'uploading' }"
            shadow="hover"
          >
            <!-- 任务信息 -->
            <div class="task-header">
              <div class="task-info">
                <div class="task-title">
                  <el-icon :size="20" class="file-icon">
                    <Upload />
                  </el-icon>
                  <span class="filename">{{ getFilename(item.local_path) }}</span>
                  <el-tag :type="getStatusType(item.status)" size="small">
                    {{ getStatusText(item.status) }}
                  </el-tag>
                  <!-- 秒传标识 -->
                  <el-tag v-if="item.is_rapid_upload && item.status === 'completed'" type="success" size="small">
                    <el-icon><CircleCheck /></el-icon>
                    秒传
                  </el-tag>
                </div>
                <div class="task-path">
                  本地: {{ item.local_path }} → 网盘: {{ item.remote_path }}
                </div>
              </div>

              <!-- 操作按钮 -->
              <div class="task-actions">
                <el-button
                  v-if="item.status === 'uploading'"
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
                  v-if="item.status === 'failed'"
                  size="small"
                  type="warning"
                  @click="handleResume(item)"
                >
                  <el-icon><RefreshRight /></el-icon>
                  重试
                </el-button>
                <el-button
                  size="small"
                  type="danger"
                  @click="handleDelete(item)"
                >
                  <el-icon><Delete /></el-icon>
                  删除
                </el-button>
              </div>
            </div>

            <!-- 进度条 -->
            <div class="task-progress">
              <el-progress
                :percentage="calculateProgress(item)"
                :status="getProgressStatus(item.status)"
                :stroke-width="8"
              >
                <template #default="{ percentage }">
                  <span class="progress-text">{{ percentage.toFixed(1) }}%</span>
                </template>
              </el-progress>
            </div>

            <!-- 上传统计 -->
            <div class="task-stats">
              <div class="stat-item">
                <span class="stat-label">已上传:</span>
                <span class="stat-value">{{ formatFileSize(item.uploaded_size) }}</span>
              </div>
              <div class="stat-item">
                <span class="stat-label">总大小:</span>
                <span class="stat-value">{{ formatFileSize(item.total_size) }}</span>
              </div>
              <div class="stat-item" v-if="item.status === 'uploading'">
                <span class="stat-label">速度:</span>
                <span class="stat-value speed">{{ formatSpeed(item.speed) }}</span>
              </div>
              <div class="stat-item" v-if="item.status === 'uploading'">
                <span class="stat-label">剩余时间:</span>
                <span class="stat-value">{{ formatETA(calculateETA(item)) }}</span>
              </div>
              <div class="stat-item" v-if="item.error">
                <span class="stat-label error">错误:</span>
                <span class="stat-value error">{{ item.error }}</span>
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
  getAllUploads,
  pauseUpload,
  resumeUpload,
  deleteUpload,
  clearCompleted,
  clearFailed,
  calculateProgress,
  calculateETA,
  formatFileSize,
  formatSpeed,
  formatETA,
  getStatusText,
  getStatusType,
  extractFilename,
  type UploadTask,
  type UploadTaskStatus,
} from '@/api/upload'
import {
  Refresh,
  Upload,
  VideoPause,
  VideoPlay,
  Delete,
  CircleCheck,
  RefreshRight,
} from '@element-plus/icons-vue'

// 状态
const loading = ref(false)
const uploadItems = ref<UploadTask[]>([])

// 自动刷新定时器
let refreshTimer: number | null = null

// 是否有活跃任务（需要实时刷新）
const hasActiveTasks = computed(() => {
  return uploadItems.value.some(item =>
    item.status === 'uploading' || item.status === 'pending'
  )
})

// 计算属性
const activeCount = computed(() => {
  return uploadItems.value.filter(item => item.status === 'uploading').length
})

const completedCount = computed(() => {
  return uploadItems.value.filter(item => item.status === 'completed').length
})

const failedCount = computed(() => {
  return uploadItems.value.filter(item => item.status === 'failed').length
})

const activeCountType = computed(() => {
  if (activeCount.value === 0) return 'info'
  if (activeCount.value <= 3) return 'success'
  return 'warning'
})

// 获取文件名
function getFilename(path: string): string {
  return extractFilename(path)
}

// 获取进度条状态
function getProgressStatus(status: UploadTaskStatus): 'success' | 'exception' | 'warning' | undefined {
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
    uploadItems.value = await getAllUploads()
  } catch (error: any) {
    console.error('刷新任务列表失败:', error)
    // 请求失败时，清空任务列表，避免显示过时数据
    uploadItems.value = []
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
      }, 1000) // 上传每秒刷新一次
    }
  } else {
    // 没有活跃任务时，停止定时刷新
    if (refreshTimer) {
      console.log('停止自动刷新定时器，当前任务数:', uploadItems.value.length)
      clearInterval(refreshTimer)
      refreshTimer = null
    }
  }
}

// 暂停任务
async function handlePause(item: UploadTask) {
  try {
    await pauseUpload(item.id)
    ElMessage.success('任务已暂停')
    refreshTasks()
  } catch (error: any) {
    console.error('暂停任务失败:', error)
  }
}

// 恢复任务
async function handleResume(item: UploadTask) {
  try {
    await resumeUpload(item.id)
    ElMessage.success(item.status === 'failed' ? '任务正在重试' : '任务已继续')
    refreshTasks()
  } catch (error: any) {
    console.error('恢复任务失败:', error)
  }
}

// 删除任务
async function handleDelete(item: UploadTask) {
  try {
    await ElMessageBox.confirm(
      '确定要删除此上传任务吗？',
      '删除确认',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )

    await deleteUpload(item.id)
    ElMessage.success('任务已删除')
    refreshTasks()
  } catch (error: any) {
    if (error !== 'cancel') {
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

// 组件挂载时加载任务列表
onMounted(() => {
  refreshTasks()
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
.uploads-container {
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
    border-color: #67c23a;
    box-shadow: 0 2px 12px rgba(103, 194, 58, 0.2);
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
    color: #67c23a;
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
