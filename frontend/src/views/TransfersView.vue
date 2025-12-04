<template>
  <div class="transfers-container">
    <!-- 顶部工具栏 -->
    <div class="toolbar">
      <div class="header-left">
        <h2>转存管理</h2>
        <el-tag :type="activeCountType" size="large">
          {{ activeCount }} 个任务进行中
        </el-tag>
      </div>
      <div class="header-right">
        <el-button type="primary" @click="showTransferDialog = true">
          <el-icon><Share /></el-icon>
          新建转存
        </el-button>
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

    <!-- 转存任务列表 -->
    <div class="task-container">
      <el-empty v-if="!loading && tasks.length === 0" description="暂无转存任务">
        <el-button type="primary" @click="showTransferDialog = true">
          <el-icon><Share /></el-icon>
          新建转存
        </el-button>
      </el-empty>

      <div v-else class="task-list">
        <el-card
          v-for="task in tasks"
          :key="task.id"
          class="task-card"
          :class="{ 'task-active': isActiveStatus(task.status) }"
          shadow="hover"
        >
          <!-- 任务信息 -->
          <div class="task-header">
            <div class="task-info">
              <div class="task-title">
                <el-icon :size="20" class="share-icon"><Share /></el-icon>
                <el-tooltip :content="task.share_url" placement="top" :show-after="500">
                  <span class="share-url">
                    {{ getTaskDisplayName(task) }}
                  </span>
                </el-tooltip>
                <el-tag :type="getTransferStatusType(task.status)" size="small">
                  {{ getTransferStatusText(task.status) }}
                </el-tag>
              </div>
              <div class="task-path">
                <span class="path-label">保存到:</span>
                {{ task.save_path }}
              </div>
            </div>

            <!-- 操作按钮 -->
            <div class="task-actions">
              <el-button
                v-if="!isTerminalStatus(task.status)"
                size="small"
                type="danger"
                @click="handleCancel(task)"
              >
                <el-icon><CircleClose /></el-icon>
                取消
              </el-button>
              <el-button
                size="small"
                type="danger"
                plain
                @click="handleDelete(task)"
              >
                <el-icon><Delete /></el-icon>
                删除
              </el-button>
            </div>
          </div>

          <!-- 进度条 -->
          <div class="task-progress" v-if="task.total_count > 0">
            <el-progress
              :percentage="calculateTransferProgress(task)"
              :status="getProgressStatus(task.status)"
              :stroke-width="8"
            >
              <template #default="{ percentage }">
                <span class="progress-text">{{ percentage.toFixed(1) }}%</span>
              </template>
            </el-progress>
          </div>

          <!-- 任务统计 -->
          <div class="task-stats">
            <div class="stat-item" v-if="task.total_count > 0">
              <span class="stat-label">进度:</span>
              <span class="stat-value">{{ task.transferred_count }}/{{ task.total_count }} 个文件</span>
            </div>
            <div class="stat-item" v-if="task.auto_download">
              <span class="stat-label">自动下载:</span>
              <span class="stat-value">
                <el-tag type="success" size="small">已开启</el-tag>
              </span>
            </div>
            <div class="stat-item" v-if="task.download_task_ids.length > 0">
              <span class="stat-label">下载任务:</span>
              <span class="stat-value">{{ task.download_task_ids.length }} 个</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">创建时间:</span>
              <span class="stat-value">{{ formatTransferTime(task.created_at) }}</span>
            </div>
            <div class="stat-item" v-if="task.error">
              <span class="stat-label error">错误:</span>
              <span class="stat-value error">{{ task.error }}</span>
            </div>
          </div>

          <!-- 文件列表（可展开） -->
          <el-collapse v-if="task.file_list.length > 0" class="file-collapse">
            <el-collapse-item :title="`文件列表 (${task.file_list.length} 个)`" name="files">
              <div class="file-list">
                <div
                  v-for="file in task.file_list"
                  :key="file.fs_id"
                  class="file-item"
                >
                  <el-icon>
                    <Folder v-if="file.is_dir" />
                    <Document v-else />
                  </el-icon>
                  <span class="file-name">{{ file.name }}</span>
                  <span class="file-size" v-if="!file.is_dir">
                    {{ formatFileSize(file.size) }}
                  </span>
                </div>
              </div>
            </el-collapse-item>
          </el-collapse>
        </el-card>
      </div>
    </div>

    <!-- 新建转存对话框 -->
    <TransferDialog
      v-model="showTransferDialog"
      @success="handleTransferSuccess"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Share,
  Refresh,
  Delete,
  CircleClose,
  Folder,
  Document,
} from '@element-plus/icons-vue'
import {
  getAllTransfers,
  deleteTransfer,
  cancelTransfer,
  getTransferStatusText,
  getTransferStatusType,
  calculateTransferProgress,
  isTerminalStatus,
  formatTransferTime,
  type TransferTask,
  type TransferStatus,
} from '@/api/transfer'
import { formatFileSize } from '@/api/file'
import TransferDialog from '@/components/TransferDialog.vue'

// 状态
const loading = ref(false)
const tasks = ref<TransferTask[]>([])
const showTransferDialog = ref(false)

// 自动刷新定时器
let refreshTimer: number | null = null

// 是否为活跃状态
function isActiveStatus(status: TransferStatus): boolean {
  return ['queued', 'checking_share', 'transferring', 'downloading'].includes(status)
}

// 获取任务显示名称（优先显示文件名）
function getTaskDisplayName(task: TransferTask): string {
  if (task.file_list.length === 0) {
    // 还没有获取到文件列表，显示简短链接
    const match = task.share_url.match(/\/s\/([a-zA-Z0-9_-]+)/)
    if (match) {
      return `pan.baidu.com/s/${match[1].substring(0, 8)}...`
    }
    return task.share_url.length > 40 ? task.share_url.substring(0, 37) + '...' : task.share_url
  }

  if (task.file_list.length === 1) {
    // 只有一个文件，显示文件名
    return task.file_list[0].name
  }

  // 多个文件，显示第一个文件名 + 数量
  return `${task.file_list[0].name} 等 ${task.file_list.length} 个文件`
}

// 是否有活跃任务
const hasActiveTasks = computed(() => {
  return tasks.value.some(task => isActiveStatus(task.status))
})

// 计算属性
const activeCount = computed(() => {
  return tasks.value.filter(task => isActiveStatus(task.status)).length
})

const completedCount = computed(() => {
  return tasks.value.filter(task =>
    task.status === 'completed' || task.status === 'transferred'
  ).length
})

const failedCount = computed(() => {
  return tasks.value.filter(task =>
    task.status === 'transfer_failed' || task.status === 'download_failed'
  ).length
})

const activeCountType = computed(() => {
  if (activeCount.value === 0) return 'info'
  if (activeCount.value <= 3) return 'success'
  return 'warning'
})

// 获取进度条状态
function getProgressStatus(status: TransferStatus): 'success' | 'exception' | 'warning' | undefined {
  if (status === 'completed' || status === 'transferred') return 'success'
  if (status === 'transfer_failed' || status === 'download_failed') return 'exception'
  return undefined
}

// 刷新任务列表
async function refreshTasks() {
  if (loading.value) return

  loading.value = true
  try {
    const response = await getAllTransfers()
    tasks.value = response.tasks
  } catch (error: any) {
    console.error('刷新转存任务列表失败:', error)
    tasks.value = []
  } finally {
    loading.value = false
    updateAutoRefresh()
  }
}

// 更新自动刷新状态
function updateAutoRefresh() {
  if (hasActiveTasks.value) {
    if (!refreshTimer) {
      console.log('启动转存任务自动刷新')
      refreshTimer = window.setInterval(() => {
        refreshTasks()
      }, 2000) // 每2秒刷新
    }
  } else {
    if (refreshTimer) {
      console.log('停止转存任务自动刷新')
      clearInterval(refreshTimer)
      refreshTimer = null
    }
  }
}

// 取消任务
async function handleCancel(task: TransferTask) {
  try {
    await ElMessageBox.confirm(
      '确定要取消此转存任务吗？',
      '取消确认',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )

    await cancelTransfer(task.id)
    ElMessage.success('任务已取消')
    refreshTasks()
  } catch (error: any) {
    if (error !== 'cancel') {
      console.error('取消任务失败:', error)
      ElMessage.error('取消任务失败: ' + (error.message || error))
    }
  }
}

// 删除任务
async function handleDelete(task: TransferTask) {
  try {
    await ElMessageBox.confirm(
      '确定要删除此转存任务吗？',
      '删除确认',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )

    await deleteTransfer(task.id)
    ElMessage.success('任务已删除')
    refreshTasks()
  } catch (error: any) {
    if (error !== 'cancel') {
      console.error('删除任务失败:', error)
      ElMessage.error('删除任务失败: ' + (error.message || error))
    }
  }
}

// 清除已完成
async function handleClearCompleted() {
  try {
    await ElMessageBox.confirm(
      `确定要清除所有已完成的转存任务吗？（共${completedCount.value}个）`,
      '批量清除',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )

    // 逐个删除已完成的任务
    const completedTasks = tasks.value.filter(task =>
      task.status === 'completed' || task.status === 'transferred'
    )

    let successCount = 0
    for (const task of completedTasks) {
      try {
        await deleteTransfer(task.id)
        successCount++
      } catch (error) {
        console.error('删除任务失败:', task.id, error)
      }
    }

    ElMessage.success(`已清除 ${successCount} 个任务`)
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
      `确定要清除所有失败的转存任务吗？（共${failedCount.value}个）`,
      '批量清除',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )

    // 逐个删除失败的任务
    const failedTasks = tasks.value.filter(task =>
      task.status === 'transfer_failed' || task.status === 'download_failed'
    )

    let successCount = 0
    for (const task of failedTasks) {
      try {
        await deleteTransfer(task.id)
        successCount++
      } catch (error) {
        console.error('删除任务失败:', task.id, error)
      }
    }

    ElMessage.success(`已清除 ${successCount} 个任务`)
    refreshTasks()
  } catch (error: any) {
    if (error !== 'cancel') {
      console.error('清除失败任务失败:', error)
    }
  }
}

// 转存成功回调
function handleTransferSuccess(taskId: string) {
  console.log('转存任务创建成功:', taskId)
  refreshTasks()
}

// 组件挂载
onMounted(() => {
  refreshTasks()
})

// 组件卸载
onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
})
</script>

<style scoped lang="scss">
.transfers-container {
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

  .share-icon {
    flex-shrink: 0;
    color: #409eff;
  }

  .share-url {
    font-size: 16px;
    font-weight: 500;
    color: #333;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    cursor: pointer;

    &:hover {
      color: #409eff;
    }
  }
}

.task-path {
  font-size: 12px;
  color: #999;
  padding-left: 30px;

  .path-label {
    color: #666;
    margin-right: 4px;
  }
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

      &.error {
        color: #f56c6c;
      }
    }
  }
}

.file-collapse {
  margin-top: 15px;
  border-top: 1px solid #ebeef5;
  padding-top: 10px;

  :deep(.el-collapse-item__header) {
    font-size: 13px;
    color: #666;
  }
}

.file-list {
  max-height: 200px;
  overflow-y: auto;
}

.file-item {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 0;
  font-size: 13px;
  color: #606266;

  .el-icon {
    color: #909399;
  }

  .file-name {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .file-size {
    color: #909399;
    font-size: 12px;
  }
}

:deep(.el-progress__text) {
  font-size: 12px !important;
}
</style>
