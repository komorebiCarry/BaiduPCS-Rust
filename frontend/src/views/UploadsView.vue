<template>
  <div class="uploads-container" :class="{ 'is-mobile': isMobile }">
    <!-- 顶部工具栏 -->
    <div class="toolbar">
      <div class="header-left">
        <h2 v-if="!isMobile">上传管理</h2>
        <el-tag :type="activeCountType" size="large">
          {{ activeCount }} 个任务进行中
        </el-tag>
      </div>
      <div class="header-right">
        <el-button @click="refreshTasks" :circle="isMobile">
          <el-icon><Refresh /></el-icon>
          <span v-if="!isMobile">刷新</span>
        </el-button>
        <el-dropdown @command="handleBatchCommand" trigger="click">
          <el-button>
            批量操作
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="pause" :disabled="activeCount === 0">
                <el-icon><VideoPause /></el-icon>
                全部暂停 ({{ activeCount }})
              </el-dropdown-item>
              <el-dropdown-item command="resume" :disabled="pausedCount === 0">
                <el-icon><VideoPlay /></el-icon>
                全部继续 ({{ pausedCount }})
              </el-dropdown-item>
              <el-dropdown-item command="clearCompleted" :disabled="completedCount === 0" divided>
                <el-icon><Delete /></el-icon>
                清除已完成 ({{ completedCount }})
              </el-dropdown-item>
              <el-dropdown-item command="clearFailed" :disabled="failedCount === 0">
                <el-icon><Delete /></el-icon>
                清除失败 ({{ failedCount }})
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
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
            前往「网盘管理」页面点击"上传"按钮
          </p>
        </template>
      </el-empty>

      <div v-else class="task-list">
        <el-card
            v-for="item in uploadItems"
            :key="item.id"
            class="task-card"
            :class="{ 'task-active': item.status === 'uploading' || item.status === 'encrypting' }"
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
                <!-- 加密标识（已完成的加密任务） -->
                <el-tag v-if="item.encrypt_enabled && (item.status === 'completed' || item.status === 'rapid_upload_success')" type="info" size="small">
                  <el-icon><Lock /></el-icon>
                  已加密
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

          <!-- 加密进度显示 -->
          <div v-if="item.status === 'encrypting'" class="encrypt-progress">
            <div class="encrypt-header">
              <el-icon class="encrypt-icon"><Lock /></el-icon>
              <span>正在加密文件...</span>
            </div>
            <el-progress
                :percentage="item.encrypt_progress || 0"
                :stroke-width="6"
                status="warning"
            >
              <template #default="{ percentage }">
                <span class="progress-text">{{ percentage.toFixed(1) }}%</span>
              </template>
            </el-progress>
          </div>

          <!-- 进度条 -->
          <div class="task-progress" v-if="item.status !== 'encrypting'">
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
  batchPauseUploads,
  batchResumeUploads,
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
  Lock,
  ArrowDown,
} from '@element-plus/icons-vue'
import {useIsMobile} from '@/utils/responsive'
// 🔥 WebSocket 相关导入
import { getWebSocketClient, connectWebSocket, type ConnectionState } from '@/utils/websocket'
import type { UploadEvent } from '@/types/events'

// 响应式检测
const isMobile = useIsMobile()

// 状态
const loading = ref(false)
const uploadItems = ref<UploadTask[]>([])

// 自动刷新定时器
let refreshTimer: number | null = null
// 🔥 WebSocket 事件订阅清理函数
let unsubscribeUpload: (() => void) | null = null
let unsubscribeConnectionState: (() => void) | null = null
// 🔥 WebSocket 连接状态
const wsConnected = ref(false)

// 是否有活跃任务（需要实时刷新）
const hasActiveTasks = computed(() => {
  return uploadItems.value.some(item =>
      item.status === 'uploading' || item.status === 'pending' || item.status === 'encrypting' || item.status === 'checking_rapid'
  )
})

// 计算属性
const activeCount = computed(() => {
  return uploadItems.value.filter(item => item.status === 'uploading' || item.status === 'encrypting').length
})

const completedCount = computed(() => {
  return uploadItems.value.filter(item => item.status === 'completed').length
})

const failedCount = computed(() => {
  return uploadItems.value.filter(item => item.status === 'failed').length
})

const pausedCount = computed(() => {
  return uploadItems.value.filter(item => item.status === 'paused').length
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
  if (status === 'completed' || status === 'rapid_upload_success') return 'success'
  if (status === 'failed') return 'exception'
  if (status === 'paused') return 'warning'
  if (status === 'encrypting') return 'warning'
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
  // 🔥 如果 WebSocket 已连接，不使用轮询（由 WebSocket 推送更新）
  if (wsConnected.value) {
    if (refreshTimer) {
      console.log('[UploadsView] WebSocket 已连接，停止轮询')
      clearInterval(refreshTimer)
      refreshTimer = null
    }
    return
  }

  // 🔥 WebSocket 未连接时，回退到轮询模式
  if (hasActiveTasks.value) {
    if (!refreshTimer) {
      console.log('[UploadsView] WebSocket 未连接，启动轮询模式，活跃任务数:', activeCount.value)
      refreshTimer = window.setInterval(() => {
        refreshTasks()
      }, 1000)
    }
  } else {
    if (refreshTimer) {
      console.log('[UploadsView] 停止轮询，当前任务数:', uploadItems.value.length)
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

// 批量操作命令分发
function handleBatchCommand(command: string) {
  switch (command) {
    case 'pause': handleBatchPause(); break
    case 'resume': handleBatchResume(); break
    case 'clearCompleted': handleClearCompleted(); break
    case 'clearFailed': handleClearFailed(); break
  }
}

// 全部暂停
async function handleBatchPause() {
  try {
    const res = await batchPauseUploads({ all: true })
    ElMessage.success(`已暂停 ${res.success_count} 个任务`)
    refreshTasks()
  } catch (error: any) {
    console.error('批量暂停失败:', error)
  }
}

// 全部继续
async function handleBatchResume() {
  try {
    const res = await batchResumeUploads({ all: true })
    ElMessage.success(`已恢复 ${res.success_count} 个任务`)
    refreshTasks()
  } catch (error: any) {
    console.error('批量恢复失败:', error)
  }
}

// 🔥 处理上传事件
function handleUploadEvent(event: UploadEvent) {
  console.log('[UploadsView] 收到上传事件:', event.event_type, event.task_id)

  switch (event.event_type) {
    case 'created':
      // 新任务创建，刷新列表
      refreshTasks()
      break
    case 'progress':
      // 进度更新
      const progressIdx = uploadItems.value.findIndex(t => t.id === event.task_id)
      if (progressIdx !== -1) {
        uploadItems.value[progressIdx].uploaded_size = event.uploaded_size
        uploadItems.value[progressIdx].total_size = event.total_size
        uploadItems.value[progressIdx].speed = event.speed
        if (event.completed_chunks !== undefined) {
          uploadItems.value[progressIdx].completed_chunks = event.completed_chunks
        }
        if (event.total_chunks !== undefined) {
          uploadItems.value[progressIdx].total_chunks = event.total_chunks
        }
        // 🔥 如果当前是加密状态，收到传输进度后自动切换为上传状态
        if (uploadItems.value[progressIdx].status === 'encrypting') {
          uploadItems.value[progressIdx].status = 'uploading'
        }
      }
      break
    case 'encrypt_progress':
      // 🔥 加密进度更新
      const encryptIdx = uploadItems.value.findIndex(t => t.id === event.task_id)
      if (encryptIdx !== -1) {
        uploadItems.value[encryptIdx].encrypt_progress = event.encrypt_progress
        uploadItems.value[encryptIdx].status = 'encrypting'
      }
      break
    case 'encrypt_completed':
      // 🔥 加密完成
      const encryptCompletedIdx = uploadItems.value.findIndex(t => t.id === event.task_id)
      if (encryptCompletedIdx !== -1) {
        uploadItems.value[encryptCompletedIdx].encrypt_progress = 100
        uploadItems.value[encryptCompletedIdx].original_size = event.original_size
        // 🔥 直接更新状态为 uploading，避免依赖 status_changed 事件导致状态不同步
        uploadItems.value[encryptCompletedIdx].status = 'uploading'
      }
      break
    case 'status_changed':
      // 状态变更
      const statusIdx = uploadItems.value.findIndex(t => t.id === event.task_id)
      if (statusIdx !== -1) {
        uploadItems.value[statusIdx].status = event.new_status as UploadTaskStatus
      }
      break
    case 'completed':
    case 'failed':
      // 完成或失败，刷新列表获取最终状态
      refreshTasks()
      break
    case 'paused':
      // 任务暂停，直接更新状态
      const pausedIdx = uploadItems.value.findIndex(t => t.id === event.task_id)
      if (pausedIdx !== -1) {
        uploadItems.value[pausedIdx].status = 'paused'
        uploadItems.value[pausedIdx].speed = 0
      }
      break
    case 'resumed':
      // 任务恢复，直接更新状态为 uploading
      const resumedIdx = uploadItems.value.findIndex(t => t.id === event.task_id)
      if (resumedIdx !== -1) {
        // 🔥 设为 uploading 而不是 pending，这样 UI 会显示速度和剩余时间
        // 后续的 progress 事件会更新实际的速度值
        uploadItems.value[resumedIdx].status = 'uploading'
      }
      break
    case 'deleted':
      uploadItems.value = uploadItems.value.filter(t => t.id !== event.task_id)
      break
  }
}

// 🔥 设置 WebSocket 订阅
function setupWebSocketSubscriptions() {
  const wsClient = getWebSocketClient()

  // 🔥 订阅服务端上传事件
  wsClient.subscribe(['upload:*'])

  unsubscribeUpload = wsClient.onUploadEvent(handleUploadEvent)

  unsubscribeConnectionState = wsClient.onConnectionStateChange((state: ConnectionState) => {
    const wasConnected = wsConnected.value
    wsConnected.value = state === 'connected'

    console.log('[UploadsView] WebSocket 状态变化:', state, ', 是否连接:', wsConnected.value)

    // 🔥 任何状态变化都检查轮询策略（包括 connecting 状态）
    updateAutoRefresh()

    // 🔥 WebSocket 重新连接成功时，刷新一次获取最新数据
    if (!wasConnected && wsConnected.value) {
      refreshTasks()
    }
  })

  connectWebSocket()
  console.log('[UploadsView] WebSocket 订阅已设置')
}

// 🔥 清理 WebSocket 订阅
function cleanupWebSocketSubscriptions() {
  const wsClient = getWebSocketClient()

  // 🔥 取消服务端订阅
  wsClient.unsubscribe(['upload:*'])

  if (unsubscribeUpload) {
    unsubscribeUpload()
    unsubscribeUpload = null
  }
  if (unsubscribeConnectionState) {
    unsubscribeConnectionState()
    unsubscribeConnectionState = null
  }
  console.log('[UploadsView] WebSocket 订阅已清理')
}

// 组件挂载时加载任务列表
onMounted(() => {
  refreshTasks()
  setupWebSocketSubscriptions()
})

// 组件卸载时清除定时器
onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  cleanupWebSocketSubscriptions()
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

// =====================
// 加密进度样式
// =====================
.encrypt-progress {
  margin-bottom: 15px;
  padding: 10px;
  background: #fdf6ec;
  border-radius: 4px;

  .encrypt-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
    color: #e6a23c;
    font-size: 13px;

    .encrypt-icon {
      animation: pulse 1.5s infinite;
    }
  }

  .progress-text {
    font-size: 12px;
    font-weight: 500;
  }
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}

// =====================
// 移动端样式
// =====================
.is-mobile {
  // 移动端高度适配（减去顶部栏60px和底部导航栏56px）
  height: calc(100vh - 60px - 56px);

  .toolbar {
    padding: 12px 16px;

    .header-left {
      gap: 12px;
    }
  }

  .task-container {
    padding: 12px;
  }

  .task-list {
    gap: 10px;
  }

  .task-header {
    flex-direction: column;
    gap: 12px;
  }

  .task-actions {
    margin-left: 0;
    flex-wrap: wrap;
  }

  .task-title {
    flex-wrap: wrap;

    .filename {
      font-size: 14px;
      max-width: 100%;
    }
  }

  .task-path {
    padding-left: 0;
    word-break: break-all;
    white-space: normal;
  }

  .task-stats {
    gap: 12px;

    .stat-item {
      font-size: 12px;
    }
  }
}
</style>
