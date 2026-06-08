<template>
  <div class="transfers-container" :class="{ 'is-mobile': isMobile }">
    <!-- 顶部工具栏 -->
    <div class="toolbar">
      <div class="header-left">
        <h2 v-if="!isMobile">转存管理</h2>
        <el-tag :type="activeCountType" size="large">
          {{ activeCount }} 个任务进行中
        </el-tag>
        <!-- 多账号过滤器 -->
        <AccountFilter
            v-if="authStore.hasMultipleAccounts"
            v-model="ownerFilter"
            :counts="ownerFilterCounts"
            size="large"
            class="account-filter-slot"
        />
      </div>
      <div class="header-right">
        <template v-if="!isMobile">
          <el-button type="primary" @click="showTransferDialog = true">
            <el-icon><Share /></el-icon>
            新建转存
          </el-button>
          <el-button type="success" @click="showShareDirectDialog = true">
            <el-icon><Download /></el-icon>
            分享直下
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
        </template>
        <template v-else>
          <el-button type="primary" circle @click="showTransferDialog = true">
            <el-icon><Share /></el-icon>
          </el-button>
          <el-button type="success" circle @click="showShareDirectDialog = true">
            <el-icon><Download /></el-icon>
          </el-button>
          <el-button circle @click="refreshTasks">
            <el-icon><Refresh /></el-icon>
          </el-button>
          <el-button circle @click="handleClearCompleted" :disabled="completedCount === 0">
            <el-icon><Delete /></el-icon>
          </el-button>
        </template>
      </div>
    </div>

    <!-- 转存任务列表 -->
    <div class="task-container">
      <el-empty
          v-if="!loading && displayedTasks.length === 0"
          :description="ownerFilter === null ? '暂无转存任务' : '当前过滤条件下暂无转存任务'"
      >
        <el-button v-if="ownerFilter === null" type="primary" @click="showTransferDialog = true">
          <el-icon><Share /></el-icon>
          新建转存
        </el-button>
      </el-empty>

      <div v-else class="task-list">
        <el-card
            v-for="task in displayedTasks"
            :key="task.id"
            :data-task-id="task.id"
            class="task-card"
            :class="{ 'task-active': isActiveStatus(task.status), 'task-highlighted': highlightIds.has(task.id) }"
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
                <el-tag v-if="task.is_share_direct_download" type="success" size="small">
                  直下
                </el-tag>
                <el-tag :type="getTransferStatusType(task.status)" size="small">
                  {{ getTransferStatusText(task.status) }}
                </el-tag>
                <!-- 多账号 chip -->
                <AccountBadge :owner-uid="task.owner_uid" size="small" class="task-account-badge" />
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
              <span class="stat-value">
                {{ task.download_task_ids.length }} 个
                <el-button
                    size="small"
                    type="primary"
                    link
                    @click="goToDownloadTasks(task.download_task_ids)"
                    style="margin-left: 8px"
                >
                  <el-icon><Document /></el-icon>
                  查看下载
                </el-button>
              </span>
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

    <!-- 分享直下对话框 -->
    <ShareDirectDownloadDialog
        v-model="showShareDirectDialog"
        @success="handleTransferSuccess"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, nextTick } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Share,
  Refresh,
  Delete,
  CircleClose,
  Folder,
  Document,
  Download,
} from '@element-plus/icons-vue'
import { useRouter, useRoute } from 'vue-router'
import { useIsMobile } from '@/utils/responsive'
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
import ShareDirectDownloadDialog from '@/components/ShareDirectDownloadDialog.vue'
// 🔥 WebSocket 相关导入
import { getWebSocketClient, connectWebSocket, type ConnectionState } from '@/utils/websocket'
import type { TransferEvent } from '@/types/events'
// 多账号集成
import { useAuthStore } from '@/stores/auth'
import AccountFilter from '@/components/AccountFilter.vue'
import AccountBadge from '@/components/AccountBadge.vue'

// 路由
const router = useRouter()
const route = useRoute()

// 响应式检测
const isMobile = useIsMobile()

// 高亮的转存任务 ID 集合（从下载页跳转过来时使用）
const highlightIds = ref<Set<string>>(new Set())

// 状态
const loading = ref(false)
const tasks = ref<TransferTask[]>([])
const showTransferDialog = ref(false)
const showShareDirectDialog = ref(false)

// 多账号状态
const authStore = useAuthStore()
const ownerFilter = ref<number | null>(null)

const displayedTasks = computed(() => {
  if (ownerFilter.value === null) return tasks.value
  return tasks.value.filter((t) => t.owner_uid === ownerFilter.value)
})

const ownerFilterCounts = computed(() => {
  const map: Record<number, number> = {}
  for (const t of tasks.value) {
    const uid = typeof t.owner_uid === 'number' ? t.owner_uid : null
    if (uid !== null) map[uid] = (map[uid] || 0) + 1
  }
  return map
})

// 自动刷新定时器
let refreshTimer: number | null = null
// 🔥 WebSocket 事件订阅清理函数
let unsubscribeTransfer: (() => void) | null = null
let unsubscribeConnectionState: (() => void) | null = null
// 🔥 WebSocket 连接状态
const wsConnected = ref(false)

// 是否为活跃状态
function isActiveStatus(status: TransferStatus): boolean {
  return ['queued', 'checking_share', 'transferring', 'downloading', 'cleaning'].includes(status)
}

// 获取任务显示名称（优先显示文件名）
function getTaskDisplayName(task: TransferTask): string {
  // 🔥 优先使用后端返回的 file_name 字段（历史任务也能正确显示）
  if (task.file_name) {
    return task.file_name
  }

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
  // 🔥 如果 WebSocket 已连接，不使用轮询（由 WebSocket 推送更新）
  if (wsConnected.value) {
    if (refreshTimer) {
      console.log('[TransfersView] WebSocket 已连接，停止轮询')
      clearInterval(refreshTimer)
      refreshTimer = null
    }
    return
  }

  // 🔥 WebSocket 未连接时，回退到轮询模式
  if (hasActiveTasks.value) {
    if (!refreshTimer) {
      console.log('[TransfersView] WebSocket 未连接，启动轮询模式，活跃任务数:', activeCount.value)
      refreshTimer = window.setInterval(() => {
        refreshTasks()
      }, 2000)
    }
  } else {
    if (refreshTimer) {
      console.log('[TransfersView] 停止轮询，当前任务数:', tasks.value.length)
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

// 🔥 跳转到关联的下载任务（支持多个）
function goToDownloadTasks(downloadTaskIds: string[]) {
  router.push({
    name: 'Downloads',
    query: { highlight: downloadTaskIds.join(',') }
  })
}

// 🔥 处理转存事件
function handleTransferEvent(event: TransferEvent) {
  console.log('[TransfersView] 收到转存事件:', event.event_type, event.task_id)

  switch (event.event_type) {
    case 'created':
      // 新任务创建，刷新列表
      refreshTasks()
      break
    case 'status_changed':
      // 状态变更
      const statusIdx = tasks.value.findIndex(t => t.id === event.task_id)
      if (statusIdx !== -1) {
        tasks.value[statusIdx].status = event.new_status as TransferStatus
      }
      break
    case 'completed':
    case 'failed':
      // 完成或失败，刷新列表
      refreshTasks()
      break
    case 'deleted':
      // 任务删除
      tasks.value = tasks.value.filter(t => t.id !== event.task_id)
      break
  }
}

// 🔥 设置 WebSocket 订阅
function setupWebSocketSubscriptions() {
  const wsClient = getWebSocketClient()

  // 🔥 订阅服务端转存事件
  wsClient.subscribe(['transfer:*'])

  unsubscribeTransfer = wsClient.onTransferEvent(handleTransferEvent)

  unsubscribeConnectionState = wsClient.onConnectionStateChange((state: ConnectionState) => {
    const wasConnected = wsConnected.value
    wsConnected.value = state === 'connected'

    console.log('[TransfersView] WebSocket 状态变化:', state, ', 是否连接:', wsConnected.value)

    // 🔥 任何状态变化都检查轮询策略（包括 connecting 状态）
    updateAutoRefresh()

    // 🔥 WebSocket 重新连接成功时，刷新一次获取最新数据
    if (!wasConnected && wsConnected.value) {
      refreshTasks()
    }
  })

  connectWebSocket()
  console.log('[TransfersView] WebSocket 订阅已设置')
}

// 🔥 清理 WebSocket 订阅
function cleanupWebSocketSubscriptions() {
  const wsClient = getWebSocketClient()

  // 🔥 取消服务端订阅
  wsClient.unsubscribe(['transfer:*'])

  if (unsubscribeTransfer) {
    unsubscribeTransfer()
    unsubscribeTransfer = null
  }
  if (unsubscribeConnectionState) {
    unsubscribeConnectionState()
    unsubscribeConnectionState = null
  }
  console.log('[TransfersView] WebSocket 订阅已清理')
}

// 组件挂载
onMounted(async () => {
  // 解析 highlight 参数（从下载页跳转过来时）
  const highlightParam = route.query.highlight as string | undefined
  if (highlightParam) {
    highlightIds.value = new Set(highlightParam.split(',').filter(Boolean))
  }

  await refreshTasks()

  // 高亮任务加载完成后滚动到第一个高亮任务
  if (highlightIds.value.size > 0) {
    nextTick(() => {
      const firstId = [...highlightIds.value][0]
      const el = document.querySelector(`[data-task-id="${firstId}"]`)
      if (el) {
        el.scrollIntoView({ behavior: 'smooth', block: 'center' })
      }
      // 3 秒后清除高亮
      setTimeout(() => {
        highlightIds.value = new Set()
        router.replace({ query: {} })
      }, 3000)
    })
  }

  setupWebSocketSubscriptions()
  window.addEventListener('multi-account:active-changed', handleActiveChanged)
})

// 多账号切换处理器
function handleActiveChanged() {
  refreshTasks()
}

// 组件卸载
onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  cleanupWebSocketSubscriptions()
  window.removeEventListener('multi-account:active-changed', handleActiveChanged)
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

  &.task-highlighted {
    border-color: #e6a23c;
    box-shadow: 0 2px 16px rgba(230, 162, 60, 0.35);
    animation: highlight-fade 3s ease-out;
  }

  @keyframes highlight-fade {
    0% { box-shadow: 0 2px 16px rgba(230, 162, 60, 0.5); }
    70% { box-shadow: 0 2px 16px rgba(230, 162, 60, 0.35); }
    100% { box-shadow: none; border-color: transparent; }
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

    .share-url {
      font-size: 14px;
      max-width: 100%;
    }
  }

  .task-path {
    padding-left: 0;
  }

  .task-stats {
    gap: 12px;

    .stat-item {
      font-size: 12px;
    }
  }

  .file-collapse {
    :deep(.el-collapse-item__header) {
      font-size: 12px;
    }
  }
}
</style>
