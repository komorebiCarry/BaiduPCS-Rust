<script setup lang="ts">
import { computed, ref, watch, onMounted, onUnmounted } from 'vue'
import {
  CircleCheck, Warning, Loading, Clock,
  VideoPause, VideoPlay, Delete, Refresh,
  Document, Lock, Filter,
  Close, Check, QuestionFilled
} from '@element-plus/icons-vue'
import { ElMessage } from 'element-plus'
import type { BackupTask, BackupFileTask, BackupFileStatus, SkipReason, FilterReasonType } from '@/api/autobackup'
import { listFileTasks, retryFileTask } from '@/api/autobackup'
import { getWebSocketClient } from '@/utils/websocket'
import type { BackupEvent, BackupEventFileProgress, BackupEventProgress } from '@/types/events'

const props = defineProps<{
  modelValue: boolean
  tasks: BackupTask[]  // 改为任务列表
  configName?: string
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'pause': [taskId: string]
  'resume': [taskId: string]
  'cancel': [taskId: string]
}>()

// 当前选中的任务索引
const selectedTaskIndex = ref(0)

// 当前选中的任务
const task = computed(() => {
  if (props.tasks.length === 0) return null
  return props.tasks[selectedTaskIndex.value] || props.tasks[0]
})

// 文件任务列表相关状态
const fileTasks = ref<BackupFileTask[]>([])
const fileTasksTotal = ref(0)
const fileTasksPage = ref(1)
const fileTasksPageSize = ref(20)
const fileTasksLoading = ref(false)
const activeTab = ref('overview') // 'overview' | 'files'
const retryingFileId = ref<string | null>(null)

const visible = computed({
  get: () => props.modelValue,
  set: (val) => emit('update:modelValue', val)
})

// 任务选项列表（用于下拉选择）
const taskOptions = computed(() => {
  return props.tasks.map((t, index) => ({
    value: index,
    label: `${getStatusText(t.status)} - ${formatDate(t.created_at)}`,
    task: t
  }))
})

// 状态相关
function getStatusText(status: string): string {
  const statusMap: Record<string, string> = {
    queued: '等待中',
    preparing: '准备中',
    transferring: '传输中',
    completed: '已完成',
    partially_completed: '部分完成',
    failed: '失败',
    cancelled: '已取消',
    paused: '已暂停'
  }
  return statusMap[status] || status
}

function getStatusColor(status: string): string {
  const colorMap: Record<string, string> = {
    queued: '#909399',
    preparing: '#409EFF',
    transferring: '#409EFF',
    completed: '#67C23A',
    partially_completed: '#E6A23C',
    failed: '#F56C6C',
    cancelled: '#909399',
    paused: '#E6A23C'
  }
  return colorMap[status] || '#909399'
}

function getStatusIcon(status: string) {
  switch (status) {
    case 'completed':
    case 'partially_completed': return CircleCheck
    case 'failed': return Warning
    case 'paused': return VideoPause
    case 'cancelled': return Delete
    case 'queued':
    case 'preparing':
    case 'transferring': return Loading
    default: return Clock
  }
}

// 进度计算
const progress = computed(() => {
  if (!task.value || task.value.total_count === 0) return 0
  return Math.round((task.value.completed_count / task.value.total_count) * 100)
})

const bytesProgress = computed(() => {
  if (!task.value || task.value.total_bytes === 0) return 0
  return Math.round((task.value.transferred_bytes / task.value.total_bytes) * 100)
})

// 格式化
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

function formatDate(dateStr: string | undefined): string {
  if (!dateStr) return '-'
  return new Date(dateStr).toLocaleString('zh-CN')
}

function getTriggerTypeText(type: string): string {
  const typeMap: Record<string, string> = {
    manual: '手动触发',
    watch: '文件监听',
    poll: '定时轮询',
    scheduled: '计划任务'
  }
  return typeMap[type] || type
}

// 操作
function handlePause() {
  if (task.value) {
    emit('pause', task.value.id)
  }
}

function handleResume() {
  if (task.value) {
    emit('resume', task.value.id)
  }
}

// 是否可以操作
const canPause = computed(() => {
  if (!task.value) return false
  return ['queued', 'preparing', 'transferring'].includes(task.value.status)
})

const canResume = computed(() => {
  if (!task.value) return false
  return task.value.status === 'paused'
})

// 监听任务列表变化，重置选中索引
watch(() => props.tasks, (newTasks) => {
  if (newTasks.length > 0) {
    // 默认选中第一个（最新的）任务
    selectedTaskIndex.value = 0
  }
}, { immediate: true })

// 监听选中任务变化，重置并加载文件任务列表
watch(() => task.value?.id, async (newId, oldId) => {
  if (newId !== oldId) {
    fileTasks.value = []
    fileTasksTotal.value = 0
    fileTasksPage.value = 1
    activeTab.value = 'overview'
    // 自动加载文件列表
    if (newId && props.modelValue) {
      await loadFileTasks()
    }
  }
})

// 监听弹窗打开，加载文件任务列表
watch(() => props.modelValue, async (visible) => {
  if (visible && task.value && fileTasks.value.length === 0) {
    await loadFileTasks()
  }
})

// WebSocket 事件监听器取消函数
let unsubscribeBackupEvent: (() => void) | null = null

// 处理 WebSocket 备份事件
function handleBackupEvent(event: BackupEvent) {
  // 只处理当前任务的事件
  if (!task.value || event.task_id !== task.value.id) return

  switch (event.event_type) {
    case 'file_progress': {
      // 更新文件任务进度（仅进度，不更新状态）
      const fileProgressEvent = event as BackupEventFileProgress
      const fileTask = fileTasks.value.find(f => f.id === fileProgressEvent.file_task_id)
      if (fileTask) {
        fileTask.transferred_bytes = fileProgressEvent.transferred_bytes
        console.log(`[BackupTaskDetail] 文件进度更新: ${fileProgressEvent.file_name} -> ${fileProgressEvent.transferred_bytes}/${fileProgressEvent.total_bytes}`)
      }
      break
    }
    case 'file_status_changed': {
      // 更新文件任务状态（仅状态，不更新进度）
      const fileStatusEvent = event as BackupEvent & { event_type: 'file_status_changed' }
      const fileTask = fileTasks.value.find(f => f.id === fileStatusEvent.file_task_id)
      if (fileTask) {
        fileTask.status = fileStatusEvent.new_status as BackupFileStatus
        console.log(`[BackupTaskDetail] 文件状态变更: ${fileStatusEvent.file_name} -> ${fileStatusEvent.old_status} -> ${fileStatusEvent.new_status}`)
      }
      break
    }
    case 'progress': {
      // 主任务进度更新 - 通过 props.tasks 更新（父组件会处理）
      // 这里可以触发重新加载文件列表以获取最新状态
      const progressEvent = event as BackupEventProgress
      console.log(`[BackupTaskDetail] 任务进度更新: completed=${progressEvent.completed_count}, failed=${progressEvent.failed_count}`)
      break
    }
    case 'status_changed':
    case 'completed':
    case 'failed':
      // 任务状态变更时刷新文件列表
      console.log(`[BackupTaskDetail] 任务状态变更: ${event.event_type}`)
      loadFileTasks()
      break
  }
}

// 组件挂载时订阅 WebSocket 事件
onMounted(() => {
  const wsClient = getWebSocketClient()
  unsubscribeBackupEvent = wsClient.onBackupEvent(handleBackupEvent)
  // 确保订阅了 backup 事件
  wsClient.subscribe(['backup:*'])
})

// 组件卸载时取消订阅
onUnmounted(() => {
  if (unsubscribeBackupEvent) {
    unsubscribeBackupEvent()
    unsubscribeBackupEvent = null
  }
})

// 加载文件任务列表
async function loadFileTasks() {
  if (!task.value) return

  fileTasksLoading.value = true
  try {
    const response = await listFileTasks(task.value.id, fileTasksPage.value, fileTasksPageSize.value)
    fileTasks.value = response.file_tasks
    fileTasksTotal.value = response.total
  } catch (error) {
    ElMessage.error('加载文件任务列表失败')
    console.error('Failed to load file tasks:', error)
  } finally {
    fileTasksLoading.value = false
  }
}

// 分页变化
function handlePageChange(page: number) {
  fileTasksPage.value = page
  loadFileTasks()
}

// 重试单个文件任务
async function handleRetryFile(fileTask: BackupFileTask) {
  if (!task.value || retryingFileId.value) return

  retryingFileId.value = fileTask.id
  try {
    await retryFileTask(task.value.id, fileTask.id)
    ElMessage.success('已重新加入队列')
    // 刷新文件任务列表
    await loadFileTasks()
  } catch (error) {
    ElMessage.error('重试失败')
    console.error('Failed to retry file task:', error)
  } finally {
    retryingFileId.value = null
  }
}

// 获取文件状态文本
function getFileStatusText(status: BackupFileStatus): string {
  const statusMap: Record<BackupFileStatus, string> = {
    pending: '待处理',
    checking: '检查中',
    skipped: '已跳过',
    encrypting: '加密中',
    decrypting: '解密中',
    waiting_transfer: '等待传输',
    transferring: '传输中',
    completed: '已完成',
    failed: '失败'
  }
  return statusMap[status] || status
}

// 获取文件状态图标
function getFileStatusIcon(status: BackupFileStatus) {
  switch (status) {
    case 'completed': return Check
    case 'failed': return Close
    case 'skipped': return Filter
    case 'checking':
    case 'encrypting':
    case 'decrypting':
    case 'transferring': return Loading
    default: return Clock
  }
}

// 获取跳过原因文本
function getSkipReasonText(reason: SkipReason | undefined): string {
  if (!reason) return ''

  if (reason === 'already_exists') return '文件已存在（去重）'
  if (reason === 'unchanged') return '文件未变化'
  if (reason === 'user_cancelled') return '用户取消'
  if (reason === 'config_disabled') return '配置已禁用'

  if (typeof reason === 'object' && 'filtered' in reason) {
    return getFilterReasonText(reason.filtered)
  }

  return '未知原因'
}

// 获取过滤原因文本
function getFilterReasonText(filterReason: FilterReasonType): string {
  if (filterReason === 'hidden_file') return '隐藏文件'
  if (filterReason === 'system_file') return '系统文件'
  if (filterReason === 'temp_file') return '临时文件'

  if (typeof filterReason === 'object') {
    if ('extension_not_included' in filterReason) {
      return `扩展名不在包含列表: ${filterReason.extension_not_included}`
    }
    if ('extension_excluded' in filterReason) {
      return `扩展名被排除: ${filterReason.extension_excluded}`
    }
    if ('directory_excluded' in filterReason) {
      return `目录被排除: ${filterReason.directory_excluded}`
    }
    if ('file_too_large' in filterReason) {
      return `文件过大: ${formatBytes(filterReason.file_too_large.size)} > ${formatBytes(filterReason.file_too_large.max)}`
    }
    if ('file_too_small' in filterReason) {
      return `文件过小: ${formatBytes(filterReason.file_too_small.size)} < ${formatBytes(filterReason.file_too_small.min)}`
    }
  }

  return '被过滤'
}

// 获取跳过原因图标
function getSkipReasonIcon(reason: SkipReason | undefined) {
  if (!reason) return QuestionFilled

  if (reason === 'already_exists' || reason === 'unchanged') return Check
  if (reason === 'user_cancelled' || reason === 'config_disabled') return Close

  if (typeof reason === 'object' && 'filtered' in reason) {
    return Filter
  }

  return QuestionFilled
}

// 获取文件名
function getFileName(path: string): string {
  const parts = path.replace(/\\/g, '/').split('/')
  return parts[parts.length - 1] || path
}

// 是否可以重试
function canRetryFile(fileTask: BackupFileTask): boolean {
  return fileTask.status === 'failed' && fileTask.retry_count < 3
}
</script>

<template>
  <el-dialog
    v-model="visible"
    title="备份任务详情"
    width="600px"
    :close-on-click-modal="false"
  >
    <div v-if="tasks.length === 0" class="empty-tasks">
      <el-empty description="暂无备份任务记录" :image-size="80" />
    </div>

    <div v-else-if="task" class="task-detail">
      <!-- 任务选择器（多个任务时显示） -->
      <div v-if="tasks.length > 1" class="task-selector">
        <span class="selector-label">选择任务：</span>
        <el-select
          v-model="selectedTaskIndex"
          size="small"
          style="width: 280px"
        >
          <el-option
            v-for="(option, index) in taskOptions"
            :key="option.task.id"
            :value="index"
            :label="option.label"
          >
            <div class="task-option">
              <el-icon :size="14" :style="{ color: getStatusColor(option.task.status) }">
                <component :is="getStatusIcon(option.task.status)" />
              </el-icon>
              <span>{{ getStatusText(option.task.status) }}</span>
              <span class="task-option-time">{{ formatDate(option.task.created_at) }}</span>
            </div>
          </el-option>
        </el-select>
        <span class="task-count">共 {{ tasks.length }} 个任务</span>
      </div>

      <!-- 状态头部 -->
      <div class="status-header">
        <div class="status-icon" :style="{ backgroundColor: getStatusColor(task.status) + '20' }">
          <component
            :is="getStatusIcon(task.status)"
            :style="{ color: getStatusColor(task.status) }"
            :class="{ 'is-loading': ['queued', 'preparing', 'transferring'].includes(task.status) }"
          />
        </div>
        <div class="status-info">
          <div class="status-text" :style="{ color: getStatusColor(task.status) }">
            {{ getStatusText(task.status) }}
          </div>
          <div class="config-name" v-if="configName">{{ configName }}</div>
        </div>
      </div>

      <!-- Tab 切换 -->
      <el-tabs v-model="activeTab" class="task-tabs">
        <el-tab-pane label="概览" name="overview">
          <!-- 进度条 -->
          <div class="progress-section">
            <div class="progress-label">
              <span>文件进度</span>
              <span>{{ task.completed_count }} / {{ task.total_count }} 文件</span>
            </div>
            <el-progress :percentage="progress" :status="task.status === 'completed' ? 'success' : undefined" />

            <div class="progress-label mt-3">
              <span>数据进度</span>
              <span>{{ formatBytes(task.transferred_bytes) }} / {{ formatBytes(task.total_bytes) }}</span>
            </div>
            <el-progress :percentage="bytesProgress" :status="task.status === 'completed' ? 'success' : undefined" />
          </div>

          <!-- 统计信息 -->
          <div class="stats-grid stats-grid-3">
            <div class="stat-item">
              <div class="stat-value text-green-500">{{ task.completed_count }}</div>
              <div class="stat-label">成功</div>
            </div>
            <div class="stat-item">
              <div class="stat-value text-red-500">{{ task.failed_count }}</div>
              <div class="stat-label">失败</div>
            </div>
            <div class="stat-item">
              <div class="stat-value text-blue-500">{{ task.total_count }}</div>
              <div class="stat-label">总计</div>
            </div>
          </div>

          <!-- 详细信息（折叠显示） -->
          <el-collapse class="detail-collapse">
            <el-collapse-item title="详细信息" name="details">
              <div class="detail-section">
                <div class="detail-item">
                  <span class="detail-label">任务 ID</span>
                  <span class="detail-value font-mono text-xs">{{ task.id }}</span>
                </div>
                <div class="detail-item">
                  <span class="detail-label">触发方式</span>
                  <span class="detail-value">{{ getTriggerTypeText(task.trigger_type) }}</span>
                </div>
                <div class="detail-item">
                  <span class="detail-label">创建时间</span>
                  <span class="detail-value">{{ formatDate(task.created_at) }}</span>
                </div>
                <div class="detail-item">
                  <span class="detail-label">开始时间</span>
                  <span class="detail-value">{{ formatDate(task.started_at) }}</span>
                </div>
                <div class="detail-item">
                  <span class="detail-label">完成时间</span>
                  <span class="detail-value">{{ formatDate(task.completed_at) }}</span>
                </div>
                <div v-if="task.error_message" class="detail-item error">
                  <span class="detail-label">错误信息</span>
                  <span class="detail-value text-red-500">{{ task.error_message }}</span>
                </div>
              </div>
            </el-collapse-item>
          </el-collapse>
        </el-tab-pane>

        <el-tab-pane :label="`文件列表 (${fileTasksTotal})`" name="files">
          <!-- 文件任务列表 -->
          <div class="file-tasks-section" v-loading="fileTasksLoading">
            <div v-if="fileTasks.length === 0 && !fileTasksLoading" class="empty-files">
              <el-empty description="暂无文件任务" :image-size="80" />
            </div>

            <div v-else class="file-task-list">
              <div
                v-for="fileTask in fileTasks"
                :key="fileTask.id"
                class="file-task-item"
              >
                <!-- 文件信息 -->
                <div class="file-info">
                  <div class="file-icon">
                    <el-icon v-if="fileTask.encrypted" class="encrypted-icon"><Lock /></el-icon>
                    <el-icon v-else><Document /></el-icon>
                  </div>
                  <div class="file-details">
                    <div class="file-name" :title="fileTask.local_path">
                      {{ getFileName(fileTask.local_path) }}
                    </div>
                    <div class="file-path">{{ fileTask.local_path }}</div>
                    <div class="file-meta">
                      <span>{{ formatBytes(fileTask.file_size) }}</span>
                      <span v-if="fileTask.retry_count > 0" class="retry-count">
                        重试 {{ fileTask.retry_count }} 次
                      </span>
                    </div>
                  </div>
                </div>

                <!-- 状态和操作 -->
                <div class="file-status-actions">
                  <!-- 状态标签 -->
                  <el-tag
                    :type="fileTask.status === 'completed' ? 'success' :
                           fileTask.status === 'failed' ? 'danger' :
                           fileTask.status === 'skipped' ? 'warning' : 'info'"
                    size="small"
                  >
                    <el-icon class="status-icon-small">
                      <component :is="getFileStatusIcon(fileTask.status)" />
                    </el-icon>
                    {{ getFileStatusText(fileTask.status) }}
                  </el-tag>

                  <!-- 跳过原因 -->
                  <el-tooltip
                    v-if="fileTask.status === 'skipped' && fileTask.skip_reason"
                    :content="getSkipReasonText(fileTask.skip_reason)"
                    placement="top"
                  >
                    <div class="skip-reason">
                      <el-icon :color="'#E6A23C'">
                        <component :is="getSkipReasonIcon(fileTask.skip_reason)" />
                      </el-icon>
                      <span class="skip-reason-text">{{ getSkipReasonText(fileTask.skip_reason) }}</span>
                    </div>
                  </el-tooltip>

                  <!-- 错误信息 -->
                  <el-tooltip
                    v-if="fileTask.status === 'failed' && fileTask.error_message"
                    :content="fileTask.error_message"
                    placement="top"
                  >
                    <div class="error-info">
                      <el-icon color="#F56C6C"><Warning /></el-icon>
                      <span class="error-text">{{ fileTask.error_message }}</span>
                    </div>
                  </el-tooltip>

                  <!-- 重试按钮 -->
                  <el-button
                    v-if="canRetryFile(fileTask)"
                    type="primary"
                    size="small"
                    :loading="retryingFileId === fileTask.id"
                    @click="handleRetryFile(fileTask)"
                  >
                    <el-icon class="mr-1"><Refresh /></el-icon>
                    重试
                  </el-button>
                </div>
              </div>
            </div>

            <!-- 分页 -->
            <div v-if="fileTasksTotal > fileTasksPageSize" class="pagination-wrapper">
              <el-pagination
                v-model:current-page="fileTasksPage"
                :page-size="fileTasksPageSize"
                :total="fileTasksTotal"
                layout="prev, pager, next"
                @current-change="handlePageChange"
              />
            </div>
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>

    <template #footer>
      <div class="dialog-footer">
        <el-button @click="visible = false">关闭</el-button>
        <el-button
          v-if="canPause"
          type="warning"
          @click="handlePause"
        >
          <el-icon class="mr-1"><VideoPause /></el-icon>
          暂停
        </el-button>
        <el-button
          v-if="canResume"
          type="success"
          @click="handleResume"
        >
          <el-icon class="mr-1"><VideoPlay /></el-icon>
          恢复
        </el-button>
      </div>
    </template>
  </el-dialog>
</template>

<style scoped>
.task-detail {
  padding: 0 8px;
}

.status-header {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 24px;
}

.status-icon {
  width: 56px;
  height: 56px;
  border-radius: 12px;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 28px;
}

.status-icon .is-loading {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.status-info {
  flex: 1;
}

.status-text {
  font-size: 20px;
  font-weight: 600;
}

.config-name {
  color: var(--el-text-color-secondary);
  font-size: 14px;
  margin-top: 4px;
}

.progress-section {
  margin-bottom: 24px;
}

.progress-label {
  display: flex;
  justify-content: space-between;
  font-size: 13px;
  color: var(--el-text-color-secondary);
  margin-bottom: 8px;
}

.mt-3 {
  margin-top: 12px;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  margin-bottom: 24px;
  padding: 16px;
  background: var(--el-fill-color-light);
  border-radius: 8px;
}

.stats-grid-3 {
  grid-template-columns: repeat(3, 1fr);
}

.stat-item {
  text-align: center;
}

.stat-value {
  font-size: 24px;
  font-weight: 600;
}

.stat-label {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}

.detail-section {
  border-top: 1px solid var(--el-border-color-lighter);
  padding-top: 16px;
}

.detail-item {
  display: flex;
  justify-content: space-between;
  padding: 8px 0;
  border-bottom: 1px solid var(--el-border-color-lighter);
}

.detail-item:last-child {
  border-bottom: none;
}

.detail-item.error {
  flex-direction: column;
  gap: 4px;
}

.detail-label {
  color: var(--el-text-color-secondary);
  font-size: 13px;
}

.detail-value {
  color: var(--el-text-color-primary);
  font-size: 13px;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 8px;
}

.mr-1 {
  margin-right: 4px;
}

.text-green-500 { color: #22c55e; }
.text-red-500 { color: #ef4444; }
.text-gray-500 { color: #6b7280; }
.text-blue-500 { color: #3b82f6; }
.font-mono { font-family: monospace; }
.text-xs { font-size: 12px; }

/* Tab 样式 */
.task-tabs {
  margin-top: -8px;
}

/* 文件任务列表样式 */
.file-tasks-section {
  min-height: 200px;
}

.empty-files {
  padding: 40px 0;
}

.file-task-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.file-task-item {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  padding: 12px;
  background: var(--el-fill-color-light);
  border-radius: 8px;
  gap: 12px;
}

.file-info {
  display: flex;
  gap: 12px;
  flex: 1;
  min-width: 0;
}

.file-icon {
  width: 36px;
  height: 36px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--el-fill-color);
  border-radius: 6px;
  font-size: 18px;
  color: var(--el-text-color-secondary);
  flex-shrink: 0;
}

.file-icon .encrypted-icon {
  color: var(--el-color-warning);
}

.file-details {
  flex: 1;
  min-width: 0;
}

.file-name {
  font-weight: 500;
  font-size: 14px;
  color: var(--el-text-color-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.file-path {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  margin-top: 2px;
}

.file-meta {
  display: flex;
  gap: 12px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}

.retry-count {
  color: var(--el-color-warning);
}

.file-status-actions {
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 8px;
  flex-shrink: 0;
}

.status-icon-small {
  margin-right: 4px;
  font-size: 12px;
}

.skip-reason,
.error-info {
  display: flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  max-width: 200px;
}

.skip-reason-text,
.error-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  color: var(--el-text-color-secondary);
}

.error-text {
  color: var(--el-color-danger);
}

.pagination-wrapper {
  display: flex;
  justify-content: center;
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid var(--el-border-color-lighter);
}

/* 空状态 */
.empty-tasks {
  padding: 40px 0;
}

/* 任务选择器样式 */
.task-selector {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 16px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--el-border-color-lighter);
}

.selector-label {
  font-size: 14px;
  color: var(--el-text-color-secondary);
  flex-shrink: 0;
}

.task-count {
  font-size: 12px;
  color: var(--el-text-color-placeholder);
}

.task-option {
  display: flex;
  align-items: center;
  gap: 8px;
}

.task-option-time {
  margin-left: auto;
  font-size: 12px;
  color: var(--el-text-color-placeholder);
}

/* 详细信息折叠样式 */
.detail-collapse {
  border: none;
}

.detail-collapse :deep(.el-collapse-item__header) {
  font-size: 14px;
  color: var(--el-text-color-secondary);
  background: transparent;
  border-bottom: none;
  height: 40px;
}

.detail-collapse :deep(.el-collapse-item__wrap) {
  border-bottom: none;
}

.detail-collapse :deep(.el-collapse-item__content) {
  padding-bottom: 0;
}
</style>
