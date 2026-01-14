<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Plus, VideoPlay, VideoPause, Delete, Key,
  Upload, Download,
  Warning, CircleCheck, Loading, Refresh,
  FolderOpened, Clock
} from '@element-plus/icons-vue'
import {
  listBackupConfigs, createBackupConfig, updateBackupConfig, deleteBackupConfig,
  triggerBackup, listBackupTasks, cancelBackupTask, pauseBackupTask, resumeBackupTask,
  getEncryptionStatus,
  getManagerStatus,
  listFileTasks,
  type BackupConfig, type BackupTask, type EncryptionStatus, type ManagerStatus,
  type CreateBackupConfigRequest, type BackupFileTask
} from '@/api/autobackup'
import FilePickerModal from '@/components/FilePicker/FilePickerModal.vue'
import NetdiskPathSelector from '@/components/NetdiskPathSelector.vue'
import BackupTaskDetail from '@/components/BackupTaskDetail.vue'
import { getWebSocketClient, connectWebSocket, type ConnectionState } from '@/utils/websocket'
import { useIsMobile } from '@/utils/responsive'
import type { BackupEvent } from '@/types/events'

// 响应式检测
const isMobile = useIsMobile()

// ==================== 状态 ====================

const configs = ref<BackupConfig[]>([])
// 每个配置的活跃任务（正在进行的备份任务）
const activeTaskByConfig = ref<Map<string, BackupTask | null>>(new Map())
// 每个配置的活跃文件任务（前5个正在传输的文件）
const activeFileTasks = ref<Map<string, BackupFileTask[]>>(new Map())
const encryptionStatus = ref<EncryptionStatus | null>(null)
const managerStatus = ref<ManagerStatus | null>(null)
const loading = ref(false)
const error = ref('')

// 对话框状态
const showCreateDialog = ref(false)

// 表单数据
const newConfig = ref<CreateBackupConfigRequest>({
  name: '',
  local_path: '',
  remote_path: '/',
  direction: 'upload',
  watch_config: { enabled: true, debounce_ms: 3000, recursive: true },
  poll_config: { enabled: true, mode: 'interval', interval_minutes: 60 },
  filter_config: { include_patterns: [], exclude_patterns: ['.*', '*.tmp', '~$*'] },
  encrypt_enabled: false
})


// 文件选择器状态
const showLocalPathPicker = ref(false)
const remoteFsId = ref(0)

// 任务详情弹窗状态
const showTaskDetail = ref(false)
const selectedTasks = ref<BackupTask[]>([])  // 改为任务列表
const selectedConfigName = ref('')

// 下载备份时禁用监听选项
const isDownloadBackup = computed(() => newConfig.value.direction === 'download')

// 监听备份方向变化，自动禁用监听
watch(() => newConfig.value.direction, (direction) => {
  if (direction === 'download') {
    newConfig.value.watch_config.enabled = false
  }
})

// ==================== 方法 ====================

async function loadData() {
  loading.value = true
  error.value = ''
  try {
    const [configList, encryption, status] = await Promise.all([
      listBackupConfigs(),
      getEncryptionStatus(),
      getManagerStatus()
    ])
    configs.value = configList
    encryptionStatus.value = encryption
    managerStatus.value = status

    // 为每个配置加载活跃任务
    await loadActiveTasksForAllConfigs()
  } catch (e: any) {
    error.value = e.message || '加载数据失败'
  } finally {
    loading.value = false
  }
}

// 为所有配置加载活跃任务
async function loadActiveTasksForAllConfigs() {
  for (const config of configs.value) {
    await loadActiveTaskForConfig(config.id)
  }
}

// 为单个配置加载活跃任务和文件任务
async function loadActiveTaskForConfig(configId: string) {
  try {
    const taskList = await listBackupTasks(configId)
    // 找到活跃的任务（非完成、非取消、非失败状态）
    const activeTask = taskList.find(t =>
        !['completed', 'cancelled', 'failed', 'partially_completed'].includes(t.status)
    ) || null

    activeTaskByConfig.value.set(configId, activeTask)

    // 如果有活跃任务，加载前5个文件任务
    if (activeTask) {
      await loadFileTasksForActiveTask(activeTask.id, configId)
    } else {
      activeFileTasks.value.set(configId, [])
    }
  } catch (e: any) {
    console.error('加载活跃任务失败:', e)
  }
}

// 加载活跃任务的前5个文件任务
async function loadFileTasksForActiveTask(taskId: string, configId: string) {
  try {
    const response = await listFileTasks(taskId, 1, 5)
    activeFileTasks.value.set(configId, response.file_tasks)
  } catch (e: any) {
    console.error('加载文件任务失败:', e)
  }
}

async function handleCreateConfig() {
  try {
    const config = await createBackupConfig(newConfig.value)
    configs.value.push(config)
    showCreateDialog.value = false
    resetNewConfig()
    ElMessage.success('配置创建成功')
  } catch (e: any) {
    // 使用 ElMessage.error 显示错误信息，保持对话框打开让用户可以修改后重试
    ElMessage.error(e.message || '创建配置失败')
  }
}

function resetNewConfig() {
  newConfig.value = {
    name: '',
    local_path: '',
    remote_path: '/',
    direction: 'upload',
    watch_config: { enabled: true, debounce_ms: 3000, recursive: true },
    poll_config: { enabled: true, mode: 'interval', interval_minutes: 60 },
    filter_config: { include_patterns: [], exclude_patterns: ['.*', '*.tmp', '~$*'] },
    encrypt_enabled: false
  }
}

async function handleDeleteConfig(id: string) {
  try {
    await ElMessageBox.confirm('确定要删除此备份配置吗？', '删除确认', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning',
    })
    await deleteBackupConfig(id)
    configs.value = configs.value.filter(c => c.id !== id)
    ElMessage.success('配置已删除')
  } catch (e: any) {
    if (e !== 'cancel') {
      error.value = e.message || '删除配置失败'
    }
  }
}

async function handleToggleConfig(config: BackupConfig) {
  try {
    const updated = await updateBackupConfig(config.id, { enabled: !config.enabled })
    const index = configs.value.findIndex(c => c.id === config.id)
    if (index !== -1) {
      configs.value[index] = updated
    }
  } catch (e: any) {
    error.value = e.message || '更新配置失败'
  }
}

async function handleTriggerBackup(configId: string) {
  try {
    await triggerBackup(configId)
    await loadActiveTaskForConfig(configId)
  } catch (e: any) {
    error.value = e.message || '触发备份失败'
  }
}

// 获取文件名
function getFileName(path: string): string {
  const parts = path.replace(/\\/g, '/').split('/')
  return parts[parts.length - 1] || path
}

// 获取文件状态文本
function getFileStatusText(status: string): string {
  const statusMap: Record<string, string> = {
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

// 获取文件状态颜色
function getFileStatusColor(status: string): string {
  switch (status) {
    case 'completed': return 'success'
    case 'failed': return 'danger'
    case 'skipped': return 'warning'
    case 'checking':
    case 'encrypting':
    case 'decrypting':
    case 'transferring': return 'primary'
    default: return 'info'
  }
}

async function handleCancelTask(taskId: string, configId: string) {
  try {
    await cancelBackupTask(taskId)
    await loadActiveTaskForConfig(configId)
  } catch (e: any) {
    error.value = e.message || '取消任务失败'
  }
}

async function handlePauseTask(taskId: string, configId: string) {
  try {
    await pauseBackupTask(taskId)
    await loadActiveTaskForConfig(configId)
  } catch (e: any) {
    error.value = e.message || '暂停任务失败'
  }
}

async function handleResumeTask(taskId: string, configId: string) {
  try {
    await resumeBackupTask(taskId)
    await loadActiveTaskForConfig(configId)
  } catch (e: any) {
    error.value = e.message || '恢复任务失败'
  }
}

// 本地路径选择确认
function handleLocalPathConfirm(path: string) {
  newConfig.value.local_path = path
  showLocalPathPicker.value = false
}

// 打开任务详情（传入任务列表）
function openTaskDetail(tasks: BackupTask[], configName: string) {
  selectedTasks.value = tasks
  selectedConfigName.value = configName
  showTaskDetail.value = true
}

// 打开历史任务（获取所有任务并打开详情）
async function openHistoryTasks(config: BackupConfig) {
  try {
    const tasks = await listBackupTasks(config.id)
    if (tasks.length > 0) {
      // 打开任务列表详情
      openTaskDetail(tasks, config.name)
    } else {
      ElMessage.info('暂无历史备份记录')
    }
  } catch (e: any) {
    error.value = e.message || '加载历史任务失败'
  }
}

// 任务详情弹窗中的操作
async function handleTaskDetailPause(taskId: string) {
  // 从任务列表中找到对应任务的 configId
  const task = selectedTasks.value.find(t => t.id === taskId)
  const configId = task?.config_id
  if (configId) {
    await handlePauseTask(taskId, configId)
    // 刷新任务列表
    await refreshSelectedTasks(configId)
  }
}

async function handleTaskDetailResume(taskId: string) {
  const task = selectedTasks.value.find(t => t.id === taskId)
  const configId = task?.config_id
  if (configId) {
    await handleResumeTask(taskId, configId)
    await refreshSelectedTasks(configId)
  }
}

async function handleTaskDetailCancel(taskId: string) {
  const task = selectedTasks.value.find(t => t.id === taskId)
  const configId = task?.config_id
  if (configId) {
    await handleCancelTask(taskId, configId)
    await refreshSelectedTasks(configId)
  }
}

// 刷新选中的任务列表
async function refreshSelectedTasks(configId: string) {
  try {
    const tasks = await listBackupTasks(configId)
    selectedTasks.value = tasks
  } catch (e: any) {
    console.error('刷新任务列表失败:', e)
  }
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

function getStatusColor(status: string) {
  switch (status) {
    case 'completed': return 'text-green-500'
    case 'partially_completed': return 'text-yellow-500'
    case 'failed': return 'text-red-500'
    case 'paused': return 'text-yellow-500'
    case 'cancelled': return 'text-gray-500'
    case 'queued':
    case 'preparing':
    case 'transferring': return 'text-blue-500'
    default: return 'text-gray-500'
  }
}

function getStatusText(status: string) {
  switch (status) {
    case 'queued': return '等待中'
    case 'preparing': return '准备中'
    case 'transferring': return '传输中'
    case 'completed': return '已完成'
    case 'partially_completed': return '部分完成'
    case 'failed': return '失败'
    case 'paused': return '已暂停'
    case 'cancelled': return '已取消'
    default: return status
  }
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

// 计算备份任务进度百分比
function calcBackupPercent(task: BackupTask): number {
  if (task.total_bytes === 0) return 0
  return Math.round((task.transferred_bytes / task.total_bytes) * 100)
}

// 计算文件任务进度百分比
function calcFilePercent(fileTask: BackupFileTask): number {
  if (fileTask.file_size === 0) return 0
  return Math.round((fileTask.transferred_bytes / fileTask.file_size) * 100)
}

// 格式化文件已传输大小
function formatFileTransferred(fileTask: BackupFileTask): string {
  return `${formatBytes(fileTask.transferred_bytes)} / ${formatBytes(fileTask.file_size)}`
}

// ==================== WebSocket 事件处理 ====================

let unsubscribeBackup: (() => void) | null = null
let unsubscribeConnectionState: (() => void) | null = null
let refreshTimer: number | null = null
const wsConnected = ref(false)

// 选项1：仅当存在活跃任务且 WebSocket 未连接时启用轮询兜底
const hasActiveTask = computed(() => {
  for (const [, task] of activeTaskByConfig.value) {
    if (task) return true
  }
  return false
})

function stopPolling() {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
}

function startPolling() {
  if (refreshTimer) return
  const interval = 2000
  refreshTimer = window.setInterval(() => {
    // 仅在“仍然未连接且仍存在活跃任务”时轮询
    if (wsConnected.value || !hasActiveTask.value) {
      stopPolling()
      return
    }
    loadActiveTasksForAllConfigs()
  }, interval)
}

function updateAutoRefresh() {
  if (!wsConnected.value && hasActiveTask.value) {
    startPolling()
    return
  }
  stopPolling()
}

function handleBackupEvent(event: BackupEvent) {
  console.log('[AutoBackup] 收到备份事件:', event)

  // 获取 config_id（部分事件有，部分没有）
  const configId = 'config_id' in event ? event.config_id : null

  switch (event.event_type) {
    case 'created':
      // 新任务创建，刷新对应配置的活跃任务
      if (configId) {
        loadActiveTaskForConfig(configId)
      }
      break

    case 'progress':
      // 更新任务进度
      updateTaskProgress(event)
      break

    case 'status_changed':
      // 状态变更，刷新活跃任务
      findConfigIdByTaskId(event.task_id).then(foundConfigId => {
        if (foundConfigId) {
          loadActiveTaskForConfig(foundConfigId)
          // 如果是当前查看的任务详情，也刷新任务列表
          if (selectedTasks.value.some(t => t.id === event.task_id)) {
            refreshSelectedTasks(foundConfigId)
          }
        }
      })
      break

    case 'completed':
    case 'failed':
      // 任务完成或失败，刷新活跃任务和管理器状态
      findConfigIdByTaskId(event.task_id).then(foundConfigId => {
        if (foundConfigId) {
          loadActiveTaskForConfig(foundConfigId)
        }
      })
      loadData() // 刷新整体状态
      break

    case 'file_progress':
      // 文件级别进度事件，直接更新内存中的文件任务状态
      updateFileTaskProgress(event as BackupEvent & { event_type: 'file_progress' })
      break

    case 'file_status_changed':
      // 文件状态变更事件，更新文件任务状态
      updateFileTaskStatus(event as BackupEvent & { event_type: 'file_status_changed' })
      break

    case 'paused':
    case 'resumed':
    case 'cancelled':
      // 任务状态变更
      findConfigIdByTaskId(event.task_id).then(foundConfigId => {
        if (foundConfigId) {
          loadActiveTaskForConfig(foundConfigId)
        }
      })
      break

    case 'file_encrypting':
      // 文件开始加密
      updateFileTaskEncryptStatus(event as BackupEvent & { event_type: 'file_encrypting' }, 'encrypting')
      break

    case 'file_encrypted':
      // 文件加密完成，状态将变为等待传输或传输中
      updateFileTaskEncryptStatus(event as BackupEvent & { event_type: 'file_encrypted' }, 'waiting_transfer')
      break

    case 'file_decrypting':
      // 文件开始解密
      updateFileTaskDecryptStatus(event as BackupEvent & { event_type: 'file_decrypting' }, 'decrypting')
      break

    case 'file_decrypted':
      // 文件解密完成
      updateFileTaskDecryptStatus(event as BackupEvent & { event_type: 'file_decrypted' }, 'completed')
      break

    case 'file_encrypt_progress':
      // 文件加密进度
      updateFileTaskEncryptProgress(event as BackupEvent & { event_type: 'file_encrypt_progress' })
      break

    case 'file_decrypt_progress':
      // 文件解密进度
      updateFileTaskDecryptProgress(event as BackupEvent & { event_type: 'file_decrypt_progress' })
      break
  }
}

// 根据任务ID查找配置ID
async function findConfigIdByTaskId(taskId: string): Promise<string | null> {
  // 先从活跃任务中查找
  for (const [configId, task] of activeTaskByConfig.value) {
    if (task?.id === taskId) {
      return configId
    }
  }
  // 如果内存中没有，从选中的任务列表中查找
  const selectedTask = selectedTasks.value.find(t => t.id === taskId)
  if (selectedTask) {
    return selectedTask.config_id
  }
  return null
}

function updateTaskProgress(event: BackupEvent & { event_type: 'progress' }) {
  // 遍历所有配置的活跃任务查找匹配的任务
  for (const [configId, task] of activeTaskByConfig.value) {
    if (task?.id === event.task_id) {
      // 更新任务进度
      const updatedTask = {
        ...task,
        completed_count: event.completed_count,
        failed_count: event.failed_count,
        skipped_count: event.skipped_count,
        total_count: event.total_count,
        transferred_bytes: event.transferred_bytes,
        total_bytes: event.total_bytes,
      }
      activeTaskByConfig.value.set(configId, updatedTask)
      // 触发响应式更新
      activeTaskByConfig.value = new Map(activeTaskByConfig.value)

      // 如果是当前查看的任务详情列表中有这个任务，也更新
      const taskIndex = selectedTasks.value.findIndex(t => t.id === event.task_id)
      if (taskIndex !== -1) {
        selectedTasks.value[taskIndex] = { ...updatedTask }
        // 触发响应式更新
        selectedTasks.value = [...selectedTasks.value]
      }
      break
    }
  }
}

// 更新文件任务进度（直接更新内存，避免频繁 API 请求）
// 注意：仅更新进度，不更新状态
function updateFileTaskProgress(event: BackupEvent & { event_type: 'file_progress' }) {
  // 遍历所有配置的文件任务列表查找匹配的文件任务
  for (const [_configId, fileTasks] of activeFileTasks.value) {
    const fileTask = fileTasks.find(f => f.id === event.file_task_id)
    if (fileTask) {
      // 仅更新进度，不更新状态（状态由 file_status_changed 事件处理）
      fileTask.transferred_bytes = event.transferred_bytes
      // 🔥 如果当前是加密/解密状态，收到传输进度后自动切换为传输状态
      if (fileTask.status === 'encrypting' || fileTask.status === 'decrypting') {
        fileTask.status = 'transferring'
      }
      // 触发响应式更新
      activeFileTasks.value = new Map(activeFileTasks.value)
      console.log(`[AutoBackup] 文件进度更新: ${event.file_name} -> ${event.transferred_bytes}/${event.total_bytes}`)
      return
    }
  }

  // 如果在当前列表中没找到（可能是新开始传输的文件），则重新加载文件列表
  findConfigIdByTaskId(event.task_id).then(foundConfigId => {
    if (foundConfigId) {
      const activeTask = activeTaskByConfig.value.get(foundConfigId)
      if (activeTask) {
        loadFileTasksForActiveTask(activeTask.id, foundConfigId)
      }
    }
  })
}

// 更新文件任务状态（仅状态变更，不更新进度）
function updateFileTaskStatus(event: BackupEvent & { event_type: 'file_status_changed' }) {
  // 遍历所有配置的文件任务列表查找匹配的文件任务
  for (const [_configId, fileTasks] of activeFileTasks.value) {
    const fileTask = fileTasks.find(f => f.id === event.file_task_id)
    if (fileTask) {
      // 仅更新状态
      fileTask.status = event.new_status as BackupFileTask['status']
      // 触发响应式更新
      activeFileTasks.value = new Map(activeFileTasks.value)
      console.log(`[AutoBackup] 文件状态变更: ${event.file_name} -> ${event.old_status} -> ${event.new_status}`)
      return
    }
  }

  // 如果在当前列表中没找到，则重新加载文件列表
  findConfigIdByTaskId(event.task_id).then(foundConfigId => {
    if (foundConfigId) {
      const activeTask = activeTaskByConfig.value.get(foundConfigId)
      if (activeTask) {
        loadFileTasksForActiveTask(activeTask.id, foundConfigId)
      }
    }
  })
}

// 更新文件任务加密状态
function updateFileTaskEncryptStatus(
    event: BackupEvent & { event_type: 'file_encrypting' | 'file_encrypted' },
    newStatus: string
) {
  // 遍历所有配置的文件任务列表查找匹配的文件任务
  for (const [, fileTasks] of activeFileTasks.value) {
    const fileTask = fileTasks.find(f => f.id === event.file_task_id)
    if (fileTask) {
      // 更新状态
      fileTask.status = newStatus as BackupFileTask['status']
      // 触发响应式更新
      activeFileTasks.value = new Map(activeFileTasks.value)
      console.log(`[AutoBackup] 文件加密状态变更: ${event.file_name} -> ${newStatus}`)
      return
    }
  }

  // 如果在当前列表中没找到，则重新加载文件列表
  findConfigIdByTaskId(event.task_id).then(foundConfigId => {
    if (foundConfigId) {
      const activeTask = activeTaskByConfig.value.get(foundConfigId)
      if (activeTask) {
        loadFileTasksForActiveTask(activeTask.id, foundConfigId)
      }
    }
  })
}

// 更新文件任务解密状态
function updateFileTaskDecryptStatus(
    event: BackupEvent & { event_type: 'file_decrypting' | 'file_decrypted' },
    newStatus: string
) {
  // 遍历所有配置的文件任务列表查找匹配的文件任务
  for (const [, fileTasks] of activeFileTasks.value) {
    const fileTask = fileTasks.find(f => f.id === event.file_task_id)
    if (fileTask) {
      // 更新状态
      fileTask.status = newStatus as BackupFileTask['status']
      // 触发响应式更新
      activeFileTasks.value = new Map(activeFileTasks.value)
      console.log(`[AutoBackup] 文件解密状态变更: ${event.file_name} -> ${newStatus}`)
      return
    }
  }

  // 如果在当前列表中没找到，则重新加载文件列表
  findConfigIdByTaskId(event.task_id).then(foundConfigId => {
    if (foundConfigId) {
      const activeTask = activeTaskByConfig.value.get(foundConfigId)
      if (activeTask) {
        loadFileTasksForActiveTask(activeTask.id, foundConfigId)
      }
    }
  })
}

// 更新文件任务加密进度
function updateFileTaskEncryptProgress(
    event: BackupEvent & { event_type: 'file_encrypt_progress' }
) {
  // 遍历所有配置的文件任务列表查找匹配的文件任务
  for (const [, fileTasks] of activeFileTasks.value) {
    const fileTask = fileTasks.find(f => f.id === event.file_task_id)
    if (fileTask) {
      // 更新加密进度和状态
      fileTask.encrypt_progress = event.progress
      fileTask.status = 'encrypting'
      // 触发响应式更新
      activeFileTasks.value = new Map(activeFileTasks.value)
      console.log(`[AutoBackup] 文件加密进度更新: ${event.file_name} -> ${event.progress.toFixed(1)}%`)
      return
    }
  }

  // 如果在当前列表中没找到，则重新加载文件列表
  findConfigIdByTaskId(event.task_id).then(foundConfigId => {
    if (foundConfigId) {
      const activeTask = activeTaskByConfig.value.get(foundConfigId)
      if (activeTask) {
        loadFileTasksForActiveTask(activeTask.id, foundConfigId)
      }
    }
  })
}

// 更新文件任务解密进度
function updateFileTaskDecryptProgress(
    event: BackupEvent & { event_type: 'file_decrypt_progress' }
) {
  // 遍历所有配置的文件任务列表查找匹配的文件任务
  for (const [, fileTasks] of activeFileTasks.value) {
    const fileTask = fileTasks.find(f => f.id === event.file_task_id)
    if (fileTask) {
      // 更新解密进度和状态
      fileTask.decrypt_progress = event.progress
      fileTask.status = 'decrypting'
      // 触发响应式更新
      activeFileTasks.value = new Map(activeFileTasks.value)
      console.log(`[AutoBackup] 文件解密进度更新: ${event.file_name} -> ${event.progress.toFixed(1)}%`)
      return
    }
  }

  // 如果在当前列表中没找到，则重新加载文件列表
  findConfigIdByTaskId(event.task_id).then(foundConfigId => {
    if (foundConfigId) {
      const activeTask = activeTaskByConfig.value.get(foundConfigId)
      if (activeTask) {
        loadFileTasksForActiveTask(activeTask.id, foundConfigId)
      }
    }
  })
}

function setupWebSocket() {
  const wsClient = getWebSocketClient()

  // 与其他页面保持一致：进入页面先确保 WebSocket 连接
  connectWebSocket()

  // 订阅备份事件
  wsClient.subscribe(['backup:*'])

  // 监听备份事件
  unsubscribeBackup = wsClient.onBackupEvent(handleBackupEvent)

  // 订阅连接状态变化：连接中/失败时启动轮询兜底，连接恢复后停止并刷新一次
  unsubscribeConnectionState = wsClient.onConnectionStateChange((state: ConnectionState) => {
    const wasConnected = wsConnected.value
    wsConnected.value = state === 'connected'
    updateAutoRefresh()
    if (!wasConnected && wsConnected.value) {
      loadActiveTasksForAllConfigs()
    }
  })

  // 初始化时也检查一次，避免首次进入页面 WS 尚未连上出现空窗
  updateAutoRefresh()
}

function cleanupWebSocket() {
  const wsClient = getWebSocketClient()

  // 取消订阅
  wsClient.unsubscribe(['backup:*'])

  // 移除事件监听
  if (unsubscribeBackup) {
    unsubscribeBackup()
    unsubscribeBackup = null
  }

  if (unsubscribeConnectionState) {
    unsubscribeConnectionState()
    unsubscribeConnectionState = null
  }
  stopPolling()
}

// ==================== 生命周期 ====================

onMounted(() => {
  loadData()
  setupWebSocket()
})

onUnmounted(() => {
  cleanupWebSocket()
})

// 活跃任务变化时同步检查轮询策略（例如：刚创建任务但 WS 未连接）
watch(hasActiveTask, () => {
  updateAutoRefresh()
})
</script>

<template>
  <div class="autobackup-container" :class="{ 'is-mobile': isMobile }">
    <!-- 顶部工具栏 -->
    <div class="toolbar">
      <div class="header-left">
        <h2 v-if="!isMobile">自动备份</h2>
        <el-tag v-if="managerStatus" :type="managerStatus.active_task_count > 0 ? 'success' : 'info'" size="large">
          {{ managerStatus.active_task_count }} 个任务进行中
        </el-tag>
      </div>
      <div class="header-right">
        <template v-if="!isMobile">
          <el-button @click="loadData">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
          <el-button type="primary" @click="showCreateDialog = true">
            <el-icon><Plus /></el-icon>
            新建配置
          </el-button>
        </template>
        <template v-else>
          <el-button circle @click="loadData">
            <el-icon><Refresh /></el-icon>
          </el-button>
          <el-button circle type="primary" @click="showCreateDialog = true">
            <el-icon><Plus /></el-icon>
          </el-button>
        </template>
      </div>
    </div>

    <!-- 错误提示 -->
    <el-alert v-if="error" :title="error" type="error" show-icon closable @close="error = ''" class="error-alert" />

    <!-- 状态概览 -->
    <div v-if="managerStatus" class="stats-grid">
      <div class="stat-card">
        <div class="stat-label">备份配置</div>
        <div class="stat-value">{{ managerStatus.config_count }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">活跃任务</div>
        <div class="stat-value primary">{{ managerStatus.active_task_count }}</div>
      </div>
      <div class="stat-card">
        <div class="stat-label">监听状态</div>
        <div class="stat-value" :class="managerStatus.watcher_running ? 'success' : ''">
          {{ managerStatus.watcher_running ? '运行中' : '已停止' }}
        </div>
      </div>
      <div class="stat-card">
        <div class="stat-label">加密状态</div>
        <div class="stat-value" :class="managerStatus.encryption_enabled ? 'success' : ''">
          {{ managerStatus.encryption_enabled ? '已启用' : '未启用' }}
        </div>
      </div>
    </div>

    <!-- 加载状态 -->
    <div v-if="loading" class="loading-container">
      <el-icon :size="32" class="is-loading"><Loading /></el-icon>
    </div>

    <!-- 配置列表 -->
    <div v-else class="config-container">
      <el-empty v-if="configs.length === 0" description="暂无备份配置，点击「新建配置」创建第一个备份任务" />

      <div class="config-list">
        <el-card
            v-for="config in configs"
            :key="config.id"
            class="config-card"
            :class="{ 'is-upload': config.direction === 'upload' }"
            shadow="hover"
        >
          <!-- 配置头部 -->
          <div class="config-header">
            <div class="config-info">
              <div class="config-title">
                <el-icon :size="20" class="direction-icon">
                  <Upload v-if="config.direction === 'upload'" />
                  <Download v-else />
                </el-icon>
                <span class="config-name">{{ config.name }}</span>
                <el-tag :type="config.enabled ? 'success' : 'info'" size="small">
                  {{ config.enabled ? '已启用' : '已禁用' }}
                </el-tag>
                <el-tag v-if="config.encrypt_enabled" type="warning" size="small">
                  <el-icon :size="12"><Key /></el-icon> 加密
                </el-tag>
              </div>
              <div class="config-path">
                <template v-if="config.direction === 'upload'">
                  {{ config.local_path }} → {{ config.remote_path }}
                </template>
                <template v-else>
                  {{ config.remote_path }} → {{ config.local_path }}
                </template>
              </div>
            </div>

            <!-- 操作按钮 -->
            <div class="config-actions">
              <el-button size="small" type="success" @click.stop="handleTriggerBackup(config.id)">
                <el-icon><VideoPlay /></el-icon>
                手动备份
              </el-button>
              <el-button size="small" @click.stop="handleToggleConfig(config)">
                <el-icon v-if="config.enabled"><VideoPause /></el-icon>
                <el-icon v-else><VideoPlay /></el-icon>
                {{ config.enabled ? '禁用' : '启用' }}
              </el-button>
              <el-button size="small" type="danger" @click.stop="handleDeleteConfig(config.id)">
                <el-icon><Delete /></el-icon>
                删除
              </el-button>
            </div>
          </div>

          <!-- 活跃任务展示（直接显示，无需展开） -->
          <div v-if="activeTaskByConfig.get(config.id)" class="active-task-container">
            <div class="active-task-card" @click="openHistoryTasks(config)">
              <!-- 任务进度信息 -->
              <div class="task-progress-header">
                <div class="task-status-info">
                  <el-icon :size="18" :class="getStatusColor(activeTaskByConfig.get(config.id)!.status)" class="status-icon">
                    <component :is="getStatusIcon(activeTaskByConfig.get(config.id)!.status)" :class="{ 'is-loading': activeTaskByConfig.get(config.id)!.status === 'transferring' }" />
                  </el-icon>
                  <span class="task-status-text">{{ getStatusText(activeTaskByConfig.get(config.id)!.status) }}</span>
                </div>
                <div class="task-progress-stats">
                  <span class="task-files">{{ activeTaskByConfig.get(config.id)!.completed_count }}/{{ activeTaskByConfig.get(config.id)!.total_count }} 文件</span>
                  <span class="task-size">{{ formatBytes(activeTaskByConfig.get(config.id)!.transferred_bytes) }} / {{ formatBytes(activeTaskByConfig.get(config.id)!.total_bytes) }}</span>
                </div>
                <div class="task-actions" @click.stop>
                  <el-button
                      v-if="['queued', 'preparing', 'transferring'].includes(activeTaskByConfig.get(config.id)!.status)"
                      size="small"
                      @click="handlePauseTask(activeTaskByConfig.get(config.id)!.id, config.id)"
                  >
                    <el-icon><VideoPause /></el-icon>
                  </el-button>
                  <el-button
                      v-if="activeTaskByConfig.get(config.id)!.status === 'paused'"
                      size="small"
                      type="primary"
                      @click="handleResumeTask(activeTaskByConfig.get(config.id)!.id, config.id)"
                  >
                    <el-icon><VideoPlay /></el-icon>
                  </el-button>
                  <el-button
                      v-if="['queued', 'preparing', 'transferring', 'paused'].includes(activeTaskByConfig.get(config.id)!.status)"
                      size="small"
                      type="danger"
                      @click="handleCancelTask(activeTaskByConfig.get(config.id)!.id, config.id)"
                  >
                    <el-icon><Delete /></el-icon>
                  </el-button>
                </div>
              </div>

              <!-- 进度条 -->
              <div class="task-progress-bar">
                <el-progress
                    :percentage="calcBackupPercent(activeTaskByConfig.get(config.id)!)"
                    :stroke-width="6"
                    :show-text="false"
                    :status="activeTaskByConfig.get(config.id)!.status === 'paused' ? 'warning' : undefined"
                />
              </div>

              <!-- 文件任务列表（前5个） -->
              <div v-if="activeFileTasks.get(config.id)?.length" class="file-tasks-preview">
                <div
                    v-for="fileTask in activeFileTasks.get(config.id)"
                    :key="fileTask.id"
                    class="file-task-item"
                    :class="{ encrypting: fileTask.status === 'encrypting', decrypting: fileTask.status === 'decrypting' }"
                >
                  <div class="file-task-info">
                    <span class="file-name" :title="fileTask.local_path">{{ getFileName(fileTask.local_path) }}</span>
                    <!-- 加密/解密状态时显示进度百分比 -->
                    <span v-if="fileTask.status === 'encrypting' || fileTask.status === 'decrypting'" class="file-progress">
                      {{ (fileTask.status === 'encrypting' ? fileTask.encrypt_progress : fileTask.decrypt_progress)?.toFixed(1) || 0 }}%
                    </span>
                    <!-- 其他状态显示传输进度 -->
                    <span v-else class="file-size">
                      {{ calcFilePercent(fileTask) }}%（{{ formatFileTransferred(fileTask) }}）
                    </span>
                  </div>
                  <!-- 加密/解密进度条 -->
                  <el-progress
                      v-if="fileTask.status === 'encrypting' || fileTask.status === 'decrypting'"
                      :percentage="fileTask.status === 'encrypting' ? (fileTask.encrypt_progress || 0) : (fileTask.decrypt_progress || 0)"
                      :stroke-width="4"
                      :show-text="false"
                      status="warning"
                      class="encrypt-decrypt-progress"
                  />
                  <el-tag :type="getFileStatusColor(fileTask.status)" size="small">
                    {{ getFileStatusText(fileTask.status) }}
                  </el-tag>
                </div>
                <div v-if="activeTaskByConfig.get(config.id)!.total_count > 5" class="more-files">
                  还有 {{ activeTaskByConfig.get(config.id)!.total_count - 5 }} 个文件...
                </div>
              </div>
            </div>
          </div>

          <!-- 无活跃任务时显示查看历史入口 -->
          <div v-else class="no-active-task">
            <span class="idle-text">当前无备份任务</span>
            <el-button size="small" text type="primary" @click="openHistoryTasks(config)">
              查看历史记录
            </el-button>
          </div>
        </el-card>
      </div>
    </div>

    <!-- 创建配置对话框 -->
    <el-dialog v-model="showCreateDialog" title="新建备份配置" width="500px" :close-on-click-modal="false">
      <el-form label-position="top">
        <el-form-item label="配置名称">
          <el-input v-model="newConfig.name" placeholder="例如：文档备份" />
        </el-form-item>
        <el-form-item label="本地路径">
          <el-input
              v-model="newConfig.local_path"
              placeholder="点击选择本地目录"
              readonly
              @click="showLocalPathPicker = true"
          >
            <template #prefix>
              <el-icon><FolderOpened /></el-icon>
            </template>
          </el-input>
        </el-form-item>
        <el-form-item label="远程路径（百度网盘）">
          <NetdiskPathSelector
              v-model="newConfig.remote_path"
              :fs-id="remoteFsId"
              @update:fs-id="remoteFsId = $event"
          />
        </el-form-item>
        <el-form-item label="备份方向">
          <el-select v-model="newConfig.direction" style="width: 100%">
            <el-option value="upload" label="上传备份（本地 → 云端）" />
            <el-option value="download" label="下载备份（云端 → 本地）" />
          </el-select>
        </el-form-item>
        <el-form-item v-if="!isDownloadBackup">
          <div class="encrypt-switch-row">
            <span class="encrypt-label">启用加密</span>
            <el-switch
                v-model="newConfig.encrypt_enabled"
                :disabled="!encryptionStatus?.has_key"
            />
            <span v-if="!encryptionStatus?.has_key" class="hint-text">（需先在系统设置中配置加密密钥）</span>
          </div>
        </el-form-item>
        <el-alert v-if="encryptionStatus?.has_key" type="warning" :closable="false" show-icon>
          <template #title>加密选项在创建后不可更改。请在创建前确认是否需要加密备份。</template>
        </el-alert>
        <el-alert type="info" :closable="false" show-icon style="margin-top: 12px">
          <template #title>
            备份触发方式（文件监听、定时轮询）请在「系统设置 → 自动备份设置」中统一配置。
          </template>
        </el-alert>
        <el-alert type="warning" :closable="false" show-icon style="margin-top: 12px">
          <template #title>路径配置注意事项</template>
          <template #default>
            <div style="font-size: 12px; line-height: 1.8; margin-top: 4px; color: #606266;">
              <div style="margin-bottom: 6px;"><strong>1. 避免重复备份：</strong></div>
              <div style="padding-left: 12px; margin-bottom: 8px;">
                如果已创建「D:\文档  →  /备份/文档」的上传配置，就不能再创建「D:\文档\工作  →  /备份/文档/工作」，因为父目录配置已经包含了子目录的内容，会导致重复上传。
              </div>
              <div style="margin-bottom: 6px;"><strong>2. 避免循环同步：</strong></div>
              <div style="padding-left: 12px;">
                不能对相同的本地路径和云端路径同时创建上传和下载配置。例如已有「D:\同步  →  /云端同步」的上传配置，就不能再创建「/云端同步  →  D:\同步」的下载配置，否则文件会在本地和云端之间无限循环。
              </div>
            </div>
          </template>
        </el-alert>
      </el-form>
      <template #footer>
        <el-button @click="showCreateDialog = false">取消</el-button>
        <el-button type="primary" @click="handleCreateConfig">创建</el-button>
      </template>
    </el-dialog>

    <!-- 本地路径选择器 -->
    <FilePickerModal
        v-model="showLocalPathPicker"
        mode="select-directory"
        title="选择本地目录"
        confirm-text="确定"
        select-type="directory"
        @confirm="handleLocalPathConfirm"
    />

    <!-- 任务详情弹窗 -->
    <BackupTaskDetail
        v-model="showTaskDetail"
        :tasks="selectedTasks"
        :config-name="selectedConfigName"
        @pause="handleTaskDetailPause"
        @resume="handleTaskDetailResume"
        @cancel="handleTaskDetailCancel"
    />

  </div>
</template>

<style scoped lang="scss">
.autobackup-container {
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

.error-alert {
  margin: 16px 20px 0;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 16px;
  padding: 16px 20px;

  .stat-card {
    background: white;
    border-radius: 8px;
    padding: 16px;
    text-align: center;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);

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
    }
  }
}

.loading-container {
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 48px;
}

.config-container {
  flex: 1;
  padding: 0 20px 20px;
  overflow: auto;
}

.config-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.config-card {
  transition: all 0.3s;

  &.is-upload {
    border-left: 4px solid #409eff;
  }

  &:not(.is-upload) {
    border-left: 4px solid #67c23a;
  }

  &:hover {
    transform: translateY(-2px);
  }
}

.config-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}

.config-info {
  flex: 1;
  min-width: 0;
}

.config-title {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
  flex-wrap: wrap;

  .direction-icon {
    flex-shrink: 0;
    color: #409eff;
  }

  .config-name {
    font-size: 16px;
    font-weight: 500;
    color: #333;
  }
}

.config-path {
  font-size: 12px;
  color: #999;
  word-break: break-all;
}

.config-actions {
  display: flex;
  gap: 8px;
  flex-shrink: 0;
  flex-wrap: wrap;
}

// 活跃任务容器样式
.active-task-container {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid #ebeef5;
}

.active-task-card {
  background: #f5f7fa;
  border-radius: 8px;
  overflow: hidden;
  cursor: pointer;
  transition: all 0.2s;

  &:hover {
    background: #ecf5ff;
  }
}

.task-progress-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px;
  flex-wrap: wrap;
  gap: 12px;
}

.task-status-info {
  display: flex;
  align-items: center;
  gap: 8px;

  .task-status-text {
    font-size: 14px;
    font-weight: 500;
    color: #303133;
  }
}

.task-progress-stats {
  display: flex;
  align-items: center;
  gap: 12px;
  font-size: 13px;
  color: #606266;

  .task-files {
    color: #303133;
  }

  .task-size {
    color: #909399;
  }
}

// 无活跃任务时的样式
.no-active-task {
  margin-top: 12px;
  padding: 12px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: #f5f7fa;
  border-radius: 6px;

  .idle-text {
    font-size: 13px;
    color: #909399;
  }
}

.task-list-container {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid #ebeef5;
}

.task-list-header {
  font-size: 14px;
  font-weight: 500;
  color: #606266;
  margin-bottom: 12px;
}

.no-tasks {
  font-size: 13px;
  color: #909399;
}

.task-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.task-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px;
  background: #f5f7fa;
  border-radius: 8px;
  cursor: pointer;
  transition: background-color 0.2s;

  &:hover {
    background: #ecf5ff;
  }
}

.task-info {
  display: flex;
  align-items: center;
  gap: 12px;
}

.status-icon {
  &.text-green-500 {
    color: #67c23a;
  }
  &.text-yellow-500 {
    color: #e6a23c;
  }
  &.text-red-500 {
    color: #f56c6c;
  }
  &.text-blue-500 {
    color: #409eff;
  }
  &.text-gray-500 {
    color: #909399;
  }
}

.task-detail {
  .task-progress-text {
    font-size: 14px;
    color: #303133;

    .task-size {
      color: #909399;
      margin-left: 8px;
    }
  }

  .task-time {
    font-size: 12px;
    color: #909399;
    margin-top: 4px;
  }
}

.task-actions {
  display: flex;
  gap: 8px;
}

// 任务卡片样式
.task-card {
  background: #f5f7fa;
  border-radius: 8px;
  overflow: hidden;
  transition: all 0.2s;

  &:hover {
    background: #ecf5ff;
  }
}

.task-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px;
  cursor: pointer;
}

.task-progress-bar {
  padding: 0 12px 8px;
}

.task-skipped {
  color: #e6a23c;
  font-size: 12px;
  margin-left: 4px;
}

// 文件任务预览样式
.file-tasks-preview {
  padding: 0 12px 12px;
  border-top: 1px dashed #dcdfe6;
  margin-top: 4px;
}

.file-task-item {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 8px 0;
  border-bottom: 1px solid #ebeef5;

  &:last-child {
    border-bottom: none;
  }
}

.file-task-info {
  display: flex;
  align-items: center;
  gap: 12px;
  flex: 1;
  min-width: 0;

  .file-name {
    font-size: 13px;
    color: #303133;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 200px;
  }

  .file-size {
    font-size: 12px;
    color: #909399;
    flex-shrink: 0;
  }
}

.more-files {
  padding: 8px 0;
  font-size: 12px;
  color: #409eff;
  cursor: pointer;
  text-align: center;

  &:hover {
    text-decoration: underline;
  }
}

.hint-text {
  color: #909399;
  font-size: 12px;
}

.encrypt-switch-row {
  display: flex;
  align-items: center;
  gap: 12px;

  .encrypt-label {
    font-size: 14px;
    color: #606266;
  }
}

// 加解密状态样式
.file-task-item {
  &.encrypting,
  &.decrypting {
    background: #fdf6ec;
    border-radius: 4px;
    padding: 8px;
    margin: 4px 0;
  }
}

// 加解密进度条样式
.encrypt-decrypt-progress {
  width: 80px;
  flex-shrink: 0;
  margin: 0 12px;
}

// 加解密进度百分比文字
.file-progress {
  font-size: 12px;
  color: #e6a23c;
  font-weight: 500;
  flex-shrink: 0;
}

// 加解密状态标签动画
:deep(.el-tag) {
  &.el-tag--primary {
    .el-icon {
      animation: pulse 1.5s infinite;
    }
  }
}

@keyframes pulse {
  0%, 100% { opacity: 1; }
  50% { opacity: 0.5; }
}

// 加载动画
.is-loading {
  animation: spin 1s linear infinite;
}

@keyframes spin {
  from {
    transform: rotate(0deg);
  }
  to {
    transform: rotate(360deg);
  }
}

// 移动端适配
.is-mobile {
  .toolbar {
    padding: 12px 16px;

    .header-left {
      gap: 12px;
    }
  }

  .stats-grid {
    grid-template-columns: repeat(2, 1fr);
    padding: 12px 16px;
    gap: 12px;

    .stat-card {
      padding: 12px;

      .stat-value {
        font-size: 20px;
      }
    }
  }

  .config-container {
    padding: 0 16px 16px;
  }

  .config-header {
    flex-direction: column;
    gap: 12px;
  }

  .config-actions {
    width: 100%;
    justify-content: flex-start;
  }

  .task-item {
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }

  .task-actions {
    width: 100%;
    justify-content: flex-end;
  }
}

// 移动端对话框适配
@media (max-width: 767px) {
  :deep(.el-dialog) {
    width: 95% !important;
    margin: 3vh auto !important;
  }

  .stats-grid {
    grid-template-columns: repeat(2, 1fr);
  }
}

@media (max-width: 480px) {
  .stats-grid {
    grid-template-columns: 1fr;
  }
}
</style>
