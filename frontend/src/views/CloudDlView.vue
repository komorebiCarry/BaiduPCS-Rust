<template>
  <div class="cloud-dl-view" :class="{ 'is-mobile': isMobile }">
    <!-- 顶部工具栏 -->
    <div class="toolbar">
      <div class="header-left">
        <h2 v-if="!isMobile">离线下载</h2>
        <el-tag :type="activeCountType" size="large">
          {{ activeCount }} 个任务进行中
        </el-tag>
      </div>
      <div class="header-right">
        <!-- PC端按钮 -->
        <template v-if="!isMobile">
          <el-button type="primary" @click="showAddDialog = true">
            <el-icon><Plus /></el-icon>
            添加任务
          </el-button>
          <el-button @click="handleRefresh" :loading="isRefreshing">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
        </template>
        <!-- 移动端按钮 -->
        <template v-else>
          <el-button type="primary" circle @click="showAddDialog = true">
            <el-icon><Plus /></el-icon>
          </el-button>
          <el-button circle @click="handleRefresh" :loading="isRefreshing">
            <el-icon><Refresh /></el-icon>
          </el-button>
        </template>
      </div>
    </div>

    <!-- PC端任务列表表格 -->
    <div v-if="!isMobile" class="task-container">
      <el-empty v-if="!loading && tasks.length === 0" description="暂无离线下载任务" />
      <el-table v-else :data="tasks" v-loading="loading" style="width: 100%">
        <el-table-column prop="task_name" label="任务名称" min-width="200" show-overflow-tooltip />
        <el-table-column label="状态" width="120">
          <template #default="{ row }">
            <el-tag :type="getStatusType(row.status)">{{ row.status_text || getStatusText(row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="save_path" label="保存路径" min-width="150" show-overflow-tooltip />
        <el-table-column label="创建时间" width="180">
          <template #default="{ row }">{{ formatTimestamp(row.create_time) }}</template>
        </el-table-column>
        <el-table-column label="操作" width="200" fixed="right">
          <template #default="{ row }">
            <el-button size="small" type="primary" plain @click="handleShowDetail(row)">详情</el-button>
            <el-button v-if="row.status === 1" size="small" @click="handleCancel(row)">取消</el-button>
            <el-button size="small" type="danger" @click="handleDelete(row)">删除</el-button>
          </template>
        </el-table-column>
      </el-table>
    </div>

    <!-- 移动端任务卡片列表 -->
    <div v-else class="task-cards">
      <el-empty v-if="!loading && tasks.length === 0" description="暂无离线下载任务" />
      <div
          v-for="task in tasks"
          :key="task.task_id"
          class="task-card"
          :class="getTaskCardClass(task.status)"
          @click="handleShowDetail(task)"
      >
        <div class="task-header">
          <span class="task-name">{{ task.task_name }}</span>
          <el-tag :type="getStatusType(task.status)" size="small">
            {{ task.status_text || getStatusText(task.status) }}
          </el-tag>
        </div>
        <div class="task-info">
          <div class="info-row">
            <span class="label">路径:</span>
            <span class="path">{{ task.save_path }}</span>
          </div>
          <div class="info-row">
            <span class="label">时间:</span>
            <span>{{ formatTimestamp(task.create_time) }}</span>
          </div>
        </div>
        <div class="task-actions" @click.stop>
          <el-button v-if="task.status === 1" size="small" @click="handleCancel(task)">取消</el-button>
          <el-button size="small" type="danger" @click="handleDelete(task)">删除</el-button>
        </div>
      </div>
    </div>

    <!-- 任务详情弹窗 -->
    <el-dialog
        v-model="showDetailDialog"
        title="任务详情"
        :width="isMobile ? '95%' : '600px'"
        :fullscreen="isMobile"
        @close="detailTask = null"
    >
      <div v-if="detailLoading" class="detail-loading">
        <el-icon class="is-loading"><Loading /></el-icon>
        <span>加载中...</span>
      </div>
      <div v-else-if="detailTask" class="task-detail">
        <div class="detail-header">
          <h3 class="detail-title">{{ detailTask.task_name }}</h3>
          <el-tag :type="getStatusType(detailTask.status)" size="large">
            {{ detailTask.status_text || getStatusText(detailTask.status) }}
          </el-tag>
        </div>

        <!-- 进度条（仅进行中任务显示） -->
        <div v-if="detailTask.status === 1" class="detail-progress">
          <el-progress
              :percentage="getProgress(detailTask)"
              :stroke-width="10"
              :color="'#e6a23c'"
          >
            <template #default="{ percentage }">
              <span class="progress-text">{{ percentage.toFixed(1) }}%</span>
            </template>
          </el-progress>
          <div class="progress-info">
            {{ formatFileSize(detailTask.finished_size) }} / {{ formatFileSize(detailTask.file_size) }}
          </div>
        </div>

        <!-- 基本信息 -->
        <div class="detail-section">
          <h4>基本信息</h4>
          <div class="detail-grid">
            <div class="detail-item">
              <span class="detail-label">任务ID</span>
              <span class="detail-value">{{ detailTask.task_id }}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">文件大小</span>
              <span class="detail-value">{{ formatFileSize(detailTask.file_size) || '未知' }}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">已下载</span>
              <span class="detail-value">{{ formatFileSize(detailTask.finished_size) }}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">保存路径</span>
              <span class="detail-value path">{{ detailTask.save_path }}</span>
            </div>
            <div class="detail-item">
              <span class="detail-label">创建时间</span>
              <span class="detail-value">{{ formatTimestamp(detailTask.create_time) }}</span>
            </div>
            <div class="detail-item" v-if="detailTask.start_time > 0">
              <span class="detail-label">开始时间</span>
              <span class="detail-value">{{ formatTimestamp(detailTask.start_time) }}</span>
            </div>
            <div class="detail-item" v-if="detailTask.finish_time > 0">
              <span class="detail-label">完成时间</span>
              <span class="detail-value">{{ formatTimestamp(detailTask.finish_time) }}</span>
            </div>
          </div>
        </div>

        <!-- 下载链接 -->
        <div class="detail-section">
          <h4>下载链接</h4>
          <div class="source-url">{{ detailTask.source_url }}</div>
        </div>

        <!-- 文件列表 -->
        <div v-if="detailTask.file_list && detailTask.file_list.length > 0" class="detail-section">
          <h4>文件列表 ({{ detailTask.file_list.length }} 个文件)</h4>
          <div class="file-list">
            <div v-for="(file, index) in detailTask.file_list" :key="index" class="file-item">
              <el-icon><Document /></el-icon>
              <span class="file-name">{{ file.file_name }}</span>
              <span class="file-size">{{ formatFileSize(file.file_size) }}</span>
            </div>
          </div>
        </div>
      </div>
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="showDetailDialog = false">关闭</el-button>
          <el-button v-if="detailTask && detailTask.status === 1" @click="handleCancelFromDetail">取消任务</el-button>
          <el-button v-if="detailTask" type="danger" @click="handleDeleteFromDetail">删除任务</el-button>
        </div>
      </template>
    </el-dialog>

    <!-- 添加任务对话框 -->
    <el-dialog
        v-model="showAddDialog"
        title="添加离线下载任务"
        :width="isMobile ? '95%' : '550px'"
        :fullscreen="isMobile"
        @close="resetAddForm"
    >
      <el-form :model="addForm" label-width="100px" :label-position="isMobile ? 'top' : 'right'">
        <el-form-item label="下载链接" required>
          <el-input
              v-model="addForm.source_url"
              type="textarea"
              :rows="3"
              placeholder="请输入下载链接（支持 HTTP/HTTPS/磁力链接/ed2k）"
          />
        </el-form-item>
        <el-form-item label="保存路径">
          <div class="path-selector">
            <el-input v-model="addForm.save_path" placeholder="默认保存到根目录" readonly />
            <el-button @click="showPathSelector = true">选择</el-button>
          </div>
        </el-form-item>
        <el-form-item label="自动下载">
          <el-switch v-model="addForm.auto_download" />
          <span class="auto-download-hint">完成后自动下载到本地</span>
        </el-form-item>
        <el-form-item v-if="addForm.auto_download" label="下载目录">
          <div class="path-selector">
            <el-input v-model="addForm.local_download_path" placeholder="选择本地下载目录" readonly />
            <el-button @click="showLocalPathSelector = true">选择</el-button>
          </div>
          <el-checkbox v-model="addForm.ask_download_path" class="ask-path-checkbox">
            每次询问下载目录
          </el-checkbox>
        </el-form-item>
      </el-form>
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="showAddDialog = false">取消</el-button>
          <el-button type="primary" @click="handleAddTask" :loading="adding" :disabled="!addForm.source_url.trim()">
            确定
          </el-button>
        </div>
      </template>
    </el-dialog>

    <!-- 网盘路径选择器 -->
    <NetdiskPathSelector
        v-model="addForm.save_path"
        :fs-id="addForm.save_path_fs_id"
        @update:fs-id="addForm.save_path_fs_id = $event"
        ref="netdiskPathSelectorRef"
    />
    <el-dialog
        v-model="showPathSelector"
        title="选择网盘保存路径"
        :width="isMobile ? '95%' : '600px'"
        @open="handlePathSelectorOpen"
    >
      <NetdiskPathSelector
          v-model="tempSavePath"
          :fs-id="tempSavePathFsId"
          @update:fs-id="tempSavePathFsId = $event"
      />
      <template #footer>
        <div class="dialog-footer">
          <el-button @click="showPathSelector = false">取消</el-button>
          <el-button type="primary" @click="confirmSavePath">确定</el-button>
        </div>
      </template>
    </el-dialog>

    <!-- 本地目录选择器 -->
    <FilePickerModal
        v-model="showLocalPathSelector"
        mode="select-directory"
        title="选择本地下载目录"
        confirm-text="选择"
        :initial-path="downloadConfig?.recent_directory || downloadConfig?.default_directory"
        @confirm="handleLocalPathSelect"
    />

    <!-- 自动下载确认弹窗 -->
    <FilePickerModal
        v-model="showAutoDownloadPicker"
        mode="download"
        title="选择下载目录"
        :default-download-dir="autoDownloadDefaultDir || downloadConfig?.default_directory"
        :initial-path="downloadConfig?.recent_directory || downloadConfig?.default_directory"
        @confirm-download="handleAutoDownloadConfirm"
        @use-default="handleAutoDownloadUseDefault"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, Refresh, Loading, Document } from '@element-plus/icons-vue'
import { useIsMobile } from '@/utils/responsive'
import { formatFileSize } from '@/utils/fileUtils'
import NetdiskPathSelector from '@/components/NetdiskPathSelector.vue'
import { FilePickerModal } from '@/components/FilePicker'
import {
  listTasks,
  addTask,
  cancelTask,
  deleteTask,
  queryTask,
  getStatusText,
  getStatusType,
  calculateProgress,
  formatTimestamp,
  type CloudDlTaskInfo,
  type AddTaskRequest,
  CloudDlTaskStatus,
} from '@/api/cloudDl'
import {
  useCloudDlWebSocket,
  type CloudDlTaskCompletedEvent,
  type CloudDlTaskListRefreshedEvent,
  type CloudDlProgressUpdateEvent,
  type CloudDlStatusChangedEvent,
} from '@/composables/useCloudDlWebSocket'
import { createBatchDownload, type BatchDownloadItem } from '@/api/download'
import { getConfig, updateRecentDirDebounced, updateTransferConfig, type DownloadConfig, type UploadConfig, type TransferConfig } from '@/api/config'

// 响应式检测
const isMobile = useIsMobile()

// 配置状态
const downloadConfig = ref<DownloadConfig | null>(null)
const uploadConfig = ref<UploadConfig | null>(null)
const transferConfig = ref<TransferConfig | null>(null)

// 状态
const loading = ref(false)
const adding = ref(false)
const tasks = ref<CloudDlTaskInfo[]>([])
const showAddDialog = ref(false)
const showPathSelector = ref(false)
const showLocalPathSelector = ref(false)
const showAutoDownloadPicker = ref(false)

// 详情弹窗状态
const showDetailDialog = ref(false)
const detailLoading = ref(false)
const detailTask = ref<CloudDlTaskInfo | null>(null)

// 添加任务表单 - 使用明确的类型定义避免 undefined 问题
interface AddTaskFormData {
  source_url: string
  save_path: string
  save_path_fs_id: number
  auto_download: boolean
  local_download_path: string
  ask_download_path: boolean
}

const addForm = ref<AddTaskFormData>({
  source_url: '',
  save_path: '/',
  save_path_fs_id: 0,
  auto_download: false,
  local_download_path: '',
  ask_download_path: false,
})

// 临时路径选择
const tempSavePath = ref('/')
const tempSavePathFsId = ref(0)

// 自动下载相关
const autoDownloadDefaultDir = ref('')
const pendingAutoDownloadTask = ref<CloudDlTaskInfo | null>(null)

// 自动下载配置存储（task_id -> config）
const autoDownloadConfigs = ref<Map<number, { localPath: string; askEachTime: boolean }>>(new Map())

// 计算属性
const activeCount = computed(() => {
  return tasks.value.filter(task => task.status === CloudDlTaskStatus.Running).length
})

const activeCountType = computed(() => {
  if (activeCount.value === 0) return 'info'
  if (activeCount.value <= 3) return 'success'
  return 'warning'
})

// WebSocket 订阅
const { isSubscribed, isRefreshing, refresh } = useCloudDlWebSocket({
  onStatusChanged: handleStatusChanged,
  onTaskCompleted: handleTaskCompleted,
  onProgressUpdate: handleProgressUpdate,
  onTaskListRefreshed: handleTaskListRefreshed,
})

// 事件处理函数
function handleStatusChanged(event: CloudDlStatusChangedEvent) {
  const index = tasks.value.findIndex(t => t.task_id === event.task_id)
  if (index !== -1) {
    tasks.value[index] = event.task
  }
  // 同步更新详情弹窗中的任务
  if (detailTask.value && detailTask.value.task_id === event.task_id) {
    detailTask.value = event.task
  }
}

function handleTaskCompleted(event: CloudDlTaskCompletedEvent) {
  const index = tasks.value.findIndex(t => t.task_id === event.task_id)
  if (index !== -1) {
    tasks.value[index] = event.task
  }

  // 检查是否需要自动下载
  // 注意：当 askEachTime 为 false 时，后端已经自动执行了下载，前端不需要重复执行
  // 前端只在 askEachTime 为 true 时弹窗让用户选择目录
  const config = autoDownloadConfigs.value.get(event.task_id)
  if (config) {
    if (config.askEachTime) {
      // 弹窗询问下载目录（后端不会自动下载，需要前端处理）
      pendingAutoDownloadTask.value = event.task
      autoDownloadDefaultDir.value = config.localPath || ''
      showAutoDownloadPicker.value = true
    }
    // 注意：当 askEachTime 为 false 时，后端已经执行了自动下载，前端不需要再调用 triggerAutoDownload
    // 清除配置
    autoDownloadConfigs.value.delete(event.task_id)
  }
}

function handleProgressUpdate(event: CloudDlProgressUpdateEvent) {
  const index = tasks.value.findIndex(t => t.task_id === event.task_id)
  if (index !== -1) {
    tasks.value[index].finished_size = event.finished_size
    tasks.value[index].file_size = event.file_size
  }
  // 同步更新详情弹窗中的任务进度
  if (detailTask.value && detailTask.value.task_id === event.task_id) {
    detailTask.value.finished_size = event.finished_size
    detailTask.value.file_size = event.file_size
  }
}

function handleTaskListRefreshed(event: CloudDlTaskListRefreshedEvent) {
  tasks.value = event.tasks
}

// 获取进度百分比
function getProgress(task: CloudDlTaskInfo): number {
  return calculateProgress(task)
}

// 获取任务卡片状态类名
function getTaskCardClass(status: number): string {
  switch (status) {
    case CloudDlTaskStatus.Success:
      return 'status-success'
    case CloudDlTaskStatus.Running:
      return 'status-running'
    case CloudDlTaskStatus.SystemError:
    case CloudDlTaskStatus.ResourceNotFound:
    case CloudDlTaskStatus.Timeout:
    case CloudDlTaskStatus.DownloadFailed:
    case CloudDlTaskStatus.InsufficientSpace:
      return 'status-error'
    default:
      return ''
  }
}

// 显示任务详情
async function handleShowDetail(task: CloudDlTaskInfo) {
  showDetailDialog.value = true
  detailLoading.value = true
  detailTask.value = null

  try {
    const detail = await queryTask(task.task_id)
    detailTask.value = detail
  } catch (error: any) {
    ElMessage.error('获取任务详情失败: ' + (error.message || error))
    // 如果获取详情失败，使用列表中的基本信息
    detailTask.value = task
  } finally {
    detailLoading.value = false
  }
}

// 从详情弹窗取消任务
async function handleCancelFromDetail() {
  if (!detailTask.value) return

  try {
    await ElMessageBox.confirm(
        `确定要取消任务 "${detailTask.value.task_name}" 吗？`,
        '取消确认',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )

    await cancelTask(detailTask.value.task_id)
    ElMessage.success('任务已取消')
    showDetailDialog.value = false
    await handleRefresh()
  } catch (error: any) {
    if (error !== 'cancel') {
      ElMessage.error('取消任务失败: ' + (error.message || error))
    }
  }
}

// 从详情弹窗删除任务
async function handleDeleteFromDetail() {
  if (!detailTask.value) return

  try {
    await ElMessageBox.confirm(
        `确定要删除任务 "${detailTask.value.task_name}" 吗？`,
        '删除确认',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )

    await deleteTask(detailTask.value.task_id)
    ElMessage.success('任务已删除')
    showDetailDialog.value = false
    await handleRefresh()
  } catch (error: any) {
    if (error !== 'cancel') {
      ElMessage.error('删除任务失败: ' + (error.message || error))
    }
  }
}

// 刷新任务列表
async function handleRefresh() {
  try {
    const result = await refresh()
    tasks.value = result
  } catch (error: any) {
    ElMessage.error('刷新失败: ' + (error.message || error))
  }
}

// 加载任务列表
async function loadTasks() {
  loading.value = true
  try {
    const response = await listTasks()
    tasks.value = response.tasks
  } catch (error: any) {
    ElMessage.error('加载任务列表失败: ' + (error.message || error))
  } finally {
    loading.value = false
  }
}

// 重置添加表单
function resetAddForm() {
  addForm.value = {
    source_url: '',
    save_path: '/',
    save_path_fs_id: 0,
    auto_download: false,
    local_download_path: '',
    ask_download_path: false,
  }
}

// 路径选择器打开
function handlePathSelectorOpen() {
  // 优先使用当前表单中的路径，否则使用最近保存的网盘路径
  if (addForm.value.save_path && addForm.value.save_path !== '/') {
    tempSavePath.value = addForm.value.save_path
    tempSavePathFsId.value = addForm.value.save_path_fs_id
  } else if (transferConfig.value?.recent_save_path) {
    // 使用转存配置中的最近保存路径
    tempSavePath.value = transferConfig.value.recent_save_path
    tempSavePathFsId.value = transferConfig.value.recent_save_fs_id || 0
  } else {
    tempSavePath.value = '/'
    tempSavePathFsId.value = 0
  }
}

// 确认保存路径
function confirmSavePath() {
  addForm.value.save_path = tempSavePath.value
  addForm.value.save_path_fs_id = tempSavePathFsId.value
  showPathSelector.value = false
}

// 本地路径选择
function handleLocalPathSelect(path: string) {
  addForm.value.local_download_path = path
  // 更新最近下载目录
  updateRecentDirDebounced({ dir_type: 'download', path })
  if (downloadConfig.value) {
    downloadConfig.value.recent_directory = path
  }
}

// 添加任务
async function handleAddTask() {
  const sourceUrl = addForm.value.source_url.trim()
  if (!sourceUrl) {
    ElMessage.warning('请输入下载链接')
    return
  }

  adding.value = true
  try {
    const response = await addTask({
      source_url: sourceUrl,
      save_path: addForm.value.save_path || '/',
      auto_download: addForm.value.auto_download,
      local_download_path: addForm.value.local_download_path || undefined,
      ask_download_path: addForm.value.ask_download_path,
    })

    ElMessage.success(`任务添加成功，任务ID: ${response.task_id}`)

    // 更新最近保存的网盘路径
    if (addForm.value.save_path && addForm.value.save_path !== '/') {
      updateTransferConfig({
        recent_save_fs_id: addForm.value.save_path_fs_id,
        recent_save_path: addForm.value.save_path,
      }).catch(err => console.error('更新最近保存路径失败:', err))

      if (transferConfig.value) {
        transferConfig.value.recent_save_fs_id = addForm.value.save_path_fs_id
        transferConfig.value.recent_save_path = addForm.value.save_path
      }
    }

    // 自动下载配置已通过 API 传递给后端，后端会注册到监听服务
    // 前端也保存一份用于 UI 显示
    if (addForm.value.auto_download) {
      autoDownloadConfigs.value.set(response.task_id, {
        localPath: addForm.value.local_download_path,
        askEachTime: addForm.value.ask_download_path,
      })
    }

    showAddDialog.value = false
    resetAddForm()

    // 刷新任务列表
    await handleRefresh()
  } catch (error: any) {
    ElMessage.error('添加任务失败: ' + (error.message || error))
  } finally {
    adding.value = false
  }
}

// 取消任务
async function handleCancel(task: CloudDlTaskInfo) {
  try {
    await ElMessageBox.confirm(
        `确定要取消任务 "${task.task_name}" 吗？`,
        '取消确认',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )

    await cancelTask(task.task_id)
    ElMessage.success('任务已取消')
    await handleRefresh()
  } catch (error: any) {
    if (error !== 'cancel') {
      ElMessage.error('取消任务失败: ' + (error.message || error))
    }
  }
}

// 删除任务
async function handleDelete(task: CloudDlTaskInfo) {
  try {
    await ElMessageBox.confirm(
        `确定要删除任务 "${task.task_name}" 吗？`,
        '删除确认',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )

    await deleteTask(task.task_id)
    ElMessage.success('任务已删除')
    await handleRefresh()
  } catch (error: any) {
    if (error !== 'cancel') {
      ElMessage.error('删除任务失败: ' + (error.message || error))
    }
  }
}

// 自动下载确认
function handleAutoDownloadConfirm(payload: { path: string; setAsDefault: boolean }) {
  if (pendingAutoDownloadTask.value) {
    triggerAutoDownload(pendingAutoDownloadTask.value, payload.path)
    pendingAutoDownloadTask.value = null
    // 更新最近下载目录
    updateRecentDirDebounced({ dir_type: 'download', path: payload.path })
    if (downloadConfig.value) {
      downloadConfig.value.recent_directory = payload.path
    }
  }
}

// 使用默认目录自动下载
function handleAutoDownloadUseDefault() {
  if (pendingAutoDownloadTask.value && autoDownloadDefaultDir.value) {
    triggerAutoDownload(pendingAutoDownloadTask.value, autoDownloadDefaultDir.value)
    pendingAutoDownloadTask.value = null
  }
}

// 触发自动下载
async function triggerAutoDownload(task: CloudDlTaskInfo, localPath: string) {
  try {
    // 构建下载项
    const items: BatchDownloadItem[] = task.file_list.map(file => ({
      fs_id: 0, // 离线下载完成的文件需要通过路径获取 fs_id
      path: `${task.save_path}/${file.file_name}`,
      name: file.file_name,
      is_dir: false,
      size: file.file_size,
    }))

    if (items.length === 0) {
      ElMessage.warning('没有可下载的文件')
      return
    }

    const response = await createBatchDownload({
      items,
      target_dir: localPath,
    })

    if (response.task_ids.length > 0 || response.folder_task_ids.length > 0) {
      ElMessage.success(`已创建 ${response.task_ids.length + response.folder_task_ids.length} 个下载任务`)
    }

    if (response.failed.length > 0) {
      ElMessage.warning(`${response.failed.length} 个文件下载失败`)
    }
  } catch (error: any) {
    ElMessage.error('自动下载失败: ' + (error.message || error))
  }
}

// 生命周期
onMounted(async () => {
  // 加载配置
  try {
    const config = await getConfig()
    downloadConfig.value = config.download
    uploadConfig.value = config.upload
    transferConfig.value = config.transfer || null
  } catch (error: any) {
    console.error('加载配置失败:', error)
  }

  // 加载任务列表
  loadTasks()
})
</script>

<style scoped lang="scss">
.cloud-dl-view {
  height: 100%;
  display: flex;
  flex-direction: column;
  padding: 20px;
  background: #f5f5f5;
  overflow: hidden;

  &.is-mobile {
    padding: 12px;
  }
}

.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  flex-shrink: 0;

  .header-left {
    display: flex;
    align-items: center;
    gap: 16px;

    h2 {
      margin: 0;
      font-size: 20px;
      color: #333;
    }
  }

  .header-right {
    display: flex;
    gap: 8px;
  }
}

.task-container {
  flex: 1;
  background: white;
  border-radius: 8px;
  padding: 16px;
  overflow: auto;
}

// =====================
// 任务详情弹窗样式
// =====================
.detail-loading {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  padding: 40px;
  color: #909399;

  .el-icon {
    font-size: 32px;
    margin-bottom: 12px;
  }
}

.task-detail {
  .detail-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 16px;
    margin-bottom: 20px;
    padding-bottom: 16px;
    border-bottom: 1px solid #ebeef5;

    .detail-title {
      flex: 1;
      margin: 0;
      font-size: 18px;
      font-weight: 600;
      color: #303133;
      word-break: break-all;
      line-height: 1.4;
    }
  }

  .detail-progress {
    margin-bottom: 20px;
    padding: 16px;
    background: #fdf6ec;
    border-radius: 8px;

    .progress-info {
      margin-top: 8px;
      text-align: center;
      font-size: 13px;
      color: #e6a23c;
    }
  }

  .detail-section {
    margin-bottom: 20px;

    h4 {
      margin: 0 0 12px 0;
      font-size: 14px;
      font-weight: 600;
      color: #606266;
    }
  }

  .detail-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 12px;

    .detail-item {
      display: flex;
      flex-direction: column;
      gap: 4px;
      padding: 10px 12px;
      background: #f8f9fa;
      border-radius: 6px;

      .detail-label {
        font-size: 12px;
        color: #909399;
      }

      .detail-value {
        font-size: 14px;
        color: #303133;
        word-break: break-all;

        &.path {
          color: #409eff;
        }
      }
    }
  }

  .source-url {
    padding: 12px;
    background: #f8f9fa;
    border-radius: 6px;
    font-size: 13px;
    color: #606266;
    word-break: break-all;
    line-height: 1.5;
    max-height: 100px;
    overflow-y: auto;
  }

  .file-list {
    max-height: 200px;
    overflow-y: auto;
    border: 1px solid #ebeef5;
    border-radius: 6px;

    .file-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 10px 12px;
      border-bottom: 1px solid #ebeef5;

      &:last-child {
        border-bottom: none;
      }

      .el-icon {
        color: #909399;
        flex-shrink: 0;
      }

      .file-name {
        flex: 1;
        font-size: 13px;
        color: #303133;
        word-break: break-all;
      }

      .file-size {
        flex-shrink: 0;
        font-size: 12px;
        color: #909399;
      }
    }
  }
}

// =====================
// 移动端任务卡片样式
// =====================
.task-cards {
  flex: 1;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 12px;
  padding-bottom: 16px;

  // 隐藏滚动条但保留滚动功能
  -ms-overflow-style: none;
  scrollbar-width: none;

  &::-webkit-scrollbar {
    display: none;
  }
}

.task-card {
  background: white;
  border-radius: 12px;
  padding: 16px;
  box-shadow: 0 2px 12px rgba(0, 0, 0, 0.08);
  transition: all 0.2s ease;
  border: 1px solid transparent;

  &:active {
    transform: scale(0.98);
    box-shadow: 0 1px 6px rgba(0, 0, 0, 0.06);
  }

  .task-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 14px;
    gap: 12px;

    .task-name {
      flex: 1;
      font-size: 15px;
      font-weight: 600;
      color: #303133;
      word-break: break-all;
      line-height: 1.4;
      display: -webkit-box;
      -webkit-line-clamp: 2;
      -webkit-box-orient: vertical;
      overflow: hidden;
    }

    .el-tag {
      flex-shrink: 0;
      border-radius: 6px;
      font-weight: 500;
    }
  }

  .task-info {
    margin-bottom: 14px;
    background: #f8f9fa;
    border-radius: 8px;
    padding: 12px;

    .info-row {
      display: flex;
      font-size: 13px;
      margin-bottom: 8px;
      line-height: 1.5;

      &:last-child {
        margin-bottom: 0;
      }

      .label {
        color: #909399;
        width: 45px;
        flex-shrink: 0;
        font-weight: 500;
      }

      span:not(.label) {
        color: #606266;
        flex: 1;
      }

      .path {
        color: #409eff;
        word-break: break-all;
        display: -webkit-box;
        -webkit-line-clamp: 1;
        -webkit-box-orient: vertical;
        overflow: hidden;
      }
    }
  }

  .task-progress {
    margin-bottom: 14px;

    :deep(.el-progress-bar__outer) {
      border-radius: 4px;
      background: #e9ecef;
    }

    :deep(.el-progress-bar__inner) {
      border-radius: 4px;
    }
  }

  .task-actions {
    display: flex;
    justify-content: flex-end;
    gap: 10px;
    padding-top: 12px;
    border-top: 1px solid #f0f0f0;

    .el-button {
      min-width: 70px;
      border-radius: 6px;
      font-weight: 500;

      &--small {
        height: 32px;
        font-size: 13px;
      }
    }
  }

  // 不同状态的卡片边框颜色
  &.status-running {
    border-color: rgba(230, 162, 60, 0.3);
  }

  &.status-success {
    border-color: rgba(103, 194, 58, 0.3);
  }

  &.status-error {
    border-color: rgba(245, 108, 108, 0.3);
  }
}

// 进度文本
.progress-text {
  font-size: 12px;
  color: #606266;
}

// 添加任务对话框样式
.path-selector {
  display: flex;
  gap: 8px;
  width: 100%;

  .el-input {
    flex: 1;
  }
}

.auto-download-hint {
  margin-left: 12px;
  font-size: 13px;
  color: #909399;
}

.ask-path-checkbox {
  margin-top: 8px;
  display: block;
}

// =====================
// 对话框样式优化
// =====================

// 对话框基础样式
:deep(.el-dialog) {
  border-radius: 12px;
  overflow: hidden;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.12);

  // 对话框头部
  .el-dialog__header {
    background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);
    border-bottom: 1px solid #ebeef5;
    padding: 18px 24px;
    margin-right: 0;

    .el-dialog__title {
      font-size: 17px;
      font-weight: 600;
      color: #303133;
      letter-spacing: 0.3px;
    }

    .el-dialog__headerbtn {
      top: 18px;
      right: 20px;
      width: 28px;
      height: 28px;

      .el-dialog__close {
        font-size: 16px;
        color: #909399;
        transition: all 0.2s ease;

        &:hover {
          color: #409eff;
          transform: rotate(90deg);
        }
      }
    }
  }

  // 对话框内容区
  .el-dialog__body {
    padding: 24px;
    background: #ffffff;
  }

  // 对话框底部
  .el-dialog__footer {
    background: #fafafa;
    border-top: 1px solid #ebeef5;
    padding: 14px 24px;
  }
}

// 表单项样式
:deep(.el-form-item) {
  margin-bottom: 22px;

  &:last-child {
    margin-bottom: 0;
  }

  .el-form-item__label {
    font-weight: 500;
    color: #606266;
    font-size: 14px;

    &::before {
      color: #f56c6c;
    }
  }

  .el-form-item__content {
    line-height: 1.5;
  }
}

// 输入框样式
:deep(.el-input) {
  .el-input__wrapper {
    border-radius: 6px;
    transition: all 0.2s ease;
    box-shadow: 0 0 0 1px #dcdfe6 inset;

    &:hover {
      box-shadow: 0 0 0 1px #c0c4cc inset;
    }

    &.is-focus {
      box-shadow: 0 0 0 1px #409eff inset;
    }
  }

  .el-input__inner {
    font-size: 14px;
    color: #303133;

    &::placeholder {
      color: #c0c4cc;
    }
  }
}

// 文本域样式
:deep(.el-textarea) {
  .el-textarea__inner {
    font-family: inherit;
    font-size: 14px;
    line-height: 1.6;
    border-radius: 6px;
    padding: 10px 12px;
    transition: all 0.2s ease;

    &::placeholder {
      color: #c0c4cc;
    }

    &:hover {
      border-color: #c0c4cc;
    }

    &:focus {
      border-color: #409eff;
      box-shadow: 0 0 0 2px rgba(64, 158, 255, 0.1);
    }
  }
}

// 开关组件样式
:deep(.el-switch) {
  --el-switch-on-color: #409eff;
  --el-switch-off-color: #dcdfe6;

  .el-switch__core {
    border-radius: 12px;
    transition: all 0.2s ease;
  }
}

// 复选框样式
:deep(.el-checkbox) {
  .el-checkbox__label {
    font-size: 13px;
    color: #606266;
  }

  .el-checkbox__input.is-checked + .el-checkbox__label {
    color: #409eff;
  }
}

// 对话框底部按钮样式
.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;

  .el-button {
    min-width: 80px;
    border-radius: 6px;
    font-weight: 500;
    transition: all 0.2s ease;

    &--primary {
      box-shadow: 0 2px 8px rgba(64, 158, 255, 0.3);

      &:hover {
        box-shadow: 0 4px 12px rgba(64, 158, 255, 0.4);
        transform: translateY(-1px);
      }

      &:active {
        transform: translateY(0);
      }
    }

    &--default {
      &:hover {
        border-color: #409eff;
        color: #409eff;
      }
    }
  }
}

// =====================
// 移动端响应式调整
// =====================
@media (max-width: 767px) {
  .toolbar {
    flex-wrap: wrap;
    gap: 12px;

    .header-left {
      width: 100%;
      justify-content: space-between;
    }

    .header-right {
      width: 100%;
      justify-content: flex-end;
    }
  }

  // 移动端详情弹窗适配
  .task-detail {
    .detail-header {
      flex-direction: column;
      gap: 12px;

      .detail-title {
        font-size: 16px;
      }
    }

    .detail-grid {
      grid-template-columns: 1fr;
    }

    .file-list {
      max-height: 150px;
    }
  }

  // 移动端对话框适配
  :deep(.el-dialog) {
    width: 95% !important;
    margin: 3vh auto !important;
    border-radius: 16px;

    .el-dialog__header {
      padding: 16px 18px;
      background: linear-gradient(135deg, #f8f9fa 0%, #ffffff 100%);

      .el-dialog__title {
        font-size: 16px;
      }

      .el-dialog__headerbtn {
        top: 16px;
        right: 16px;
      }
    }

    .el-dialog__body {
      padding: 18px;
      max-height: 60vh;
      overflow-y: auto;
    }

    .el-dialog__footer {
      padding: 14px 18px;
    }
  }

  // 全屏对话框适配
  :deep(.el-dialog.is-fullscreen) {
    border-radius: 0;

    .el-dialog__body {
      max-height: calc(100vh - 120px);
    }
  }

  // 移动端表单标签垂直布局
  :deep(.el-form-item) {
    margin-bottom: 18px;

    .el-form-item__label {
      font-size: 14px;
      padding-bottom: 8px;
    }
  }

  // 移动端输入框优化
  :deep(.el-input) {
    .el-input__wrapper {
      padding: 8px 12px;
    }

    .el-input__inner {
      font-size: 16px; // 防止 iOS 自动缩放
    }
  }

  :deep(.el-textarea) {
    .el-textarea__inner {
      font-size: 16px; // 防止 iOS 自动缩放
      min-height: 100px;
    }
  }

  // 移动端路径选择器
  .path-selector {
    flex-direction: column;
    gap: 10px;

    .el-button {
      width: 100%;
      height: 40px;
    }
  }

  // 移动端自动下载提示
  .auto-download-hint {
    display: block;
    margin-left: 0;
    margin-top: 10px;
    font-size: 13px;
    color: #909399;
  }

  // 移动端复选框
  .ask-path-checkbox {
    margin-top: 12px;

    :deep(.el-checkbox__label) {
      font-size: 14px;
    }
  }

  // 移动端对话框底部按钮
  .dialog-footer {
    flex-direction: column;
    gap: 10px;

    .el-button {
      width: 100%;
      height: 44px;
      margin: 0;
      font-size: 15px;

      &:first-child {
        order: 2;
      }

      &:last-child {
        order: 1;
      }
    }
  }
}

// =====================
// 平板设备适配 (768px - 1024px)
// =====================
@media (min-width: 768px) and (max-width: 1024px) {
  .cloud-dl-view {
    padding: 16px;
  }

  .toolbar {
    .header-left {
      h2 {
        font-size: 18px;
      }
    }
  }

  .task-container {
    padding: 14px;

    :deep(.el-table) {
      font-size: 13px;

      .el-table__header th {
        padding: 10px 0;
      }

      .el-table__body td {
        padding: 10px 0;
      }
    }
  }

  :deep(.el-dialog) {
    width: 80% !important;
    max-width: 600px;
  }
}

// =====================
// 小屏手机适配 (< 375px)
// =====================
@media (max-width: 374px) {
  .cloud-dl-view {
    padding: 10px;
  }

  .toolbar {
    gap: 8px;

    .header-left {
      .el-tag {
        font-size: 11px;
        padding: 0 6px;
      }
    }

    .header-right {
      gap: 6px;

      .el-button.is-circle {
        width: 36px;
        height: 36px;
      }
    }
  }

  .task-cards {
    gap: 10px;
  }

  .task-card {
    padding: 14px;
    border-radius: 10px;

    .task-header {
      margin-bottom: 12px;

      .task-name {
        font-size: 14px;
      }

      .el-tag {
        font-size: 11px;
        padding: 0 6px;
      }
    }

    .task-info {
      padding: 10px;

      .info-row {
        font-size: 12px;

        .label {
          width: 40px;
        }
      }
    }

    .task-actions {
      padding-top: 10px;
      gap: 8px;

      .el-button--small {
        min-width: 60px;
        height: 30px;
        font-size: 12px;
      }
    }
  }

  :deep(.el-dialog) {
    width: 98% !important;
    margin: 2vh auto !important;

    .el-dialog__header {
      padding: 14px 16px;

      .el-dialog__title {
        font-size: 15px;
      }
    }

    .el-dialog__body {
      padding: 14px;
    }

    .el-dialog__footer {
      padding: 12px 14px;
    }
  }

  :deep(.el-form-item) {
    margin-bottom: 14px;

    .el-form-item__label {
      font-size: 13px;
    }
  }

  .dialog-footer {
    gap: 8px;

    .el-button {
      height: 40px;
      font-size: 14px;
    }
  }
}

// =====================
// 横屏模式适配
// =====================
@media (max-height: 500px) and (orientation: landscape) {
  .cloud-dl-view {
    padding: 8px 16px;
  }

  .toolbar {
    margin-bottom: 8px;
  }

  .task-cards {
    gap: 8px;
  }

  .task-card {
    padding: 12px;

    .task-header {
      margin-bottom: 8px;
    }

    .task-info {
      padding: 8px;
      margin-bottom: 8px;

      .info-row {
        margin-bottom: 4px;
      }
    }

    .task-progress {
      margin-bottom: 8px;
    }

    .task-actions {
      padding-top: 8px;
    }
  }

  :deep(.el-dialog) {
    margin: 2vh auto !important;

    .el-dialog__body {
      max-height: 50vh;
      padding: 12px;
    }
  }
}

// =====================
// 安全区域适配（刘海屏）
// =====================
@supports (padding-bottom: env(safe-area-inset-bottom)) {
  .cloud-dl-view.is-mobile {
    padding-bottom: calc(12px + env(safe-area-inset-bottom));
  }

  .task-cards {
    padding-bottom: env(safe-area-inset-bottom);
  }
}

// =====================
// 深色模式适配（预留）
// =====================
@media (prefers-color-scheme: dark) {
  // 深色模式样式可在此处添加
  // 目前保持与 Element Plus 默认主题一致
}

// =====================
// 高对比度模式适配
// =====================
@media (prefers-contrast: high) {
  .task-card {
    border: 2px solid #303133;

    &.status-running {
      border-color: #e6a23c;
    }

    &.status-success {
      border-color: #67c23a;
    }

    &.status-error {
      border-color: #f56c6c;
    }
  }
}

// =====================
// 减少动画模式适配
// =====================
@media (prefers-reduced-motion: reduce) {
  .task-card {
    transition: none;

    &:active {
      transform: none;
    }
  }

  :deep(.el-dialog__close) {
    transition: none;

    &:hover {
      transform: none;
    }
  }

  .dialog-footer .el-button {
    transition: none;

    &:hover {
      transform: none;
    }
  }
}
</style>
