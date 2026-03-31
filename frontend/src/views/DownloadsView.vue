<template>
  <div class="downloads-container" :class="{ 'is-mobile': isMobile }">
    <!-- 顶部工具栏 -->
    <div class="toolbar">
      <div class="header-left">
        <h2 v-if="!isMobile">下载管理</h2>
        <el-tag :type="activeCountType" size="large">
          {{ activeCount }} 个任务进行中
        </el-tag>
      </div>
      <div class="header-right">
        <el-button @click="refreshTasks" :circle="isMobile">
          <el-icon><Refresh/></el-icon>
          <span v-if="!isMobile">刷新</span>
        </el-button>
        <el-dropdown @command="handleBatchCommand" trigger="click">
          <el-button>
            批量操作
            <el-icon class="el-icon--right"><ArrowDown/></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="pause" :disabled="activeCount === 0">
                <el-icon><VideoPause/></el-icon>
                全部暂停 ({{ activeCount }})
              </el-dropdown-item>
              <el-dropdown-item command="resume" :disabled="pausedCount === 0">
                <el-icon><VideoPlay/></el-icon>
                全部继续 ({{ pausedCount }})
              </el-dropdown-item>
              <el-dropdown-item command="clearCompleted" :disabled="completedCount === 0" divided>
                <el-icon><Delete/></el-icon>
                清除已完成 ({{ completedCount }})
              </el-dropdown-item>
              <el-dropdown-item command="clearFailed" :disabled="failedCount === 0">
                <el-icon><Delete/></el-icon>
                清除失败 ({{ failedCount }})
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>
    </div>

    <!-- 下载任务列表 -->
    <div class="task-container">
      <el-empty v-if="!loading && downloadItems.length === 0" description="暂无下载任务"/>

      <div v-else class="task-list">
        <el-card
            v-for="item in downloadItems"
            :key="item.id"
            :data-task-id="item.id"
            class="task-card"
            :class="{
              'task-active': item.status === 'downloading' || item.status === 'scanning' || item.status === 'decrypting',
              'is-folder': item.type === 'folder',
              'task-highlighted': highlightIds.has(item.id ?? '')
            }"
            shadow="hover"
        >
          <!-- 任务信息 -->
          <div class="task-header">
            <div class="task-info">
              <div class="task-title">
                <el-icon :size="20" class="file-icon">
                  <Folder v-if="item.type === 'folder'"/>
                  <Document v-else/>
                </el-icon>
                <span class="filename">
                    {{ item.type === 'folder' ? item.name : getDisplayFilename(item) }}
                  </span>
                <el-tag
                    :type="item.type === 'folder' ? getFolderStatusType(item.status as FolderStatus) : getStatusType(item.status as TaskStatus)"
                    size="small"
                >
                  {{
                    item.type === 'folder' ? getFolderStatusText(item.status as FolderStatus) : getStatusText(item.status as TaskStatus)
                  }}
                </el-tag>
                <span v-if="item.type === 'folder' && item.status === 'scanning'" class="scanning-hint">
                    (已发现 {{ item.total_files }} 个文件)
                  </span>
                <!-- 加密文件标识 -->
                <el-tag v-if="item.is_encrypted" type="info" size="small">
                  <el-icon><Lock /></el-icon>
                  加密文件
                </el-tag>
              </div>
              <div class="task-path">
                {{ item.type === 'folder' ? item.remote_root : item.remote_path }}
              </div>
            </div>

            <!-- 操作按钮 -->
            <div class="task-actions">
              <!-- 🔥 新增：跳转到关联的转存任务 -->
              <el-button
                  v-if="item.transfer_task_id"
                  size="small"
                  type="info"
                  plain
                  @click="goToTransferTask(item.transfer_task_id)"
              >
                <el-icon>
                  <Share/>
                </el-icon>
                查看转存
              </el-button>
              <el-button
                  v-if="item.type === 'folder'"
                  size="small"
                  @click="showFolderDetail(item)"
              >
                <el-icon>
                  <List/>
                </el-icon>
                详情
              </el-button>
              <el-button
                  v-if="item.status === 'downloading' || item.status === 'scanning'"
                  size="small"
                  @click="handlePause(item)"
              >
                <el-icon>
                  <VideoPause/>
                </el-icon>
                暂停
              </el-button>
              <el-button
                  v-if="item.status === 'paused' || item.status === 'failed'"
                  size="small"
                  type="primary"
                  @click="handleResume(item)"
              >
                <el-icon>
                  <VideoPlay/>
                </el-icon>
                {{ item.status === 'failed' ? '重试' : '继续' }}
              </el-button>
              <el-button
                  v-if="item.status === 'completed'"
                  size="small"
                  type="success"
                  @click="openLocalFile(item.type === 'folder' ? (item.local_root || '') : (item.local_path || ''))"
              >
                <el-icon>
                  <FolderOpened/>
                </el-icon>
                打开文件夹
              </el-button>
              <el-button
                  size="small"
                  type="danger"
                  :disabled="deletingIds.has(item.id!)"
                  :loading="deletingIds.has(item.id!)"
                  @click="handleDelete(item)"
              >
                <el-icon>
                  <Delete/>
                </el-icon>
                {{ deletingIds.has(item.id!) ? '删除中...' : '删除' }}
              </el-button>
            </div>
          </div>

          <!-- 解密进度显示 -->
          <div v-if="item.status === 'decrypting'" class="decrypt-progress">
            <div class="decrypt-header">
              <el-icon class="decrypt-icon"><Unlock /></el-icon>
              <span>正在解密文件...</span>
            </div>
            <el-progress
                :percentage="item.decrypt_progress || 0"
                :stroke-width="6"
                status="warning"
            >
              <template #default="{ percentage }">
                <span class="progress-text">{{ percentage.toFixed(1) }}%</span>
              </template>
            </el-progress>
          </div>

          <!-- 进度条 -->
          <div class="task-progress" v-if="item.status !== 'decrypting'">
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
              <span class="stat-value">{{
                  formatETA(calculateETA({
                    total_size: item.total_size || 0,
                    downloaded_size: item.downloaded_size || 0,
                    speed: item.speed || 0
                  } as any))
                }}</span>
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
        @close="onFolderDetailClose"
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
          <div class="stat-card" v-if="folderDetailDialog.decryptingFiles > 0">
            <div class="stat-label">解密中</div>
            <div class="stat-value primary">{{ folderDetailDialog.decryptingFiles }}</div>
          </div>
          <div class="stat-card" v-if="folderDetailDialog.pausedFiles > 0">
            <div class="stat-label">已暂停</div>
            <div class="stat-value warning">{{ folderDetailDialog.pausedFiles }}</div>
          </div>
          <div class="stat-card" v-if="folderDetailDialog.failedFiles > 0">
            <div class="stat-label">失败</div>
            <div class="stat-value danger">{{ folderDetailDialog.failedFiles }}</div>
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
                <el-icon>
                  <Search/>
                </el-icon>
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
                  <el-icon :size="16">
                    <Document/>
                  </el-icon>
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
        <el-button @click="closeFolderDetail">关闭</el-button>
        <el-button type="primary" @click="refreshFolderDetail">
          <el-icon>
            <Refresh/>
          </el-icon>
          刷新
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import {ref, computed, onMounted, onUnmounted, nextTick} from 'vue'
import {ElMessage, ElMessageBox} from 'element-plus'
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
  batchPauseDownloads,
  batchResumeDownloads,
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
  Share,
  Lock,
  Unlock,
  ArrowDown,
} from '@element-plus/icons-vue'
import {useRouter, useRoute} from 'vue-router'
import {useIsMobile} from '@/utils/responsive'
// 🔥 WebSocket 相关导入
import {getWebSocketClient, connectWebSocket, type ConnectionState} from '@/utils/websocket'

// 响应式检测
const isMobile = useIsMobile()
import type {DownloadEvent, FolderEvent} from '@/types/events'

// 路由
const router = useRouter()
const route = useRoute()

// 高亮的下载任务 ID 集合（从转存页跳转过来时使用）
const highlightIds = ref<Set<string>>(new Set())

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
  failedFiles: 0,
  pausedFiles: 0,
  decryptingFiles: 0,
  tasks: [] as DownloadTask[],
  searchText: '',
})

// 自动刷新定时器
let refreshTimer: number | null = null
// 文件夹详情弹窗刷新定时器
let folderDetailTimer: number | null = null
// 🔥 refreshFolderDetail 请求序号 + 最后应用序号，用于丢弃乱序旧响应
let folderDetailRequestSeq = 0      // 每次调用递增
let folderDetailAppliedSeq = 0      // 最后成功应用的序号
// 🔥 记录每个子任务最后被 WS 事件更新的时间戳，用于轮询合并时保护新鲜数据
const folderDetailTaskWsTime = new Map<string, number>()
// 🔥 记录 WS 删除事件的时间戳，防止旧轮询响应把已删除任务带回来
const folderDetailTaskDeletedAt = new Map<string, number>()
// 🔥 主列表 WS 事件时间戳，用于 refreshTasks 合并时保护新鲜数据
const mainListWsTime = new Map<string, number>()
const mainListDeletedAt = new Map<string, number>()
// 🔥 WebSocket 事件订阅清理函数
let unsubscribeDownload: (() => void) | null = null
let unsubscribeFolder: (() => void) | null = null
let unsubscribeConnectionState: (() => void) | null = null
// 🔥 WebSocket 连接状态
const wsConnected = ref(false)
// 🔥 是否已成功加载过一次任务列表，用于初始加载失败时保持重试
let initialLoadDone = false

// 是否有活跃任务（需要实时刷新）
const hasActiveTasks = computed(() => {
  return downloadItems.value.some(item => {
    const status = item.status
    return status === 'downloading' || status === 'scanning' || status === 'paused' || status === 'pending' || status === 'decrypting'
  })
})

// 计算属性
const activeCount = computed(() => {
  return downloadItems.value.filter(item =>
      item.status === 'downloading' || item.status === 'scanning' || item.status === 'decrypting'
  ).length
})

const completedCount = computed(() => {
  return downloadItems.value.filter(item => item.status === 'completed').length
})

const failedCount = computed(() => {
  return downloadItems.value.filter(item => item.status === 'failed').length
})

const pausedCount = computed(() => {
  return downloadItems.value.filter(item => item.status === 'paused').length
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

// 🔥 获取显示用的文件名（优先使用原始文件名）
function getDisplayFilename(item: DownloadItemFromBackend): string {
  // 优先使用原始文件名（加密文件解密后的名称）
  if (item.original_filename) {
    return item.original_filename
  }
  return getFilename(item.local_path || '')
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
  if (status === 'decrypting') return 'warning'
  return undefined
}

// 🔥 跳转到关联的转存任务
function goToTransferTask(transferTaskId: string) {
  router.push({
    name: 'Transfers',
    query: {highlight: transferTaskId}
  })
}

// 刷新任务列表
async function refreshTasks() {
  // 如果正在加载中，跳过本次请求，避免并发请求
  if (loading.value) {
    return
  }

  loading.value = true
  // 🔥 记录请求发出时间，用于合并时判断 WS 事件是否比本次请求更新
  const requestTime = Date.now()
  try {
    const apiItems = await getAllDownloadsMixed()

    // 🔥 合并而非替换：保护在 await 期间被 WS 事件更新/新增/删除过的条目
    const existingMap = new Map<string, DownloadItemFromBackend>(
        downloadItems.value.filter(item => item.id).map(item => [item.id!, item])
    )
    const apiIds = new Set(apiItems.filter(item => item.id).map(item => item.id!))
    const merged: DownloadItemFromBackend[] = []

    for (const apiItem of apiItems) {
      const itemId = apiItem.id
      if (!itemId) { merged.push(apiItem); continue }
      const deletedAt = mainListDeletedAt.get(itemId)
      if (deletedAt && deletedAt > requestTime) {
        continue // WS 在请求发出后删除了这个条目
      }
      const wsTime = mainListWsTime.get(itemId)
      if (wsTime && wsTime > requestTime && existingMap.has(itemId)) {
        merged.push(existingMap.get(itemId)!)
      } else {
        merged.push(apiItem)
      }
    }
    // 补充 WS 在 await 期间新增的条目
    for (const [id, existing] of existingMap) {
      if (!apiIds.has(id)) {
        const wsTime = mainListWsTime.get(id)
        if (wsTime && wsTime > requestTime) {
          merged.unshift(existing)
        }
      }
    }
    downloadItems.value = merged
    initialLoadDone = true
  } catch (error: any) {
    console.error('刷新任务列表失败:', error)
    // 🔥 不清空列表：保留现有数据，避免临时失败导致页面变空 + 轮询停止的死锁
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
      console.log('[DownloadsView] WebSocket 已连接，停止轮询')
      clearInterval(refreshTimer)
      refreshTimer = null
    }
    return
  }

  // 🔥 WebSocket 未连接时，回退到轮询模式
  // 有活跃任务 或 初始加载尚未成功时，启动或保持定时刷新
  if (hasActiveTasks.value || !initialLoadDone) {
    if (!refreshTimer) {
      console.log('[DownloadsView] WebSocket 未连接，启动轮询模式，活跃任务数:', activeCount.value)
      refreshTimer = window.setInterval(() => {
        refreshTasks()
      }, 1000) // 🔥 改为 1 秒间隔，减少服务器压力
    }
  } else {
    // 没有活跃任务时，停止定时刷新
    if (refreshTimer) {
      console.log('[DownloadsView] 停止轮询，当前任务数:', downloadItems.value.length)
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
    const res = await batchPauseDownloads({ all: true })
    ElMessage.success(`已暂停 ${res.success_count} 个任务`)
    refreshTasks()
  } catch (error: any) {
    console.error('批量暂停失败:', error)
  }
}

// 全部继续
async function handleBatchResume() {
  try {
    const res = await batchResumeDownloads({ all: true })
    ElMessage.success(`已恢复 ${res.success_count} 个任务`)
    refreshTasks()
  } catch (error: any) {
    console.error('批量恢复失败:', error)
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

  // 🔥 先停止旧的定时器和取消旧订阅（此时 folderId 还是旧值）
  stopFolderDetailTimer()

  // 🔥 重置应用序号和 WS 时间戳，让上一个目录的在途异步请求失效
  folderDetailAppliedSeq = ++folderDetailRequestSeq
  folderDetailTaskWsTime.clear()
  folderDetailTaskDeletedAt.clear()

  // 设置新的文件夹信息
  folderDetailDialog.value.visible = true
  folderDetailDialog.value.folderId = item.id
  folderDetailDialog.value.folderName = item.name || '未知文件夹'
  folderDetailDialog.value.searchText = ''

  const wsClient = getWebSocketClient()

  // 🔥 订阅新文件夹子任务事件（保持主列表订阅，因为弹窗时主列表仍然可见）
  wsClient.subscribe([`download:${item.id}`])
  console.log('[DownloadsView] 订阅文件夹子任务:', item.id)

  await refreshFolderDetail()

  // 启动文件夹详情自动刷新定时器
  startFolderDetailTimer()
}

// 启动文件夹详情定时器
// 🔥 修复：即使 WebSocket 已连接，也要启用轮询（2秒一次）
// 用于修正子任务状态，因为借用位暂停时可能没有收到 WebSocket 消息
function startFolderDetailTimer() {
  // 🔥 只清理定时器，不取消订阅（订阅由 showFolderDetail 和 stopFolderDetailTimer 管理）
  if (folderDetailTimer) {
    clearInterval(folderDetailTimer)
    folderDetailTimer = null
  }

  // 🔥 启用轮询，3秒间隔，用于修正状态
  const interval = 2000
  console.log('[DownloadsView] 启动文件夹详情轮询，间隔:', interval, 'ms, wsConnected:', wsConnected.value)
  folderDetailTimer = window.setInterval(() => {
    if (folderDetailDialog.value.visible) {
      refreshFolderDetail()
    } else {
      stopFolderDetailTimer()
    }
  }, interval)
}

// 停止文件夹详情定时器
function stopFolderDetailTimer(alsoUnsubscribe = true) {
  if (folderDetailTimer) {
    clearInterval(folderDetailTimer)
    folderDetailTimer = null
  }

  // 🔥 取消文件夹子任务订阅
  const folderId = folderDetailDialog.value.folderId
  if (alsoUnsubscribe && folderId) {
    const wsClient = getWebSocketClient()
    wsClient.unsubscribe([`download:${folderId}`])
    console.log('[DownloadsView] 取消文件夹子任务订阅:', folderId)
  }
}

// 🔥 关闭文件夹详情弹窗（用户点击关闭按钮）
function closeFolderDetail() {
  folderDetailDialog.value.visible = false
}

// 🔥 文件夹详情弹窗关闭回调（清理资源）
function onFolderDetailClose() {
  // 停止定时器和取消子任务订阅
  stopFolderDetailTimer()

  // 🔥 清理弹窗数据（包括统计，防止下次打开其他目录时闪现旧数据）
  folderDetailDialog.value.folderId = ''
  folderDetailDialog.value.folderName = ''
  folderDetailDialog.value.tasks = []
  folderDetailDialog.value.totalFiles = 0
  folderDetailDialog.value.completedFiles = 0
  folderDetailDialog.value.downloadingFiles = 0
  folderDetailDialog.value.pendingFiles = 0
  folderDetailDialog.value.failedFiles = 0
  folderDetailDialog.value.pausedFiles = 0
  folderDetailDialog.value.decryptingFiles = 0
  folderDetailDialog.value.searchText = ''
  // 🔥 重置应用序号和 WS 时间戳，让任何在途的异步请求失效
  folderDetailAppliedSeq = ++folderDetailRequestSeq
  folderDetailTaskWsTime.clear()
  folderDetailTaskDeletedAt.clear()

  // 🔥 主列表订阅保持不变（主列表一直需要订阅）
}

// 刷新文件夹详情
async function refreshFolderDetail() {
  const folderId = folderDetailDialog.value.folderId
  if (!folderId) return

  // 🔥 每次调用分配唯一序号，await 后比较 appliedSeq 决定是否应用
  // 不会饥死：只有当更新的响应已经落地时才跳过，不是因为有更新的请求发出
  const mySeq = ++folderDetailRequestSeq
  // 🔥 记录请求发出时间，用于合并时判断 WS 事件是否比本次请求更新
  const requestTime = Date.now()

  try {
    // 获取所有任务，然后过滤出属于该文件夹的任务
    const allTasks = await getAllDownloads()

    // 🔥 防止异步响应覆盖：
    // 1. 弹窗已关/已换目录 → folderId 或 visible 不匹配
    // 2. 同目录乱序 → 更新的响应已经应用（mySeq <= appliedSeq）
    if (!folderDetailDialog.value.visible || folderDetailDialog.value.folderId !== folderId) return
    if (mySeq <= folderDetailAppliedSeq) return
    folderDetailAppliedSeq = mySeq

    const apiFolderTasks = allTasks.filter((task) => task.group_id === folderId)

    // 🔥 合并而非替换：保护在 await 期间被 WS 事件更新/新增/删除过的子任务
    const existingMap = new Map(folderDetailDialog.value.tasks.map(t => [t.id, t]))
    const apiTaskIds = new Set(apiFolderTasks.map(t => t.id))
    const mergedTasks: DownloadTask[] = []

    // 遍历 API 任务：跳过 WS 已删除的，保护 WS 已更新的
    for (const apiTask of apiFolderTasks) {
      const deletedAt = folderDetailTaskDeletedAt.get(apiTask.id)
      if (deletedAt && deletedAt > requestTime) {
        // WS 在请求发出后删除了这个任务，不要带回来
        continue
      }
      const wsTime = folderDetailTaskWsTime.get(apiTask.id)
      if (wsTime && wsTime > requestTime && existingMap.has(apiTask.id)) {
        // WS 事件在请求发出后更新过这个任务，保留现有的新鲜数据
        mergedTasks.push(existingMap.get(apiTask.id)!)
      } else {
        mergedTasks.push(apiTask)
      }
    }
    // 补充 WS 在 await 期间新增的任务（API 响应中不包含）
    for (const [id, existing] of existingMap) {
      if (!apiTaskIds.has(id)) {
        const wsTime = folderDetailTaskWsTime.get(id)
        if (wsTime && wsTime > requestTime) {
          mergedTasks.push(existing)
        }
      }
    }

    // 🔥 使用文件夹级别的 completed_files（后端维护的累计值）
    // 不从内存子任务重新计数，因为已完成的子任务会被移除
    const folderItem = downloadItems.value.find((i) => i.id === folderId && i.type === 'folder')
    const folderCompletedFiles = folderItem?.completed_files ?? 0
    const folderTotalFiles = folderItem?.total_files ?? mergedTasks.length

    const downloadingFiles = mergedTasks.filter((t) => t.status === 'downloading').length
    const pendingFiles = mergedTasks.filter((t) => t.status === 'pending').length
    const failedFiles = mergedTasks.filter((t) => t.status === 'failed').length
    const pausedFiles = mergedTasks.filter((t) => t.status === 'paused').length
    const decryptingFiles = mergedTasks.filter((t) => t.status === 'decrypting').length
    // 未创建任务的文件数 = 总文件数 - 已完成(文件夹级累计值) - 非完成的内存子任务数
    // 注意：已完成子任务可能尚未从列表移除，要排除否则会被 completedFiles 和 tasks 同时计数
    const activeTaskCount = mergedTasks.filter((t) => t.status !== 'completed').length
    const notCreatedYet = Math.max(0, folderTotalFiles - folderCompletedFiles - activeTaskCount)

    folderDetailDialog.value.tasks = mergedTasks
    folderDetailDialog.value.totalFiles = folderTotalFiles
    folderDetailDialog.value.completedFiles = folderCompletedFiles
    folderDetailDialog.value.downloadingFiles = downloadingFiles
    folderDetailDialog.value.pendingFiles = pendingFiles + notCreatedYet
    folderDetailDialog.value.failedFiles = failedFiles
    folderDetailDialog.value.pausedFiles = pausedFiles
    folderDetailDialog.value.decryptingFiles = decryptingFiles
  } catch (error: any) {
    console.error('获取文件夹子任务失败:', error)
    ElMessage.error('获取文件夹子任务失败')
  }
}

// 🔥 处理下载事件
function handleDownloadEvent(event: DownloadEvent) {
  const taskId = event.task_id
  // 🔥 修复：放宽查找条件，只要 id 匹配且不是文件夹类型即可
  const index = downloadItems.value.findIndex(item => item.id === taskId && item.type !== 'folder')

  switch (event.event_type) {
    case 'created':
      // 新任务创建，添加到列表
      // 🔥 有 group_id 的是文件夹子任务，不应出现在主列表（主列表只显示文件夹行）
      if (index === -1 && !event.group_id) {
        downloadItems.value.unshift({
          id: taskId,
          type: 'file',
          status: 'pending',
          remote_path: event.remote_path,
          local_path: event.local_path,
          total_size: event.total_size,
          downloaded_size: 0,
          speed: 0,
          group_id: event.group_id,
          original_filename: event.original_filename, // 🔥 保存原始文件名
          is_encrypted: !!event.original_filename, // 🔥 有原始文件名说明是加密文件
        } as DownloadItemFromBackend)
        mainListWsTime.set(taskId, Date.now())
      }
      // 🔥 如果是文件夹详情弹窗中的子任务，也添加到弹窗
      if (event.group_id && folderDetailDialog.value.visible && event.group_id === folderDetailDialog.value.folderId) {
        const detailIndex = folderDetailDialog.value.tasks.findIndex(t => t.id === taskId)
        if (detailIndex === -1) {
          folderDetailDialog.value.tasks.push({
            id: taskId,
            status: 'pending',
            remote_path: event.remote_path,
            local_path: event.local_path,
            total_size: event.total_size,
            downloaded_size: 0,
            speed: 0,
            group_id: event.group_id,
          } as DownloadTask)
          // 🔥 记录 WS 新增时间戳，合并时保护不被旧轮询丢弃
          folderDetailTaskWsTime.set(taskId, Date.now())
          updateFolderDetailStats()
        }
      }
      break

    case 'progress':
      // 更新进度
      if (index !== -1) {
        downloadItems.value[index].downloaded_size = event.downloaded_size
        downloadItems.value[index].total_size = event.total_size
        downloadItems.value[index].speed = event.speed
        // 🔥 不更新状态，避免暂停后收到延迟进度事件导致状态回刷
        mainListWsTime.set(taskId, Date.now())
      }
      // 🔥 实时更新文件夹详情弹窗中的子任务进度
      if (folderDetailDialog.value.visible) {
        // 获取文件夹状态，如果文件夹是暂停状态，子任务也应该是暂停状态
        const folderItem = downloadItems.value.find(
            (i) => i.id === folderDetailDialog.value.folderId && i.type === 'folder'
        )
        const isFolderPaused = folderItem?.status === 'paused'

        updateFolderDetailTask(taskId, {
          downloaded_size: event.downloaded_size,
          total_size: event.total_size,
          speed: event.speed,
          // 🔥 如果文件夹是暂停状态，子任务也设为暂停；否则设为 downloading
          status: isFolderPaused ? 'paused' as TaskStatus : 'downloading' as TaskStatus,
        }, true)
      }
      break

    case 'decrypt_progress':
      // 🔥 解密进度更新
      if (index !== -1) {
        // 🔥 修复：如果任务已完成，忽略延迟到达的解密进度事件
        if (downloadItems.value[index].status === 'completed') {
          break
        }
        downloadItems.value[index].decrypt_progress = event.decrypt_progress
        downloadItems.value[index].status = 'decrypting'
        downloadItems.value[index].is_encrypted = true
        mainListWsTime.set(taskId, Date.now())
      }
      // 🔥 更新文件夹详情弹窗中的子任务解密进度
      updateFolderDetailTask(taskId, {
        decrypt_progress: event.decrypt_progress,
        status: 'decrypting' as TaskStatus,
        is_encrypted: true,
      }, true)
      break

    case 'decrypt_completed':
      // 🔥 解密完成
      if (index !== -1) {
        downloadItems.value[index].decrypt_progress = 100
        downloadItems.value[index].local_path = event.decrypted_path
        // 状态变更会由 status_changed 或 completed 事件处理
        mainListWsTime.set(taskId, Date.now())
      }
      // 🔥 更新文件夹详情弹窗中的子任务解密完成
      updateFolderDetailTask(taskId, {
        decrypt_progress: 100,
        local_path: event.decrypted_path,
      })
      break

    case 'status_changed':
      // 状态变更
      if (index !== -1) {
        downloadItems.value[index].status = event.new_status as TaskStatus
        mainListWsTime.set(taskId, Date.now())
      }
      // 🔥 更新文件夹详情弹窗中的子任务状态
      updateFolderDetailTask(taskId, {status: event.new_status as TaskStatus}, true)
      break

    case 'completed': {
      // 任务完成
      // 🔥 乐观递增文件夹的 completed_files，避免等待 folder progress 事件
      // 使用 event.group_id 而非主列表子任务行，因为子任务不再出现在主列表
      const groupId = event.group_id ?? (index !== -1 ? downloadItems.value[index].group_id : undefined)
      if (groupId) {
        const folderIdx = downloadItems.value.findIndex(i => i.id === groupId && i.type === 'folder')
        if (folderIdx !== -1) {
          downloadItems.value[folderIdx].completed_files = (downloadItems.value[folderIdx].completed_files ?? 0) + 1
          mainListWsTime.set(groupId, Date.now())
        }
      }
      if (index !== -1) {
        downloadItems.value[index].status = 'completed'
        downloadItems.value[index].downloaded_size = downloadItems.value[index].total_size
        downloadItems.value[index].speed = 0
        // 🔥 如果是加密文件，完成时解密进度也应该是 100%
        if (downloadItems.value[index].is_encrypted) {
          downloadItems.value[index].decrypt_progress = 100
        }
        mainListWsTime.set(taskId, Date.now())
      }
    }
      // 🔥 更新文件夹详情弹窗中的子任务完成状态（不设置 decrypt_progress，避免影响普通文件）
      updateFolderDetailTask(taskId, {status: 'completed' as TaskStatus, speed: 0}, true)
      break

    case 'failed':
      // 任务失败
      if (index !== -1) {
        downloadItems.value[index].status = 'failed'
        downloadItems.value[index].error = event.error
        downloadItems.value[index].speed = 0
        mainListWsTime.set(taskId, Date.now())
      }
      // 🔥 更新文件夹详情弹窗中的子任务失败状态
      updateFolderDetailTask(taskId, {status: 'failed' as TaskStatus, error: event.error, speed: 0}, true)
      break

    case 'paused':
      // 任务暂停
      if (index !== -1) {
        downloadItems.value[index].status = 'paused'
        downloadItems.value[index].speed = 0
        mainListWsTime.set(taskId, Date.now())
      }
      // 🔥 更新文件夹详情弹窗中的子任务暂停状态
      updateFolderDetailTask(taskId, {status: 'paused' as TaskStatus, speed: 0}, true)
      break

    case 'resumed':
      // 任务恢复
      if (index !== -1) {
        // 🔥 设为 downloading 而不是 pending，这样 UI 会显示速度和剩余时间
        // 后续的 progress 事件会更新实际的速度值
        downloadItems.value[index].status = 'downloading'
        mainListWsTime.set(taskId, Date.now())
      }
      // 🔥 更新文件夹详情弹窗中的子任务恢复状态
      updateFolderDetailTask(taskId, {status: 'downloading' as TaskStatus}, true)
      break

    case 'deleted':
      // 任务删除
      // 🔥 无论本地是否已有该行，都记录删除墓碑，防止在途的旧轮询把它带回来
      mainListDeletedAt.set(taskId, Date.now())
      if (index !== -1) {
        downloadItems.value.splice(index, 1)
      }
      // 🔥 从文件夹详情弹窗中删除子任务
      // 使用 event.group_id 判断归属，即使任务尚未出现在弹窗列表中也能记录墓碑
      if (folderDetailDialog.value.visible &&
          event.group_id && event.group_id === folderDetailDialog.value.folderId) {
        folderDetailTaskDeletedAt.set(taskId, Date.now())
        const detailIndex = folderDetailDialog.value.tasks.findIndex(t => t.id === taskId)
        if (detailIndex !== -1) {
          folderDetailDialog.value.tasks.splice(detailIndex, 1)
          updateFolderDetailStats()
        }
      }
      break
  }
}

// 🔥 更新文件夹详情弹窗中的子任务
function updateFolderDetailTask(taskId: string, updates: Partial<DownloadTask>, updateStats = false) {
  if (!folderDetailDialog.value.visible) return

  const detailIndex = folderDetailDialog.value.tasks.findIndex(t => t.id === taskId)
  if (detailIndex !== -1) {
    Object.assign(folderDetailDialog.value.tasks[detailIndex], updates)
    // 🔥 记录 WS 更新时间戳，轮询合并时保护比请求时刻更新的数据
    folderDetailTaskWsTime.set(taskId, Date.now())
    if (updateStats) {
      updateFolderDetailStats()
    }
  }
}

// 🔥 更新文件夹详情弹窗的统计数据
function updateFolderDetailStats() {
  if (!folderDetailDialog.value.visible) return

  const tasks = folderDetailDialog.value.tasks

  // 🔥 使用文件夹级别的 completed_files（后端维护的累计值）
  const folderItem = downloadItems.value.find(
      (i) => i.id === folderDetailDialog.value.folderId && i.type === 'folder'
  )
  const folderCompletedFiles = folderItem?.completed_files ?? 0
  const folderTotalFiles = folderItem?.total_files ?? tasks.length

  const downloadingFiles = tasks.filter((t) => t.status === 'downloading').length
  const pendingFiles = tasks.filter((t) => t.status === 'pending').length
  const failedFiles = tasks.filter((t) => t.status === 'failed').length
  const pausedFiles = tasks.filter((t) => t.status === 'paused').length
  const decryptingFiles = tasks.filter((t) => t.status === 'decrypting').length
  // 注意：已完成子任务可能尚未从列表移除，要排除否则会被 completedFiles 和 tasks 同时计数
  const activeTaskCount = tasks.filter((t) => t.status !== 'completed').length
  const notCreatedYet = Math.max(0, folderTotalFiles - folderCompletedFiles - activeTaskCount)

  folderDetailDialog.value.completedFiles = folderCompletedFiles
  folderDetailDialog.value.downloadingFiles = downloadingFiles
  folderDetailDialog.value.pendingFiles = pendingFiles + notCreatedYet
  folderDetailDialog.value.failedFiles = failedFiles
  folderDetailDialog.value.pausedFiles = pausedFiles
  folderDetailDialog.value.decryptingFiles = decryptingFiles
  folderDetailDialog.value.totalFiles = folderTotalFiles
}

// 🔥 处理文件夹事件
function handleFolderEvent(event: FolderEvent) {
  const folderId = event.folder_id
  const index = downloadItems.value.findIndex(item => item.id === folderId && item.type === 'folder')

  switch (event.event_type) {
    case 'created':
      // 新文件夹创建
      if (index === -1) {
        downloadItems.value.unshift({
          id: folderId,
          type: 'folder',
          status: 'scanning',
          name: event.name,
          remote_root: event.remote_root,
          local_root: event.local_root,
          total_files: 0,
          completed_files: 0,
          total_size: 0,
          downloaded_size: 0,
          speed: 0,
        } as DownloadItemFromBackend)
        mainListWsTime.set(folderId, Date.now())
      }
      break

    case 'progress':
      // 更新进度
      if (index !== -1) {
        downloadItems.value[index].downloaded_size = event.downloaded_size
        downloadItems.value[index].total_size = event.total_size
        // 🔥 只允许递增，防止延迟到达的旧 progress 回写子任务完成时的乐观 +1
        downloadItems.value[index].completed_files = Math.max(
            downloadItems.value[index].completed_files ?? 0, event.completed_files ?? 0
        )
        downloadItems.value[index].total_files = event.total_files
        downloadItems.value[index].speed = event.speed
        // 🔥 不更新状态，避免暂停后收到延迟进度事件导致状态回刷
        mainListWsTime.set(folderId, Date.now())
      }
      // 🔥 同步刷新详情弹窗统计（completed_files/total_files 可能已变）
      updateFolderDetailStats()
      break

    case 'status_changed':
      if (index !== -1) {
        downloadItems.value[index].status = event.new_status as FolderStatus
        mainListWsTime.set(folderId, Date.now())
      }
      break

    case 'scan_completed':
      if (index !== -1) {
        downloadItems.value[index].total_files = event.total_files
        downloadItems.value[index].total_size = event.total_size
        downloadItems.value[index].status = 'downloading'
        mainListWsTime.set(folderId, Date.now())
      }
      // 🔥 同步刷新详情弹窗统计（total_files 已变）
      updateFolderDetailStats()
      break

    case 'completed':
      if (index !== -1) {
        downloadItems.value[index].status = 'completed'
        downloadItems.value[index].speed = 0
        // 🔥 文件夹完成 = 所有子任务都成功，将 completed_files 推到终值
        // 防止乐观 +1 被跳过时头部统计少 1
        downloadItems.value[index].completed_files = downloadItems.value[index].total_files
        mainListWsTime.set(folderId, Date.now())
      }
      updateFolderDetailStats()
      break

    case 'failed':
      if (index !== -1) {
        downloadItems.value[index].status = 'failed'
        downloadItems.value[index].error = event.error
        downloadItems.value[index].speed = 0
        mainListWsTime.set(folderId, Date.now())
      }
      updateFolderDetailStats()
      break

    case 'paused':
      if (index !== -1) {
        downloadItems.value[index].status = 'paused'
        downloadItems.value[index].speed = 0
        mainListWsTime.set(folderId, Date.now())
      }
      break

    case 'resumed':
      // 🔥 不设置状态：后端先发 StatusChanged(已按 scan_completed 设为 scanning/downloading)再发 Resumed
      // StatusChanged 已经设置了正确状态，这里不再覆盖
      break

    case 'deleted':
      // 🔥 无论本地是否已有该行，都记录删除墓碑，防止在途的旧轮询把它带回来
      mainListDeletedAt.set(folderId, Date.now())
      if (index !== -1) {
        downloadItems.value.splice(index, 1)
      }
      break
  }
}

// 🔥 设置 WebSocket 订阅
function setupWebSocketSubscriptions() {
  const wsClient = getWebSocketClient()

  // 🔥 订阅服务端事件（下载管理页面只订阅普通文件和文件夹，不订阅子任务）
  wsClient.subscribe(['download:file', 'folder'])

  // 订阅下载事件（客户端回调）
  unsubscribeDownload = wsClient.onDownloadEvent(handleDownloadEvent)

  // 订阅文件夹事件（客户端回调）
  unsubscribeFolder = wsClient.onFolderEvent(handleFolderEvent)

  // 🔥 订阅连接状态变化
  unsubscribeConnectionState = wsClient.onConnectionStateChange((state: ConnectionState) => {
    const wasConnected = wsConnected.value
    wsConnected.value = state === 'connected'

    console.log('[DownloadsView] WebSocket 状态变化:', state, ', 是否连接:', wsConnected.value)

    // 🔥 任何状态变化都检查轮询策略（包括 connecting 状态）
    updateAutoRefresh()

    // 🔥 文件夹详情：连接时依赖推送，断开时启用兜底轮询
    if (wsConnected.value) {
      // 保留订阅，仅停止轮询
      stopFolderDetailTimer(false)
    } else if (folderDetailDialog.value.visible) {
      startFolderDetailTimer()
    }

    // 🔥 WebSocket 重新连接成功时，刷新一次获取最新数据
    if (!wasConnected && wsConnected.value) {
      refreshTasks()
    }
  })

  // 确保连接
  connectWebSocket()

  console.log('[DownloadsView] WebSocket 订阅已设置')
}

// 🔥 清理 WebSocket 订阅
function cleanupWebSocketSubscriptions() {
  const wsClient = getWebSocketClient()

  // 🔥 取消服务端订阅
  wsClient.unsubscribe(['download:file', 'folder'])

  if (unsubscribeDownload) {
    unsubscribeDownload()
    unsubscribeDownload = null
  }
  if (unsubscribeFolder) {
    unsubscribeFolder()
    unsubscribeFolder = null
  }
  if (unsubscribeConnectionState) {
    unsubscribeConnectionState()
    unsubscribeConnectionState = null
  }
  console.log('[DownloadsView] WebSocket 订阅已清理')
}

// 组件挂载时加载任务列表
onMounted(async () => {
  // 解析 highlight 参数（支持逗号分隔的多个 ID）
  // 文件夹下载 ID 带 "folder:" 前缀，需要去掉前缀才能匹配 item.id
  const highlightParam = route.query.highlight as string | undefined
  if (highlightParam) {
    highlightIds.value = new Set(
        highlightParam.split(',').filter(Boolean).map(id => id.replace(/^folder:/, ''))
    )
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
        // 清除 URL 中的 highlight 参数
        router.replace({ query: {} })
      }, 3000)
    })
  }

  // 🔥 设置 WebSocket 订阅
  setupWebSocketSubscriptions()
  // updateAutoRefresh 会在 refreshTasks 完成后根据任务状态自动启动定时器
})

// 组件卸载时清除定时器
onUnmounted(() => {
  if (refreshTimer) {
    clearInterval(refreshTimer)
    refreshTimer = null
  }
  stopFolderDetailTimer()
  // 🔥 清理 WebSocket 订阅
  cleanupWebSocketSubscriptions()
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

// =====================
// 解密进度样式
// =====================
.decrypt-progress {
  margin-bottom: 15px;
  padding: 10px;
  background: #fdf6ec;
  border-radius: 4px;

  .decrypt-header {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
    color: #e6a23c;
    font-size: 13px;

    .decrypt-icon {
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

        &.warning {
          color: #e6a23c;
        }

        &.danger {
          color: #f56c6c;
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

// =====================
// 移动端样式
// =====================
.is-mobile {
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
  }

  .task-stats {
    gap: 12px;

    .stat-item {
      font-size: 12px;
    }
  }
}

// 移动端对话框适配
@media (max-width: 767px) {
  :deep(.el-dialog) {
    width: 95% !important;
    margin: 3vh auto !important;
  }

  .folder-detail .folder-stats {
    grid-template-columns: repeat(2, 1fr);
  }
}
</style>
