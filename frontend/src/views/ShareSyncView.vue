<template>
  <div class="share-sync-view">
    <!-- 顶部工具栏（与其他页面一致） -->
    <div class="toolbar">
      <div class="header-left">
        <h2 v-if="!isMobile">分享同步</h2>
        <AccountFilter
            v-if="authStore.hasMultipleAccounts"
            v-model="ownerFilter"
            :counts="ownerFilterCounts"
            :total-count="subscriptions.length"
            size="large"
            class="account-filter-slot"
        />
      </div>
      <div class="header-right">
        <el-button type="primary" :icon="Plus" @click="showTransferDialog = true">新增</el-button>
      </div>
    </div>

    <div class="ss-content">
      <!-- 订阅卡片列表（自动备份风格：卡片 + 内联进行中子任务），占满全宽 -->
      <div class="ss-list-title">订阅列表（{{ displayedSubscriptions.length }}）</div>

      <el-empty
          v-if="displayedSubscriptions.length === 0"
          :description="ownerFilter === null ? '还没有订阅' : '当前账号下没有订阅'"
      />

      <div v-else class="config-list">
        <el-card
            v-for="s in displayedSubscriptions"
            :key="s.id"
            class="config-card"
            :class="{ active: selected?.id === s.id, 'is-disabled': !s.enabled }"
            shadow="hover"
            @click="openDetail(s)"
        >
          <!-- 卡片头部 -->
          <div class="config-header">
            <div class="config-info">
              <div class="config-title">
                <el-icon :size="18" class="direction-icon"><Link /></el-icon>
                <span class="config-name">{{ s.name }}</span>
                <AccountBadge :owner-uid="s.owner_uid" size="small" class="task-account-badge" />
                <el-tag :type="s.enabled ? 'success' : 'info'" size="small">{{ s.enabled ? '已启用' : '已停用' }}</el-tag>
                <el-tooltip
                    v-if="s.link_invalid"
                    :content="s.link_invalid_reason || '分享链接已失效（被取消/过期/提取码失效），已暂停轮询；更新链接后点「恢复」'"
                    placement="top"
                >
                  <el-tag type="danger" size="small" effect="dark">链接失效·已暂停</el-tag>
                </el-tooltip>
                <el-tag :type="strategyTagType(s.conflict_strategy)" size="small">{{ describeStrategy(s.conflict_strategy) }}</el-tag>
              </div>
              <div class="config-path">
                <span>{{ describeTargets(s.targets) }}</span>
                <span class="dot">·</span>
                <span>{{ describeInterval(s.poll_config) }}</span>
                <template v-if="s.include_paths.length">
                  <span class="dot">·</span><span>范围 {{ s.include_paths.length }} 条</span>
                </template>
                <template v-if="s.exclude_patterns.length">
                  <span class="dot">·</span><span>排除 {{ s.exclude_patterns.length }} 条</span>
                </template>
                <template v-if="s.delete_missing">
                  <span class="dot">·</span><span class="danger-text">删除缺失</span>
                </template>
              </div>
            </div>

            <!-- 操作按钮 -->
            <div class="config-actions" @click.stop>
              <el-tooltip
                  :disabled="ownerLoggedIn(s)"
                  content="订阅所属账号未登录，请先登录该账号再触发同步"
                  placement="top"
              >
                  <span>
                    <el-button
                        size="small"
                        type="success"
                        :icon="Refresh"
                        :loading="triggeringId === s.id"
                        :disabled="!ownerLoggedIn(s)"
                        @click="triggerNow(s)"
                    >
                      立即同步
                    </el-button>
                  </span>
              </el-tooltip>
              <el-button
                  v-if="s.link_invalid"
                  size="small"
                  type="primary"
                  :icon="RefreshRight"
                  :loading="resumingId === s.id"
                  @click="resumeNow(s)"
              >
                我已更新链接，恢复
              </el-button>
              <el-button size="small" :icon="Edit" @click="openEdit(s)">编辑</el-button>
              <el-button
                  size="small"
                  :type="s.enabled ? 'warning' : 'success'"
                  :icon="s.enabled ? VideoPause : VideoPlay"
                  @click="toggleEnabled(s)"
              >
                {{ s.enabled ? '停用' : '启用' }}
              </el-button>
              <el-button size="small" type="danger" :icon="Delete" @click="removeSubscription(s)" />
            </div>
          </div>

          <!-- 进行中子任务（内联展示，无需展开；转存段 / 下载段各自独立进度条） -->
          <div v-if="subtasksOf(s.id).length" class="active-task-container">
            <div class="active-task-card">
              <div class="task-progress-header is-toggle" @click.stop="toggleSubtasks(s.id)">
                <div class="task-status-info">
                  <el-icon :size="16" class="status-icon text-blue-500"><Loading class="is-loading" /></el-icon>
                  <span class="task-status-text">进行中子任务（{{ subtasksOf(s.id).length }}）</span>
                </div>
                <el-icon :size="14" class="toggle-icon">
                  <ArrowDown v-if="subtasksExpanded(s.id)" />
                  <ArrowRight v-else />
                </el-icon>
              </div>
              <div v-if="subtasksExpanded(s.id)" class="file-tasks-preview">
                <div v-for="st in subtasksCapped(s.id)" :key="st.task_id" class="subtask-item">
                  <div class="subtask-head">
                    <el-tag :type="st.kind === 'download' ? 'success' : 'warning'" size="small">
                      {{ st.kind === 'download' ? '下载' : '转存' }}
                    </el-tag>
                    <span class="file-name" :title="st.name">{{ st.name }}</span>
                    <span class="subtask-stat">{{ subtaskStat(st) }}</span>
                    <el-tag :type="subtaskStatusColor(st.status)" size="small">{{ subtaskStatusText(st.status) }}</el-tag>
                  </div>
                  <el-progress
                      :percentage="clampPercent(st.progress)"
                      :stroke-width="6"
                      :show-text="false"
                      :status="subtaskProgressStatus(st.status)"
                  />
                </div>
                <div v-if="subtasksOverflow(s.id) > 0" class="subtask-overflow">
                  仅显示前 {{ SUBTASK_RENDER_CAP }} 个，另有 {{ subtasksOverflow(s.id) }} 个进行中…
                </div>
              </div>
            </div>
          </div>
          <div v-else class="no-active-task">
            <span class="idle-text">当前无进行中子任务</span>
            <el-button size="small" text type="primary" @click.stop="openRunsDialog(s)">查看运行历史</el-button>
          </div>
        </el-card>
      </div>
    </div>

    <!-- 订阅详情对话框 -->
    <el-dialog v-model="detailDialogVisible" title="订阅详情" width="640px">
      <template v-if="selected">
        <div class="detail-dialog-toolbar">
          <el-button text :icon="Clock" @click="runsDialogVisible = true">
            运行历史<span v-if="runs.length > 0">（{{ runs.length }}）</span>
          </el-button>
        </div>
        <el-descriptions :column="1" border>
          <el-descriptions-item label="名称">{{ selected.name }}</el-descriptions-item>
          <el-descriptions-item label="分享链接">
            <el-link type="primary" :href="selected.share_url" target="_blank" :underline="false">
              {{ selected.share_url }}
            </el-link>
          </el-descriptions-item>
          <el-descriptions-item label="冲突策略">
            <el-tag :type="strategyTagType(selected.conflict_strategy)">
              {{ describeStrategy(selected.conflict_strategy) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="目标">
            <div v-for="(t, i) in selected.targets" :key="i" class="target-line">
              <el-tag :type="t.kind === 'netdisk' ? 'success' : 'warning'" size="small">
                {{ t.kind === 'netdisk' ? '网盘' : '本地' }}
              </el-tag>
              <span style="margin-left: 6px">
                {{ t.kind === 'netdisk' ? t.remote_path : t.local_path }}
              </span>
            </div>
          </el-descriptions-item>
          <el-descriptions-item label="同步范围">
            <div v-if="selected.include_paths.length > 0" class="path-tags">
              <el-tag
                  v-for="(p, i) in selected.include_paths"
                  :key="`inc-${i}`"
                  size="small"
                  type="info"
              >
                {{ p }}
              </el-tag>
            </div>
            <span v-else>同步整个分享</span>
          </el-descriptions-item>
          <el-descriptions-item label="排除规则">
            <div v-if="selected.exclude_patterns.length > 0" class="path-tags">
              <el-tag
                  v-for="(p, i) in selected.exclude_patterns"
                  :key="`ex-${i}`"
                  size="small"
                  type="info"
              >
                {{ p }}
              </el-tag>
            </div>
            <span v-else>无</span>
          </el-descriptions-item>
          <el-descriptions-item label="轮询">
            <span v-if="isPollEnabled(selected.poll_config)">
              {{ describeInterval(selected.poll_config) }}
            </span>
            <span v-else>已禁用</span>
          </el-descriptions-item>
          <el-descriptions-item label="删除缺失">
            <el-tag v-if="selected.delete_missing" type="danger" size="small">开启</el-tag>
            <el-tag v-else type="info" size="small">关闭</el-tag>
          </el-descriptions-item>
        </el-descriptions>
      </template>
    </el-dialog>

    <!-- 创建/编辑对话框 -->
    <el-dialog
        v-model="dialogVisible"
        title="编辑订阅"
        width="640px"
        @close="resetForm"
    >
      <el-form :model="form" label-width="100px" :rules="formRules" ref="formRef">
        <el-form-item label="名称" prop="name">
          <el-input v-model="form.name" placeholder="如：剧集合集同步" />
        </el-form-item>
        <el-form-item label="分享链接" prop="share_url">
          <el-input v-model="form.share_url" placeholder="https://pan.baidu.com/s/1xxx" />
        </el-form-item>
        <el-form-item label="提取码">
          <el-input v-model="form.password" placeholder="可选" maxlength="4" />
        </el-form-item>
        <el-form-item label="同步范围">
          <ShareIncludeExcludeEditor
              :share-url="form.share_url"
              :password="form.password || null"
              :owner-uid="selected?.owner_uid ?? null"
              :owner-logged-in="selectedOwnerLoggedIn"
              v-model:include-paths="form.include_paths"
              v-model:exclude-patterns="form.exclude_patterns"
          />
        </el-form-item>
        <el-form-item label="冲突策略">
          <el-radio-group v-model="form.conflict_strategy">
            <el-radio-button value="overwrite">覆盖式</el-radio-button>
            <el-radio-button value="versioned">新版本式</el-radio-button>
            <el-radio-button value="skip">跳过</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="同步目标">
          <div class="sync-mode-editor">
            <div class="sync-toggle-row">
              <el-switch v-model="netdiskEnabled" />
              <span class="sync-toggle-label">转存到网盘</span>
            </div>
            <div v-if="netdiskEnabled" class="mode-target-row">
              <span class="mode-target-label">网盘目录</span>
              <el-input
                  v-model="netdiskRemotePath"
                  placeholder="网盘路径，如 /我的资源/同步"
                  style="flex: 1"
              />
            </div>

            <div class="sync-toggle-row">
              <el-switch v-model="localEnabled" />
              <span class="sync-toggle-label">下载到本地</span>
            </div>
            <div v-if="localEnabled" class="mode-target-row">
              <span class="mode-target-label">本地目录</span>
              <span
                  class="mode-target-value"
                  :class="{ 'is-placeholder': !localTargetPath }"
                  @click="openDirPicker()"
              >
                {{ localTargetPath || '点击选择本地目录' }}
              </span>
              <el-button :icon="FolderOpened" @click="openDirPicker()" style="margin-left: 4px">
                选择
              </el-button>
            </div>

            <div class="sync-mode-hint">{{ syncModeHint }}</div>
          </div>
        </el-form-item>
        <el-form-item label="轮询">
          <el-radio-group v-model="form.poll_config.mode">
            <el-radio-button value="interval">固定间隔</el-radio-button>
            <el-radio-button value="scheduled">每日定时</el-radio-button>
            <el-radio-button value="disabled">禁用</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item v-if="form.poll_config.mode === 'interval'" label="间隔（秒）">
          <el-input-number v-model="form.poll_config.interval_secs" :min="600" :max="86400" :step="300" />
          <span style="margin-left: 8px; color: #909399; font-size: 12px">最少 600 秒（10 分钟）</span>
        </el-form-item>
        <el-form-item v-if="form.poll_config.mode === 'scheduled'" label="时刻">
          <el-time-picker
              v-model="scheduledTime"
              format="HH:mm"
              value-format="HH:mm"
              @change="onScheduledChange"
          />
        </el-form-item>
        <el-form-item label="删除缺失">
          <el-switch v-model="form.delete_missing" />
          <span style="margin-left: 8px; color: #909399; font-size: 12px">分享者删除文件时同步删除目标副本</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveForm" :loading="saving">保存</el-button>
      </template>
    </el-dialog>

    <!-- 运行详情对话框 -->
    <el-dialog v-model="runDialogVisible" title="运行详情" width="700px">
      <div v-if="currentRun" class="run-detail">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="状态">
            <el-tag :type="runStatusType(currentRun.status)">{{ describeRunStatus(currentRun.status) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="开始时间">{{ formatTime(currentRun.started_at) }}</el-descriptions-item>
          <el-descriptions-item label="结束时间">
            {{ currentRun.finished_at ? formatTime(currentRun.finished_at) : '—' }}
          </el-descriptions-item>
          <el-descriptions-item label="总文件">{{ runTotalCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="需处理">{{ runChangedCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="新增">{{ currentRun.added_count }}</el-descriptions-item>
          <el-descriptions-item label="修改">{{ currentRun.modified_count }}</el-descriptions-item>
          <el-descriptions-item label="删除">{{ currentRun.removed_count }}</el-descriptions-item>
          <el-descriptions-item label="覆盖">{{ runOverwrittenCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="一致跳过">{{ runUnchangedCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="其它跳过">{{ runSkippedCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="失败">{{ currentRun.failed_count }}</el-descriptions-item>
          <el-descriptions-item label="错误" v-if="currentRun.error">
            <span style="color: #f56c6c">{{ currentRun.error }}</span>
          </el-descriptions-item>
        </el-descriptions>
        <h4 style="margin-top: 16px">文件动作（{{ currentRun.items.length }}）</h4>
        <el-table :data="currentRun.items" size="small" max-height="400">
          <el-table-column prop="path" label="路径" />
          <el-table-column prop="action" label="动作" width="80" :formatter="describeAction" />
          <el-table-column prop="target" label="目标" width="80" :formatter="describeTarget" />
          <el-table-column prop="status" label="状态" width="100" :formatter="describeItemStatus" />
          <el-table-column prop="reason" label="跳过原因" width="120" :formatter="describeReason" />
          <el-table-column prop="error" label="错误" />
        </el-table>
      </div>
    </el-dialog>

    <!-- 运行历史对话框 -->
    <el-dialog v-model="runsDialogVisible" title="运行历史" width="640px">
      <el-empty v-if="runs.length === 0" description="暂无运行" />
      <el-timeline v-else>
        <el-timeline-item
            v-for="r in runs"
            :key="r.id"
            :timestamp="formatTime(r.started_at)"
            :type="runStatusType(r.status)"
        >
          <div @click="openRun(r.id)" class="run-item">
            <strong>{{ describeRunStatus(r.status) }}</strong>
            <div class="run-stats">
              总 {{ runTotalCount(r) }} / 需处理 {{ runChangedCount(r) }}
              <span> +{{ r.added_count }}</span>
              <span> 覆盖 {{ runOverwrittenCount(r) }}</span>
              <span> 一致跳过 {{ runUnchangedCount(r) }}</span>
              <span v-if="runSkippedCount(r) > 0" style="color: #e6a23c"> 跳过 {{ runSkippedCount(r) }}</span>
              <span v-if="r.failed_count > 0" style="color: #f56c6c"> 失败 {{ r.failed_count }}</span>
            </div>
          </div>
        </el-timeline-item>
      </el-timeline>
    </el-dialog>

    <!-- 本地目录选择（对齐转存：FilePickerModal 选目录） -->
    <FilePickerModal
        v-model="dirPickerVisible"
        mode="download"
        select-type="directory"
        title="选择本地目录"
        :initial-path="dirPickerInitialPath"
        :default-download-dir="dirPickerDefaultDir"
        @confirm-download="handleDirConfirm"
        @use-default="handleDirUseDefault"
    />

    <!-- 创建入口：复用转存对话框（默认勾“保持同步”→ 创建订阅） -->
    <TransferDialog
        v-model="showTransferDialog"
        :default-keep-sync="true"
        :lock-keep-sync="true"
        @sync-created="onSyncCreated"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import type { AxiosError } from 'axios'
import { useAuthStore } from '@/stores/auth'
import { useIsMobile } from '@/utils/responsive'
import AccountFilter from '@/components/AccountFilter.vue'
import AccountBadge from '@/components/AccountBadge.vue'
import TransferDialog from '@/components/TransferDialog.vue'
import ShareIncludeExcludeEditor from '@/components/ShareIncludeExcludeEditor.vue'
import { FilePickerModal } from '@/components/FilePicker'
import { getConfig, updateRecentDirDebounced, setDefaultDownloadDir, type DownloadConfig } from '@/api/config'
import {
  Plus, Edit, Delete, Refresh, RefreshRight, Link,
  FolderOpened, VideoPause, VideoPlay, Loading, Clock, ArrowDown, ArrowRight,
} from '@element-plus/icons-vue'
import {
  type ShareSubscription,
  type SyncTarget,
  type NetdiskTarget,
  type LocalTarget,
  type UpdateShareSubscriptionRequest,
  type RunRecord,
  type RunDetail,
  type ConflictStrategy,
  type PollConfig,
  type ShareSyncWsEvent,
  type ShareSyncSubtask,
  listSubscriptions, updateSubscription,
  deleteSubscription, setSubscriptionEnabled, triggerSubscription, resumeSubscription, listRuns, getRun, listSubtasks,
} from '@/api/shareSync'
import { getWebSocketClient, connectWebSocket, type ConnectionState } from '@/utils/websocket'
import { createAdaptivePoller } from '@/utils/backendHealth'

const subscriptions = ref<ShareSubscription[]>([])
const selected = ref<ShareSubscription | null>(null)

const isMobile = useIsMobile()

// 多账号：账号过滤（null=全部账号）
const authStore = useAuthStore()
const ownerFilter = ref<number | null>(null)

// 按账号过滤后的订阅列表（与 transfer/autobackup 一致：null 显示全部）
const displayedSubscriptions = computed(() => {
  if (ownerFilter.value === null) return subscriptions.value
  return subscriptions.value.filter(s => s.owner_uid === ownerFilter.value)
})

// 当前选中订阅的所属账号是否已登录：未登录则禁用依赖网盘 client 的操作（触发同步 / 预览目录树）
// 并提示登录该账号（对齐 AutoBackup：不要求用户先切号，但 owner 未登录时网盘操作不可用）。
const selectedOwnerLoggedIn = computed(() => {
  if (!selected.value) return false
  return authStore.accounts.some(a => a.uid === selected.value!.owner_uid)
})

// 各账号订阅数量（AccountFilter badge 展示）
const ownerFilterCounts = computed(() => {
  const map: Record<number, number> = {}
  for (const s of subscriptions.value) {
    if (typeof s.owner_uid === 'number') map[s.owner_uid] = (map[s.owner_uid] || 0) + 1
  }
  return map
})
const runs = ref<RunRecord[]>([])
const currentRun = ref<RunDetail | null>(null)

// 进行中子任务：subscription_id -> 子任务列表（WS item_progress 实时更新 + REST 轮询兜底）
const activeSubtasks = ref<Map<string, ShareSyncSubtask[]>>(new Map())

function subtasksOf(id: string): ShareSyncSubtask[] {
  return activeSubtasks.value.get(id) ?? []
}

// 子任务列表默认折叠（几千文件时不铺满卡片）；按订阅记忆展开态。
const expandedSubtasks = ref<Set<string>>(new Set())
function subtasksExpanded(id: string): boolean {
  return expandedSubtasks.value.has(id)
}
function toggleSubtasks(id: string) {
  const next = new Set(expandedSubtasks.value)
  if (next.has(id)) next.delete(id)
  else next.add(id)
  expandedSubtasks.value = next
}

// 展开时也只渲染前 N 行（避免上千个 el-progress 撑爆 DOM），其余用计数提示。
const SUBTASK_RENDER_CAP = 200
function subtasksCapped(id: string): ShareSyncSubtask[] {
  return subtasksOf(id).slice(0, SUBTASK_RENDER_CAP)
}
function subtasksOverflow(id: string): number {
  return Math.max(0, subtasksOf(id).length - SUBTASK_RENDER_CAP)
}

// 某订阅所属账号是否已登录（卡片级触发同步前置条件）
function ownerLoggedIn(s: ShareSubscription): boolean {
  return authStore.accounts.some(a => a.uid === s.owner_uid)
}

// 本地目录选择（与转存一致：FilePickerModal 选目录 + 最近目录联动）
const downloadConfig = ref<DownloadConfig | null>(null)
const dirPickerVisible = ref(false)

const dialogVisible = ref(false)
// 创建入口：复用转存对话框
const showTransferDialog = ref(false)
const runDialogVisible = ref(false)
const runsDialogVisible = ref(false)
const detailDialogVisible = ref(false)
const saving = ref(false)
const triggeringId = ref<string | null>(null)
const resumingId = ref<string | null>(null)
const formRef = ref()
const scheduledTime = ref<string>('03:00')

// 路径编辑
function createDefaultTarget(): SyncTarget {
  return { kind: 'local', local_path: '', conflict_strategy: null, mode: 'share_direct' }
}

// 目标模型收窄：最多 1 个网盘 + 1 个本地（可共存=转存并下载）。
function targetKindCounts(): { netdisk: number; local: number } {
  let netdisk = 0
  let local = 0
  for (const t of form.value.targets) {
    if (t.kind === 'netdisk') netdisk++
    else if (t.kind === 'local') local++
  }
  return { netdisk, local }
}

// ==================== 同步目标开关 ====================
// 不再让用户手选模式，改为「转存到网盘」「下载到本地」两个开关，行为由组合自动推导
// （与后端 effective_transfer_ops 一致）：
//   仅网盘        = 仅 Netdisk target
//   网盘+本地     = Netdisk + Local{ mode: transfer_and_download }（后端合并成一条腿：转存一次→从副本下载）
//   仅本地        = 仅 Local{ mode: share_direct }（分享直下：转临时目录→下载→清理，网盘不留存）
// 约束：至少开启一个。

function findNetdiskTarget(): NetdiskTarget | undefined {
  return form.value.targets.find((t): t is NetdiskTarget => t.kind === 'netdisk')
}
function findLocalTarget(): LocalTarget | undefined {
  return form.value.targets.find((t): t is LocalTarget => t.kind === 'local')
}

// 按开关重建目标列表，尽量保留已填的网盘/本地路径，避免误清空
function rebuildTargets(netdisk: boolean, local: boolean) {
  if (!netdisk && !local) {
    ElMessage.warning('至少需要开启一个同步目标（网盘或本地）')
    return
  }
  const nd = findNetdiskTarget()
  const lo = findLocalTarget()
  const remotePath = nd?.remote_path ?? '/'
  const saveFsId = nd?.save_fs_id ?? 0
  const localPath = lo?.local_path ?? ''
  const ndStrategy = nd?.conflict_strategy ?? null
  const loStrategy = lo?.conflict_strategy ?? null
  const next: SyncTarget[] = []
  if (netdisk) {
    next.push({ kind: 'netdisk', remote_path: remotePath, save_fs_id: saveFsId, conflict_strategy: ndStrategy })
  }
  if (local) {
    // 有网盘目标=转存并下载（复用网盘副本）；无=分享直下。后端最终按目标存在性推导，这里保持一致。
    next.push({ kind: 'local', local_path: localPath, conflict_strategy: loStrategy, mode: netdisk ? 'transfer_and_download' : 'share_direct' })
  }
  form.value.targets = next
}

const netdiskEnabled = computed<boolean>({
  get() { return !!findNetdiskTarget() },
  set(on: boolean) { rebuildTargets(on, !!findLocalTarget()) },
})
const localEnabled = computed<boolean>({
  get() { return !!findLocalTarget() },
  set(on: boolean) { rebuildTargets(!!findNetdiskTarget(), on) },
})

const syncModeHint = computed(() => {
  const nd = netdiskEnabled.value
  const lo = localEnabled.value
  if (nd && lo) return '转存到网盘目录并保留，再从这份网盘副本下载到本地（只转存一次，不会重复转存）。'
  if (nd) return '只把分享内容转存到指定网盘目录并保留，不下载到本地。'
  return '分享直下：转存到临时目录后下载到本地，下载完成即清理，网盘不留存。'
})

const netdiskRemotePath = computed<string>({
  get() { return findNetdiskTarget()?.remote_path ?? '' },
  set(v: string) {
    const nd = findNetdiskTarget()
    if (nd) nd.remote_path = v
  },
})
const localTargetPath = computed<string>({
  get() { return findLocalTarget()?.local_path ?? '' },
  set(v: string) {
    const lo = findLocalTarget()
    if (lo) lo.local_path = v
  },
})

const defaultForm = (): {
  name: string
  share_url: string
  password: string
  include_paths: string[]
  exclude_patterns: string[]
  targets: SyncTarget[]
  conflict_strategy: ConflictStrategy
  delete_missing: boolean
  poll_config: PollConfig
} => ({
  name: '',
  share_url: '',
  password: '',
  include_paths: [],
  exclude_patterns: [],
  targets: [createDefaultTarget()],
  conflict_strategy: 'overwrite',
  delete_missing: false,
  poll_config: { enabled: true, mode: 'interval', interval_secs: 1800, schedule_hour: null, schedule_minute: null },
})

const form = ref(defaultForm())

function normalizePollConfigForUi(p?: PollConfig | null): PollConfig {
  const rawMode = p?.mode || 'interval'
  const enabled = p?.enabled !== false
  const mode = (!enabled || rawMode === 'disabled' ? 'disabled' : rawMode) as PollConfig['mode']
  const intervalSecs = Number.isFinite(Number(p?.interval_secs))
      ? Math.max(600, Number(p?.interval_secs))
      : 1800

  return {
    enabled: mode !== 'disabled',
    mode,
    interval_secs: intervalSecs,
    schedule_hour: mode === 'scheduled' ? (p?.schedule_hour ?? 3) : null,
    schedule_minute: mode === 'scheduled' ? (p?.schedule_minute ?? 0) : null,
  }
}

function normalizePollConfigForSubmit(p: PollConfig): PollConfig {
  const mode = (p.mode || 'interval') as PollConfig['mode']
  const intervalSecs = Number.isFinite(Number(p.interval_secs))
      ? Math.max(600, Number(p.interval_secs))
      : 1800

  if (mode === 'disabled') {
    return {
      enabled: false,
      mode: 'disabled',
      interval_secs: intervalSecs,
      schedule_hour: null,
      schedule_minute: null,
    }
  }

  if (mode === 'interval') {
    return {
      enabled: true,
      mode: 'interval',
      interval_secs: intervalSecs,
      schedule_hour: null,
      schedule_minute: null,
    }
  }

  return {
    enabled: true,
    mode: 'scheduled',
    interval_secs: intervalSecs,
    schedule_hour: p.schedule_hour ?? 3,
    schedule_minute: p.schedule_minute ?? 0,
  }
}

const formRules = {
  name: [{ required: true, message: '请输入名称', trigger: 'blur' }],
  share_url: [
    { required: true, message: '请输入分享链接', trigger: 'blur' },
    { pattern: /pan\.baidu\.com/, message: '必须是 pan.baidu.com 链接', trigger: 'blur' },
  ],
}

// ==================== 数据加载 ====================

async function refresh() {
  try {
    subscriptions.value = await listSubscriptions()
  } catch (e) {
    ElMessage.error(`加载订阅失败: ${getApiErrorMessage(e)}`)
    return
  }
  if (selected.value) {
    const fresh = subscriptions.value.find(s => s.id === selected.value!.id)
    if (fresh) selected.value = fresh
  }
}

async function select(s: ShareSubscription) {
  selected.value = s
  await loadRuns(s.id)
}

// 打开订阅详情弹窗
async function openDetail(s: ShareSubscription) {
  await select(s)
  detailDialogVisible.value = true
}

// 直接打开运行历史弹窗
async function openRunsDialog(s: ShareSubscription) {
  await select(s)
  runsDialogVisible.value = true
}

// 切换账号过滤时，若当前选中项被过滤掉，则清空选择，保持详情面板与列表一致
watch(ownerFilter, () => {
  if (!selected.value) return
  if (!displayedSubscriptions.value.some(s => s.id === selected.value!.id)) {
    selected.value = null
    runs.value = []
  }
})

async function loadRuns(id: string) {
  try {
    runs.value = await listRuns(id, 1, 30)
  } catch (e) {
    runs.value = []
    const status = (e as AxiosError)?.response?.status
    if (status !== 404) {
      console.error('load runs failed', e)
    }
  }
}

// 转存对话框创建订阅后回调（WS 事件也会刷新，这里显式刷一次更可靠）
function onSyncCreated() {
  refresh()
}

function openEdit(s?: ShareSubscription) {
  if (s) selected.value = s
  if (!selected.value) return
  form.value = {
    name: selected.value.name,
    share_url: selected.value.share_url,
    password: selected.value.password || '',
    include_paths: [...selected.value.include_paths],
    exclude_patterns: [...selected.value.exclude_patterns],
    targets: selected.value.targets.map(t => ({ ...t })) as SyncTarget[],
    conflict_strategy: selected.value.conflict_strategy,
    delete_missing: selected.value.delete_missing,
    poll_config: normalizePollConfigForUi(selected.value.poll_config),
  }
  if (form.value.poll_config.mode === 'scheduled') {
    const h = String(form.value.poll_config.schedule_hour || 0).padStart(2, '0')
    const m = String(form.value.poll_config.schedule_minute || 0).padStart(2, '0')
    scheduledTime.value = `${h}:${m}`
  } else {
    scheduledTime.value = '03:00'
    form.value.poll_config.schedule_hour = null
    form.value.poll_config.schedule_minute = null
  }
  dialogVisible.value = true
}

function getApiErrorMessage(e: unknown): string {
  const ax = e as AxiosError<{ message?: string; error?: string; msg?: string; details?: string }>
  return ax?.response?.data?.message
      || ax?.response?.data?.error
      || ax?.response?.data?.msg
      || ax?.response?.data?.details
      || (e as Error)?.message
      || '未知错误'
}

function resetForm() {
  formRef.value?.resetFields?.()
}

async function saveForm() {
  try {
    await formRef.value.validate()
  } catch {
    return
  }
  syncScheduledTimeFromPicker()
  buildSanitizedPayload()
  if (!validateForm()) {
    return
  }
  saving.value = true
  try {
    if (selected.value) {
      const req: UpdateShareSubscriptionRequest = {
        name: form.value.name,
        share_url: form.value.share_url,
        password: form.value.password || null,
        include_paths: form.value.include_paths,
        exclude_patterns: form.value.exclude_patterns,
        targets: form.value.targets,
        conflict_strategy: form.value.conflict_strategy,
        delete_missing: form.value.delete_missing,
        poll_config: form.value.poll_config,
      }
      await updateSubscription(selected.value.id, req)
      ElMessage.success('已更新订阅')
    }
    dialogVisible.value = false
    await refresh()
  } catch (e) {
    ElMessage.error(`保存失败: ${getApiErrorMessage(e)}`)
  } finally {
    saving.value = false
  }
}

function validateForm(): boolean {
  if (form.value.targets.length === 0) {
    ElMessage.error('请至少配置一个同步目标')
    return false
  }

  // 目标模型收窄：最多 1 网盘 + 1 本地（与后端 validate 一致）
  const { netdisk, local } = targetKindCounts()
  if (netdisk > 1) {
    ElMessage.error('最多只能配置 1 个网盘目标')
    return false
  }
  if (local > 1) {
    ElMessage.error('最多只能配置 1 个本地目标')
    return false
  }

  for (let i = 0; i < form.value.targets.length; i++) {
    const t = form.value.targets[i] as NetdiskTarget | LocalTarget
    if (t.kind === 'netdisk') {
      if (!t.remote_path || !String(t.remote_path).trim()) {
        ElMessage.error(`目标 #${i + 1}：网盘路径不能为空`)
        return false
      }
    } else if (t.kind === 'local') {
      // 仅校验非空；绝对路径/目录存在/可写交给后端 validate_local_path()
      // （按平台用 Path::is_absolute() 判断，避免前端写死 Linux 的 / 前缀误伤 Windows 的 D:\）
      const lp = String(t.local_path || '').trim()
      if (!lp) {
        ElMessage.error(`目标 #${i + 1}：本地路径不能为空`)
        return false
      }
    } else {
      ElMessage.error(`目标 #${i + 1}：未知目标类型`)
      return false
    }
  }

  if (form.value.poll_config.mode === 'interval' && form.value.poll_config.interval_secs < 600) {
    ElMessage.error('间隔模式下最少间隔为 600 秒（10 分钟）')
    return false
  }

  return true
}

function syncScheduledTimeFromPicker() {
  if (form.value.poll_config.mode === 'disabled') {
    form.value.poll_config.enabled = false
    form.value.poll_config.schedule_hour = null
    form.value.poll_config.schedule_minute = null
    return
  }

  form.value.poll_config.enabled = true
  if (form.value.poll_config.mode !== 'scheduled') {
    form.value.poll_config.schedule_hour = null
    form.value.poll_config.schedule_minute = null
    return
  }

  const [hourStr = '03', minuteStr = '00'] = (scheduledTime.value || '03:00').split(':')
  const h = Number(hourStr)
  const m = Number(minuteStr)
  const hour = Number.isFinite(h) ? h : 3
  const minute = Number.isFinite(m) ? m : 0
  form.value.poll_config.schedule_hour = Math.min(23, Math.max(0, Math.round(hour)))
  form.value.poll_config.schedule_minute = Math.min(59, Math.max(0, Math.round(minute)))
  scheduledTime.value = `${String(form.value.poll_config.schedule_hour).padStart(2, '0')}:${String(form.value.poll_config.schedule_minute).padStart(2, '0')}`
}

function normalizePath(v: string): string {
  const s = v.trim().replace(/\/+/g, '/')
  if (!s) return ''
  const prefixed = s.startsWith('/') ? s : `/${s}`
  if (prefixed.length === 1) return '/'
  return prefixed.endsWith('/') ? prefixed.slice(0, -1) : prefixed
}

function normalizeRemotePath(v: string): string {
  return normalizePath(v) || '/'
}

function buildSanitizedPayload() {
  const includeSet = new Set<string>()
  for (const p of form.value.include_paths) {
    const n = normalizePath(p)
    if (n) {
      includeSet.add(n)
    }
  }
  const excludeSet = new Set<string>()
  for (const p of form.value.exclude_patterns) {
    const n = p.trim()
    if (n) {
      excludeSet.add(n)
    }
  }
  form.value.include_paths = Array.from(includeSet)
  form.value.exclude_patterns = Array.from(excludeSet)

  const nextTargets: SyncTarget[] = []
  for (const raw of form.value.targets) {
    const t = raw as NetdiskTarget | LocalTarget
    if (!t.kind) {
      continue
    }
    if (t.kind === 'netdisk') {
      const remote = normalizeRemotePath(String(t.remote_path || ''))
      nextTargets.push({
        kind: 'netdisk',
        remote_path: remote,
        save_fs_id: Number.isFinite(Number(t.save_fs_id)) ? Number(t.save_fs_id) : 0,
        ...(t.conflict_strategy ? { conflict_strategy: t.conflict_strategy } : {}),
      })
    } else if (t.kind === 'local') {
      // 本地路径只做 trim，不做 / 归一化（保留 Windows 的 D:\ 等平台路径原样交后端）
      const local = String(t.local_path || '').trim()
      nextTargets.push({
        kind: 'local',
        local_path: local,
        mode: t.mode === 'transfer_and_download' ? 'transfer_and_download' : 'share_direct',
        ...(t.conflict_strategy ? { conflict_strategy: t.conflict_strategy } : {}),
      })
    }
  }
  form.value.targets = nextTargets
  form.value.poll_config = normalizePollConfigForSubmit(form.value.poll_config)
}

async function removeSubscription(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  try {
    await ElMessageBox.confirm(
        `确定删除订阅 "${target.name}"？历史快照与运行记录将一并清理。`,
        '删除确认',
        { type: 'warning' }
    )
  } catch {
    return
  }
  try {
    await deleteSubscription(target.id)
    ElMessage.success('已删除')
    if (selected.value?.id === target.id) {
      selected.value = null
      runs.value = []
    }
    activeSubtasks.value.delete(target.id)
    await refresh()
  } catch (e) {
    ElMessage.error(`删除失败: ${getApiErrorMessage(e)}`)
  }
}

async function toggleEnabled(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  try {
    await setSubscriptionEnabled(target.id, !target.enabled)
    ElMessage.success('已切换启用状态')
    await refresh()
  } catch (e) {
    ElMessage.error(`操作失败: ${getApiErrorMessage(e)}`)
  }
}

async function resumeNow(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  resumingId.value = target.id
  try {
    await resumeSubscription(target.id)
    ElMessage.success('已恢复轮询并立即重试一次')
    await refresh()
  } catch (e) {
    ElMessage.error(`恢复失败: ${getApiErrorMessage(e)}`)
  } finally {
    resumingId.value = null
  }
}

async function triggerNow(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  triggeringId.value = target.id
  try {
    await triggerSubscription(target.id)
    ElMessage.success('已触发同步，结果将稍后出现在运行历史')
    setTimeout(() => {
      if (selected.value?.id === target.id) loadRuns(target.id)
      loadSubtasksFor(target.id)
    }, 1500)
  } catch (e) {
    ElMessage.error(`触发失败: ${getApiErrorMessage(e)}`)
  } finally {
    triggeringId.value = null
  }
}

async function openRun(runId: string) {
  try {
    currentRun.value = await getRun(runId)
    runDialogVisible.value = true
  } catch (e) {
    ElMessage.error(`加载运行详情失败: ${getApiErrorMessage(e)}`)
  }
}

// ==================== 本地目录选择（对齐转存） ====================

const dirPickerInitialPath = computed(
    () => downloadConfig.value?.recent_directory
        || downloadConfig.value?.default_directory
        || downloadConfig.value?.download_dir
        || '',
)
const dirPickerDefaultDir = computed(
    () => downloadConfig.value?.default_directory || downloadConfig.value?.download_dir || '',
)

function openDirPicker() {
  dirPickerVisible.value = true
}

function applyPickedDir(path: string) {
  const lo = findLocalTarget()
  if (lo) {
    lo.local_path = String(path || '').trim()
  }
}

// FilePickerModal mode="download"：选定目录（path 原样，不做归一化）
function handleDirConfirm(payload: { path: string; setAsDefault: boolean }) {
  const { path, setAsDefault } = payload
  dirPickerVisible.value = false
  applyPickedDir(path)
  if (setAsDefault) {
    setDefaultDownloadDir({ path })
        .then(() => { if (downloadConfig.value) downloadConfig.value.default_directory = path })
        .catch(() => { /* 设默认失败不阻断填写 */ })
  }
  // 与转存一致：联动最近下载目录
  updateRecentDirDebounced({ dir_type: 'download', path })
  if (downloadConfig.value) downloadConfig.value.recent_directory = path
}

function handleDirUseDefault() {
  dirPickerVisible.value = false
  const target = dirPickerDefaultDir.value
  if (target) applyPickedDir(target)
}

// ==================== 路径编辑 / 树选择 ====================

function onScheduledChange(val: string | null) {
  if (form.value.poll_config.mode !== 'scheduled') {
    return
  }
  if (!val) return
  syncScheduledTimeFromPicker()
}

// ==================== 辅助显示 ====================

function isPollEnabled(p: PollConfig): boolean {
  return normalizePollConfigForUi(p).mode !== 'disabled'
}

function describeInterval(p: PollConfig): string {
  const normalized = normalizePollConfigForUi(p)
  if (normalized.mode === 'disabled') return '已禁用'
  if (normalized.mode === 'interval') {
    const m = Math.floor(normalized.interval_secs / 60)
    return `每 ${m} 分钟`
  }
  if (normalized.mode === 'scheduled') {
    const h = String(normalized.schedule_hour || 0).padStart(2, '0')
    const m = String(normalized.schedule_minute || 0).padStart(2, '0')
    return `每日 ${h}:${m}`
  }
  return '已禁用'
}

function describeTargets(targets: SyncTarget[]): string {
  return targets.map(t => t.kind === 'netdisk' ? '网盘' : '本地').join(' + ')
}

function describeStrategy(s: ConflictStrategy): string {
  return s === 'overwrite' ? '覆盖式' : s === 'versioned' ? '新版本式' : '跳过'
}

function strategyTagType(s: ConflictStrategy): 'success' | 'warning' | 'info' {
  return s === 'overwrite' ? 'success' : s === 'versioned' ? 'warning' : 'info'
}

function describeRunStatus(s: string): string {
  return s === 'running' ? '运行中' :
      s === 'completed' ? '已完成' :
          s === 'completed_with_errors' ? '完成（部分失败）' :
              s === 'failed' ? '失败' :
                  s === 'interrupted' ? '已中断（自动重跑）' : s
}

function runStatusType(s: string): 'success' | 'warning' | 'danger' | 'info' {
  return s === 'completed' ? 'success' :
      s === 'completed_with_errors' ? 'warning' :
          s === 'failed' ? 'danger' : 'info'
}

// 运行历史「文件动作」表格里的列值原本是后端枚举英文（added/netdisk/failed…），
// 这里统一汉化展示。
const ACTION_LABELS: Record<string, string> = {
  added: '新增',
  modified: '修改',
  removed: '删除',
  skipped: '跳过',
}
const TARGET_LABELS: Record<string, string> = {
  netdisk: '网盘',
  local: '本地',
  netdisk_and_local: '网盘+本地',
}
const ITEM_STATUS_LABELS: Record<string, string> = {
  pending: '等待',
  transferring: '转存中',
  downloading: '下载中',
  deleting: '删除中',
  completed: '完成',
  failed: '失败',
  skipped: '跳过',
}
const REASON_LABELS: Record<string, string> = {
  quota_full: '网盘空间不足',
  local_disk_full: '本地磁盘空间不足',
  skip_due_to_quota_full: '网盘空间不足',
  skip_due_to_local_disk_full: '本地磁盘空间不足',
}
function describeAction(_r: unknown, _c: unknown, v: string): string {
  return ACTION_LABELS[v] ?? v
}
function describeTarget(_r: unknown, _c: unknown, v: string): string {
  return TARGET_LABELS[v] ?? v
}
function describeItemStatus(_r: unknown, _c: unknown, v: string): string {
  return ITEM_STATUS_LABELS[v] ?? v
}
function describeReason(_r: unknown, _c: unknown, v: string | null | undefined): string {
  if (!v) return '—'
  return REASON_LABELS[v] ?? v
}

function runTotalCount(r: RunRecord | RunDetail): number {
  return r.total_count || (r.added_count + r.modified_count + r.removed_count + runUnchangedCount(r))
}

function runChangedCount(r: RunRecord | RunDetail): number {
  return r.added_count + r.modified_count + r.removed_count
}

function runUnchangedCount(r: RunRecord | RunDetail): number {
  return r.unchanged_count ?? 0
}

function runSkippedCount(r: RunRecord | RunDetail): number {
  return r.skipped_count ?? 0
}

function runOverwrittenCount(r: RunRecord | RunDetail): number {
  return r.overwritten_count ?? 0
}

function formatTime(ts: number): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString('zh-CN')
}

// ==================== 子任务进度（内联展示） ====================

function formatBytes(bytes: number): string {
  if (!bytes || bytes <= 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.min(sizes.length - 1, Math.floor(Math.log(bytes) / Math.log(k)))
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`
}

function formatSpeed(bps: number): string {
  if (!bps || bps <= 0) return '0 B/s'
  return `${formatBytes(bps)}/s`
}

function clampPercent(p: number): number {
  if (!Number.isFinite(p)) return 0
  return Math.min(100, Math.max(0, Math.round(p)))
}

// 预计剩余时间（秒）格式化，与自动备份 `formatETA` 口径一致
function formatEtaSeconds(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return '--'
  if (seconds < 60) return `${Math.ceil(seconds)} 秒`
  if (seconds < 3600) {
    const minutes = Math.floor(seconds / 60)
    const secs = Math.ceil(seconds % 60)
    return `${minutes} 分 ${secs} 秒`
  }
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  return `${hours} 小时 ${minutes} 分`
}

// 下载段：已下载 / 总大小 · 速度 · 剩余预计；转存段：已完成 / 总文件数
function subtaskStat(st: ShareSyncSubtask): string {
  if (st.kind === 'download') {
    let s = `${formatBytes(st.downloaded)} / ${formatBytes(st.total)}`
    if (st.speed > 0) {
      s += ` · ${formatSpeed(st.speed)}`
      const eta = st.eta_seconds ?? (st.total > st.downloaded ? (st.total - st.downloaded) / st.speed : null)
      if (eta != null && eta > 0) s += ` · 剩余 ${formatEtaSeconds(eta)}`
    }
    return s
  }
  return `${st.downloaded}/${st.total} 文件`
}

const SUBTASK_TERMINAL = new Set(['completed', 'failed', 'cancelled', 'success'])

function subtaskStatusText(status: string): string {
  const map: Record<string, string> = {
    pending: '等待中',
    queued: '排队中',
    preparing: '准备中',
    waiting_transfer: '等待传输',
    transferring: '转存中',
    downloading: '下载中',
    paused: '已暂停',
    completed: '已完成',
    success: '已完成',
    failed: '失败',
    cancelled: '已取消',
  }
  return map[status] || status
}

function subtaskStatusColor(status: string): 'success' | 'warning' | 'danger' | 'info' | 'primary' {
  switch (status) {
    case 'completed':
    case 'success': return 'success'
    case 'failed': return 'danger'
    case 'cancelled':
    case 'paused': return 'warning'
    case 'transferring':
    case 'downloading':
    case 'preparing':
    case 'waiting_transfer': return 'primary'
    default: return 'info'
  }
}

function subtaskProgressStatus(status: string): '' | 'success' | 'exception' | 'warning' {
  if (status === 'failed') return 'exception'
  if (status === 'paused') return 'warning'
  return ''
}

// WS item_progress 到达：按 task_id upsert；终态则移除（与 REST「仅返回进行中」语义一致）
function upsertSubtask(sid: string, st: ShareSyncSubtask) {
  const list = activeSubtasks.value.get(sid) ?? []
  const idx = list.findIndex(x => x.task_id === st.task_id)
  if (SUBTASK_TERMINAL.has(st.status)) {
    if (idx >= 0) list.splice(idx, 1)
  } else if (idx >= 0) {
    list[idx] = st
  } else {
    list.push(st)
  }
  if (list.length > 0) {
    activeSubtasks.value.set(sid, list)
  } else {
    activeSubtasks.value.delete(sid)
  }
  // 触发 Map 的响应式刷新（Map 的原地修改不会触发 ref 更新）
  activeSubtasks.value = new Map(activeSubtasks.value)
}

async function loadSubtasksFor(id: string) {
  try {
    const list = await listSubtasks(id)
    // 防御：只展示未到终态的子任务。后端「进行中」接口已过滤终态，这里再兜一层，
    // 与 WS upsertSubtask 的剔除口径一致，避免切换页面后已完成的文件夹被当成进行中。
    const active = list.filter(st => !SUBTASK_TERMINAL.has(st.status))
    const next = new Map(activeSubtasks.value)
    if (active.length > 0) {
      next.set(id, active)
    } else {
      next.delete(id)
    }
    activeSubtasks.value = next
  } catch {
    // 子任务加载失败不阻断页面（账号未就绪时后端返回空，异常时静默）
  }
}

async function loadSubtasksForAll() {
  await Promise.all(subscriptions.value.map(s => loadSubtasksFor(s.id)))
}

// ==================== 生命周期 ====================

let unsubWs: (() => void) | null = null
let unsubConnState: (() => void) | null = null
const wsConnected = ref(false)

const hasActiveSubtasks = computed(() => {
  for (const list of activeSubtasks.value.values()) {
    if (list.length > 0) return true
  }
  return false
})

// 自适应轮询器：WS 未连接且存在活跃子任务时兜底刷新 /subtasks
const subtaskPoller = createAdaptivePoller(() => {
  if (wsConnected.value || !hasActiveSubtasks.value) {
    subtaskPoller.stop()
    return
  }
  loadSubtasksForAll()
}, { baseDelayMs: 2000, maxDelayMs: 30000 })

function updateSubtaskPolling() {
  if (!wsConnected.value && hasActiveSubtasks.value) {
    subtaskPoller.start()
  } else {
    subtaskPoller.stop()
  }
}

watch(hasActiveSubtasks, updateSubtaskPolling)

onMounted(async () => {
  await refresh()
  if (subscriptions.value.length > 0 && !selected.value) {
    await select(subscriptions.value[0])
  }

  // 初始拉取一次进行中子任务（与 AutoBackup 一致：进页面即有进度，不空窗）
  await loadSubtasksForAll()

  // 加载下载目录配置（本地目标选目录时用作初始/默认目录，与转存一致）
  try {
    const appConfig = await getConfig()
    downloadConfig.value = appConfig.download
  } catch {
    // 配置加载失败不阻断页面；选目录时回退到根目录
  }

  // 订阅 WebSocket
  connectWebSocket()
  const ws = getWebSocketClient()
  ws.subscribe(['share_sync'])
  const handler = (event: CustomEvent<ShareSyncWsEvent>) => {
    const evt = event?.detail
    if (!evt || !evt.type) return
    const sid = evt.subscription_id
    if (!sid) return
    if (evt.type === 'item_progress') {
      upsertSubtask(sid, {
        task_id: evt.task_id,
        name: evt.name,
        kind: evt.kind,
        status: evt.status,
        downloaded: evt.downloaded,
        total: evt.total,
        progress: evt.progress,
        speed: evt.speed,
        eta_seconds: evt.eta_seconds ?? null,
        owner_uid: evt.owner_uid ?? 0,
      })
      return
    }
    if (['subscription_created', 'subscription_updated', 'subscription_deleted', 'status_changed'].includes(evt.type)) {
      refresh()
      return
    }
    if (evt.type === 'run_started') {
      // 新一轮开始：清掉旧的进度残留，随后由 item_progress / 轮询补齐
      loadSubtasksFor(sid)
    }
    if (['run_completed', 'run_failed'].includes(evt.type)) {
      // 一轮结束：清空该订阅的进行中子任务
      const next = new Map(activeSubtasks.value)
      next.delete(sid)
      activeSubtasks.value = next
    }
    if (sid === selected.value?.id) {
      if (['run_started', 'run_completed', 'run_failed', 'diff_detected'].includes(evt.type)) {
        loadRuns(sid)
      }
      // run_completed 不再弹提示（结果可在运行历史里看）；仅失败时提醒。
      if (evt.type === 'run_failed') {
        ElMessage.error(`同步失败：${evt.error || '未知错误'}`)
      }
    }
  }
  unsubWs = ws.onShareSyncEvent(handler)

  // 连接状态：断线时启动轮询兜底，恢复后停止并刷新一次进行中子任务
  unsubConnState = ws.onConnectionStateChange((state: ConnectionState) => {
    const wasConnected = wsConnected.value
    wsConnected.value = state === 'connected'
    updateSubtaskPolling()
    if (!wasConnected && wsConnected.value) {
      loadSubtasksForAll()
    }
  })
  updateSubtaskPolling()
})

onUnmounted(() => {
  unsubWs?.()
  unsubConnState?.()
  subtaskPoller.stop()
})
</script>

<style scoped lang="scss">
.share-sync-view {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  background: #f5f5f5;
}

// 顶部工具栏（与转存管理等页面一致）
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

.ss-content {
  flex: 1;
  overflow: auto;
  padding: 16px 20px;
}

// ==================== 订阅卡片列表（自动备份风格） ====================
.ss-list-title {
  font-weight: 500;
  color: #303133;
  margin-bottom: 12px;
}

.config-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.config-card {
  border-left: 4px solid #409eff;
  transition: all 0.3s;
  cursor: pointer;

  &.is-disabled { border-left-color: #c0c4cc; }
  &.active { box-shadow: 0 0 0 1px #409eff inset; }
  &:hover { transform: translateY(-2px); }
}

.config-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}
.config-info { flex: 1; min-width: 0; }
.config-title {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
  flex-wrap: wrap;
  .direction-icon { flex-shrink: 0; color: #409eff; }
  .config-name { font-size: 16px; font-weight: 500; color: #333; }
}
.config-path {
  font-size: 12px;
  color: #909399;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px;
  .dot { color: #c0c4cc; }
  .danger-text { color: #f56c6c; }
}
.config-actions {
  display: flex;
  gap: 8px;
  flex-shrink: 0;
  flex-wrap: wrap;
}

// 进行中子任务（内联）
.active-task-container {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid #ebeef5;
}
.active-task-card {
  background: #f5f7fa;
  border-radius: 8px;
  overflow: hidden;
}
.task-progress-header {
  display: flex;
  align-items: center;
  padding: 10px 12px 4px;
  &.is-toggle { cursor: pointer; user-select: none; }
  .toggle-icon { margin-left: auto; color: #909399; }
}
.task-status-info {
  display: flex;
  align-items: center;
  gap: 8px;
  .task-status-text { font-size: 14px; font-weight: 500; color: #303133; }
  .status-icon.text-blue-500 { color: #409eff; }
  .is-loading { animation: ss-rotate 1.2s linear infinite; }
}
// 固定高度 + 滚动条：几千个子任务时不再撑满卡片
.file-tasks-preview {
  padding: 4px 12px 12px;
  max-height: 320px;
  overflow-y: auto;
}
.subtask-overflow {
  padding: 8px 0 2px;
  font-size: 12px;
  color: #909399;
  text-align: center;
}
.subtask-item {
  padding: 8px 0;
  border-bottom: 1px solid #ebeef5;
  &:last-child { border-bottom: none; }
}
.subtask-head {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
  .file-name {
    font-size: 13px;
    color: #303133;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex: 1;
    min-width: 0;
  }
  .subtask-stat { font-size: 12px; color: #909399; flex-shrink: 0; }
}

.no-active-task {
  margin-top: 12px;
  padding: 12px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: #f5f7fa;
  border-radius: 6px;
  .idle-text { font-size: 13px; color: #909399; }
}

@keyframes ss-rotate {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.detail-dialog-toolbar {
  display: flex;
  justify-content: flex-end;
  margin-bottom: 8px;
}

.target-line { margin: 4px 0; }
.target-form-row { display: flex; align-items: center; margin-bottom: 8px; }
.target-tip { margin-left: 8px; font-size: 12px; color: #909399; }

.sync-mode-editor {
  width: 100%;
  .sync-mode-hint { margin: 8px 0 4px; font-size: 12px; color: #909399; }
  .mode-target-row {
    display: flex;
    align-items: center;
    margin-top: 8px;
    .mode-target-label {
      width: 64px;
      flex-shrink: 0;
      font-size: 13px;
      color: #606266;
    }
    .mode-target-value {
      flex: 1;
      min-width: 0;
      padding: 0 11px;
      height: 32px;
      line-height: 32px;
      border: 1px solid var(--el-border-color);
      border-radius: 4px;
      font-size: 14px;
      color: #303133;
      cursor: pointer;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      &:hover { border-color: var(--el-color-primary); }
      &.is-placeholder { color: #a8abb2; }
    }
  }
}

.run-item { cursor: pointer; &:hover { color: #409eff; } }
.run-stats { font-size: 12px; color: #909399; margin-top: 2px; }

.run-detail { h4 { margin: 16px 0 8px; } }

.path-editor {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}
.path-empty, .path-empty-inline {
  color: #909399;
  font-size: 12px;
}
.path-tags { display: flex; flex-wrap: wrap; align-items: center; }
.path-actions { display: flex; align-items: center; }

.tree-picker-toolbar {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
  flex-wrap: wrap;
  gap: 4px;
}
.tree-picker-hint { margin-right: 12px; color: #909399; font-size: 12px; }
.tree-node { display: inline-flex; align-items: center; gap: 4px; }
.tree-size { margin-left: 6px; color: #909399; font-size: 11px; }
</style>
