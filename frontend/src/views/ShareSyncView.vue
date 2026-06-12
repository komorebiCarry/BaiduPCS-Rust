<template>
  <div class="share-sync-view">
    <el-page-header :icon="ArrowRight" content="分享同步" class="page-header">
      <template #content>
        <span class="page-title">分享同步</span>
      </template>
    </el-page-header>

    <p class="page-desc">
      订阅第三方分享链接，自动监听内容更新并按"覆盖式 / 新版本式 / 跳过"策略把变更同步到网盘目录或本地目录。
    </p>

    <el-row :gutter="16">
      <!-- 左侧：订阅列表 -->
      <el-col :xs="24" :md="8">
        <el-card shadow="hover" class="list-card">
          <template #header>
            <div class="card-header">
              <span>订阅列表（{{ subscriptions.length }}）</span>
              <el-button type="primary" size="small" :icon="Plus" @click="openCreate">新增</el-button>
            </div>
          </template>
          <el-empty v-if="subscriptions.length === 0" description="还没有订阅" />
          <div v-else class="sub-list">
            <div
              v-for="s in subscriptions"
              :key="s.id"
              class="sub-item"
              :class="{ active: selected?.id === s.id }"
              @click="select(s)"
            >
              <div class="sub-name">
                <el-icon><Link /></el-icon>
  <span>{{ s.name }}</span>
                <el-tag v-if="!s.enabled" size="small" type="info">已停用</el-tag>
              </div>
              <div class="sub-meta">
                <span>{{ describeInterval(s.poll_config) }}</span>
                <span>·</span>
                <span>{{ describeTargets(s.targets) }}</span>
              </div>
            </div>
          </div>
        </el-card>
      </el-col>

      <!-- 中间：详情 + 操作 -->
      <el-col :xs="24" :md="10">
        <el-card v-if="selected" shadow="hover" class="detail-card">
          <template #header>
            <div class="card-header">
              <span>订阅详情</span>
              <div>
                <el-button size="small" :icon="Edit" @click="openEdit">编辑</el-button>
                <el-button
                  size="small"
                  :type="selected.enabled ? 'warning' : 'success'"
                  :icon="selected.enabled ? VideoPause : VideoPlay"
                  @click="toggleEnabled"
                >
                  {{ selected.enabled ? '停用' : '启用' }}
                </el-button>
                <el-button size="small" type="success" :icon="Refresh" @click="triggerNow" :loading="triggering">
                  立即同步
                </el-button>
                <el-button size="small" type="danger" :icon="Delete" @click="removeSubscription">删除</el-button>
              </div>
            </div>
          </template>

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
        </el-card>
        <el-empty v-else description="请选择订阅查看详情" />
      </el-col>

      <!-- 右侧：运行历史 -->
      <el-col :xs="24" :md="6">
        <el-card v-if="selected" shadow="hover" class="runs-card">
          <template #header>
            <span>运行历史</span>
          </template>
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
        </el-card>
      </el-col>
    </el-row>

    <!-- 创建/编辑对话框 -->
    <el-dialog
      v-model="dialogVisible"
      :title="dialogMode === 'create' ? '新增订阅' : '编辑订阅'"
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
        <el-form-item label="同步路径">
          <div class="path-editor">
            <div v-if="form.include_paths.length === 0" class="path-empty">
              不填则同步整个分享；填写后只同步勾选的子路径（前缀匹配）。
            </div>
            <div v-else class="path-tags">
              <el-tag
                v-for="(p, i) in form.include_paths"
                :key="p + i"
                closable
                @close="form.include_paths.splice(i, 1)"
                style="margin: 2px 4px 2px 0"
              >{{ p }}</el-tag>
            </div>
            <div class="path-actions">
              <el-input
                v-model="pathInput"
                size="small"
                placeholder="手动输入路径，如 /剧集"
                style="width: 220px; margin-right: 8px"
                @keyup.enter="addPath"
              >
                <template #append>
                  <el-button :icon="Plus" @click="addPath" />
                </template>
              </el-input>
              <el-button
                size="small"
                :icon="FolderOpened"
                :loading="loadingTree"
                :disabled="!form.share_url"
                @click="openTreePicker"
              >从分享浏览</el-button>
            </div>
          </div>
        </el-form-item>
        <el-form-item label="排除规则">
          <div class="path-editor">
            <div class="path-tags">
              <el-tag
                v-for="(p, i) in form.exclude_patterns"
                :key="p + i"
                closable
                type="info"
                @close="form.exclude_patterns.splice(i, 1)"
                style="margin: 2px 4px 2px 0"
              >{{ p }}</el-tag>
              <span v-if="form.exclude_patterns.length === 0" class="path-empty-inline">
                支持 glob：*.tmp、sample.*、*广告* 等
              </span>
            </div>
            <el-input
              v-model="excludeInput"
              size="small"
              placeholder="添加排除规则，按回车确认"
              style="width: 280px; margin-top: 4px"
              @keyup.enter="addExclude"
            />
          </div>
        </el-form-item>
        <el-form-item label="冲突策略">
          <el-radio-group v-model="form.conflict_strategy">
            <el-radio-button value="overwrite">覆盖式</el-radio-button>
            <el-radio-button value="versioned">新版本式</el-radio-button>
            <el-radio-button value="skip">跳过</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="目标">
          <div v-for="(t, i) in form.targets" :key="i" class="target-form-row">
            <el-select v-model="t.kind" style="width: 110px" @change="onTargetKindChange(t)">
              <el-option value="netdisk" label="网盘" />
              <el-option value="local" label="本地" />
            </el-select>
            <el-input
              v-if="t.kind === 'netdisk'"
              v-model="(t as NetdiskTarget).remote_path"
              placeholder="网盘路径，如 /我的资源/同步"
              style="margin-left: 8px; flex: 1"
            />
            <el-input
              v-else
              v-model="(t as LocalTarget).local_path"
              placeholder="本地路径"
              style="margin-left: 8px; flex: 1"
            />
            <el-button :icon="Delete" link type="danger" @click="form.targets.splice(i, 1)" style="margin-left: 4px" />
          </div>
          <el-button :icon="Plus" link @click="form.targets.push(createDefaultTarget())">
            添加目标
          </el-button>
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

    <!-- 目录树选择对话框 -->
    <el-dialog
      v-model="treePickerVisible"
      title="从分享中选择要同步的子路径"
      width="640px"
      :close-on-click-modal="false"
      @open="loadTree"
    >
      <div class="tree-picker-toolbar">
        <el-input
          v-model="treeFilterText"
          placeholder="搜索路径/文件名"
          clearable
          size="small"
          style="width: 240px"
        />
        <el-checkbox v-model="treeCheckStrictly" style="margin-left: 12px">
          父子独立选择
        </el-checkbox>
        <el-radio-group v-model="treeDepth" size="small" style="margin-left: 12px">
          <el-radio-button :value="1">仅根</el-radio-button>
          <el-radio-button :value="2">2 层</el-radio-button>
          <el-radio-button :value="3">3 层</el-radio-button>
        </el-radio-group>
        <el-button size="small" :loading="loadingTree" :icon="Refresh" style="margin-left: 12px" @click="loadTree">刷新</el-button>
      </div>
      <el-alert
        v-if="treeError"
        :title="treeError"
        type="error"
        :closable="false"
        show-icon
        style="margin-bottom: 8px"
      />
      <el-tree
        ref="treeRef"
        :data="treeData"
        :props="treeProps"
        node-key="path"
        show-checkbox
        :check-strictly="treeCheckStrictly"
        :default-checked-keys="form.include_paths"
        :filter-node-method="filterTreeNode"
        :default-expand-all="false"
        v-loading="loadingTree"
        empty-text="暂无内容或分享已失效"
        style="max-height: 420px; overflow: auto"
      >
        <template #default="{ node, data }">
          <span class="tree-node">
            <el-icon v-if="data.is_dir"><FolderOpened /></el-icon>
            <el-icon v-else><Document /></el-icon>
            <span style="margin-left: 4px">{{ data.name }}</span>
            <span v-if="!data.is_dir" class="tree-size">{{ formatSize(data.size) }}</span>
          </span>
        </template>
      </el-tree>
      <template #footer>
        <span class="tree-picker-hint">已选 {{ form.include_paths.length }} 个路径</span>
        <el-button @click="treePickerVisible = false">取消</el-button>
        <el-button type="primary" @click="confirmTreePicker">确定</el-button>
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
        <h4 style="margin-top: 16px">
          文件动作（{{ currentRun.items.length }} / {{ currentRun.items_total ?? currentRun.items.length }}）
        </h4>
        <el-alert
          v-if="currentRun.items_truncated"
          title="仅展示前 200 条文件动作，避免详情页卡死。其余明细仍保留在后端。"
          type="warning"
          :closable="false"
          show-icon
          style="margin-bottom: 8px"
        />
        <el-table :data="currentRun.items" size="small" max-height="400">
          <el-table-column prop="path" label="路径" />
          <el-table-column prop="action" label="动作" width="80" />
          <el-table-column prop="target" label="目标" width="80" />
          <el-table-column prop="status" label="状态" width="100" />
          <el-table-column prop="reason" label="跳过原因" width="120" />
          <el-table-column prop="error" label="错误" />
        </el-table>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import { ElMessage, ElMessageBox, type ElTree } from 'element-plus'
import type { AxiosError } from 'axios'
import {
  Plus, Edit, Delete, Refresh, ArrowRight, Link,
  FolderOpened, Document, VideoPause, VideoPlay,
} from '@element-plus/icons-vue'
import {
  type ShareSubscription,
  type SyncTarget,
  type NetdiskTarget,
  type LocalTarget,
  type CreateShareSubscriptionRequest,
  type UpdateShareSubscriptionRequest,
  type RunRecord,
  type RunDetail,
  type ConflictStrategy,
  type PollConfig,
  type TreeNode,
  type ShareSyncWsEvent,
  listSubscriptions, createSubscription, updateSubscription,
  deleteSubscription, setSubscriptionEnabled, triggerSubscription, listRuns, getRun,
  previewTree,
} from '@/api/shareSync'
import { getWebSocketClient, connectWebSocket } from '@/utils/websocket'

const subscriptions = ref<ShareSubscription[]>([])
const DEFAULT_LOCAL_TARGET_PATH = '/home/hyx/codespace/one-family/data'
const selected = ref<ShareSubscription | null>(null)
const runs = ref<RunRecord[]>([])
const currentRun = ref<RunDetail | null>(null)
const dialogVisible = ref(false)
const dialogMode = ref<'create' | 'edit'>('create')
const runDialogVisible = ref(false)
const saving = ref(false)
const triggering = ref(false)
const formRef = ref()
const scheduledTime = ref<string>('03:00')

// 路径编辑
const pathInput = ref('')
const excludeInput = ref('')

// 目录树选择
const treePickerVisible = ref(false)
const treeData = ref<TreeNode[]>([])
const treeRef = ref<InstanceType<typeof ElTree>>()
const treeProps = {
  children: 'children',
  label: 'name',
  isLeaf: (d: TreeNode) => !d.is_dir,
} as const
const treeFilterText = ref('')
const treeCheckStrictly = ref(false)
const treeDepth = ref<number>(2)
const loadingTree = ref(false)
const treeError = ref('')
watch(treeFilterText, (v) => {
  treeRef.value?.filter(v)
})

function createDefaultTarget(): SyncTarget {
  return { kind: 'local', local_path: DEFAULT_LOCAL_TARGET_PATH, conflict_strategy: null }
}

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

function openCreate() {
  form.value = defaultForm()
  scheduledTime.value = '03:00'
  dialogMode.value = 'create'
  dialogVisible.value = true
}

function openEdit() {
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
  dialogMode.value = 'edit'
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
    if (dialogMode.value === 'create') {
      const req: CreateShareSubscriptionRequest = {
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
      await createSubscription(req)
      ElMessage.success('已创建订阅')
    } else if (selected.value) {
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

  for (let i = 0; i < form.value.targets.length; i++) {
    const t = form.value.targets[i] as NetdiskTarget | LocalTarget
    if (t.kind === 'netdisk') {
      if (!t.remote_path || !String(t.remote_path).trim()) {
        ElMessage.error(`目标 #${i + 1}：网盘路径不能为空`)
        return false
      }
    } else if (t.kind === 'local') {
      const lp = String(t.local_path || '').trim()
      if (!lp) {
        ElMessage.error(`目标 #${i + 1}：本地路径不能为空`)
        return false
      }
      if (!lp.startsWith('/')) {
        ElMessage.error(`目标 #${i + 1}：本地路径必须是绝对路径`)
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

function normalizeLocalPath(v: string): string {
  const s = v.trim().replace(/\/+/g, '/')
  if (!s) return ''
  const prefixed = s.startsWith('/') ? s : `/${s}`
  if (prefixed.length === 1) return '/'
  return prefixed.endsWith('/') ? prefixed.slice(0, -1) : prefixed
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
      const local = normalizeLocalPath(String(t.local_path || ''))
      nextTargets.push({
        kind: 'local',
        local_path: local,
        ...(t.conflict_strategy ? { conflict_strategy: t.conflict_strategy } : {}),
      })
    }
  }
  form.value.targets = nextTargets
  form.value.poll_config = normalizePollConfigForSubmit(form.value.poll_config)
}

async function removeSubscription() {
  if (!selected.value) return
  try {
    await ElMessageBox.confirm(
      `确定删除订阅 "${selected.value.name}"？历史快照与运行记录将一并清理。`,
      '删除确认',
      { type: 'warning' }
    )
  } catch {
    return
  }
  try {
    await deleteSubscription(selected.value.id)
    ElMessage.success('已删除')
    selected.value = null
    runs.value = []
    await refresh()
  } catch (e) {
    ElMessage.error(`删除失败: ${getApiErrorMessage(e)}`)
  }
}

async function toggleEnabled() {
  if (!selected.value) return
  try {
    await setSubscriptionEnabled(selected.value.id, !selected.value.enabled)
    ElMessage.success('已切换启用状态')
    await refresh()
  } catch (e) {
    ElMessage.error(`操作失败: ${getApiErrorMessage(e)}`)
  }
}

async function triggerNow() {
  if (!selected.value) return
  triggering.value = true
  try {
    await triggerSubscription(selected.value.id)
    ElMessage.success('已触发同步，结果将稍后出现在运行历史')
    setTimeout(() => selected.value && loadRuns(selected.value.id), 1500)
  } catch (e) {
    ElMessage.error(`触发失败: ${getApiErrorMessage(e)}`)
  } finally {
    triggering.value = false
  }
}

async function openRun(runId: string) {
  try {
    const r = await getRun(runId)
    currentRun.value = {
      ...r,
      items: r.items.slice(0, 200),
      items_total: r.items_total ?? r.items.length,
      items_truncated: (r.items_total ?? r.items.length) > 200 || !!r.items_truncated,
    }
    runDialogVisible.value = true
  } catch (e) {
    ElMessage.error(`加载运行详情失败: ${getApiErrorMessage(e)}`)
  }
}

function onTargetKindChange(t: SyncTarget) {
  if (t.kind === 'netdisk') {
    t.remote_path = normalizeRemotePath(String(t.remote_path || '/'))
    t.save_fs_id = Number.isFinite(Number(t.save_fs_id)) ? Number(t.save_fs_id) : 0
    delete (t as unknown as Record<string, unknown>).local_path
  }
  if (t.kind === 'local') {
    t.local_path = normalizeLocalPath(String(t.local_path || DEFAULT_LOCAL_TARGET_PATH))
    delete (t as unknown as Record<string, unknown>).save_fs_id
    delete (t as unknown as Record<string, unknown>).remote_path
  }
}

// ==================== 路径编辑 / 树选择 ====================

function addPath() {
  const v = normalizePath(pathInput.value)
  if (!v) return
  if (!form.value.include_paths.includes(v)) {
    form.value.include_paths.push(v)
  }
  pathInput.value = ''
}

function addExclude() {
  const v = excludeInput.value.trim()
  if (!v) return
  if (!form.value.exclude_patterns.includes(v)) {
    form.value.exclude_patterns.push(v)
  }
  excludeInput.value = ''
}

function openTreePicker() {
  if (!form.value.share_url) {
    ElMessage.warning('请先填写分享链接')
    return
  }
  treeError.value = ''
  treePickerVisible.value = true
}

async function loadTree() {
  loadingTree.value = true
  treeError.value = ''
  try {
    const resp = await previewTree(
      form.value.share_url,
      form.value.password || null,
      treeDepth.value
    )
    treeData.value = resp.root || []
    // 重新设置已选
    await nextTick()
    form.value.include_paths.forEach((p: string) => {
      treeRef.value?.setChecked?.(p, true, false)
    })
  } catch (e) {
    const ax = e as AxiosError<{ message?: string; error?: string }>
    treeError.value =
      ax?.response?.data?.message ||
      ax?.response?.data?.error ||
      (e as Error)?.message ||
      '加载目录树失败'
  } finally {
    loadingTree.value = false
  }
}

function confirmTreePicker() {
  const checked = treeRef.value?.getCheckedNodes?.(false, false) || []
  const halfChecked = treeRef.value?.getHalfCheckedNodes?.() || []
  const all = [...checked, ...halfChecked]
  // 按照树节点路径回填，先标准化再去重
  const nextInclude: string[] = []
  const uniq = new Set<string>()
  all
    .map((n) => (n as unknown as TreeNode).path)
    .map((p: string) => normalizePath(p))
    .filter((p: string) => p.length > 0)
    .forEach((p: string) => {
      if (!uniq.has(p)) {
        uniq.add(p)
        nextInclude.push(p)
      }
    })
  form.value.include_paths = nextInclude
  treePickerVisible.value = false
}

function filterTreeNode(query: string, data: TreeNode) {
  if (!query) return true
  return (data.name as string)?.toLowerCase?.().includes(query.toLowerCase())
}

function formatSize(n: number): string {
  if (!n) return ''
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let i = 0
  let v = n
  while (v >= 1024 && i < u.length - 1) {
    v /= 1024
    i++
  }
  return v.toFixed(v >= 100 || i === 0 ? 0 : 1) + ' ' + u[i]
}

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
    s === 'failed' ? '失败' : s
}

function runStatusType(s: string): 'success' | 'warning' | 'danger' | 'info' {
  return s === 'completed' ? 'success' :
    s === 'completed_with_errors' ? 'warning' :
    s === 'failed' ? 'danger' : 'info'
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

// ==================== 生命周期 ====================

let unsubWs: (() => void) | null = null

onMounted(async () => {
  await refresh()
  if (subscriptions.value.length > 0 && !selected.value) {
    await select(subscriptions.value[0])
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
    if (['subscription_created', 'subscription_updated', 'subscription_deleted', 'status_changed'].includes(evt.type)) {
      refresh()
    } else if (sid === selected.value?.id) {
      if (['run_started', 'run_completed', 'run_failed', 'diff_detected'].includes(evt.type)) {
        loadRuns(sid)
        if (['run_completed', 'run_failed'].includes(evt.type)) {
          ElMessage[evt.type === 'run_failed' ? 'error' : 'success'](describeRunStatus(evt.type))
        }
      }
    }
  }
  unsubWs = ws.onShareSyncEvent(handler)
})

onUnmounted(() => {
  unsubWs?.()
})
</script>

<style scoped lang="scss">
.share-sync-view {
  padding: 16px;
  .page-header { margin-bottom: 12px; }
  .page-title { font-weight: 600; font-size: 18px; margin-left: 8px; }
  .page-desc { color: #909399; font-size: 13px; margin-bottom: 16px; }
}

.list-card .card-header,
.detail-card .card-header,
.runs-card .card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.sub-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.sub-item {
  padding: 10px 12px;
  border: 1px solid #ebeef5;
  border-radius: 6px;
  cursor: pointer;
  transition: all 0.2s;
  &:hover { border-color: #409eff; }
  &.active { background: #ecf5ff; border-color: #409eff; }
  .sub-name { display: flex; align-items: center; gap: 6px; font-weight: 500; }
  .sub-meta { font-size: 12px; color: #909399; margin-top: 4px; display: flex; gap: 6px; }
}

.target-line { margin: 4px 0; }
.target-form-row { display: flex; align-items: center; margin-bottom: 8px; }

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
