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
                  :icon="selected.enabled ? 'VideoPause' : 'VideoPlay'"
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
            <el-descriptions-item label="轮询">
              <span v-if="selected.poll_config.enabled">
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
                  +{{ r.added_count }} ~{{ r.modified_count }} −{{ r.removed_count }}
                  <span v-if="r.failed_count > 0" style="color: #f56c6c"> ✗{{ r.failed_count }}</span>
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
              v-model="(t as any).remote_path"
              placeholder="网盘路径，如 /我的资源/同步"
              style="margin-left: 8px; flex: 1"
            />
            <el-input
              v-else
              v-model="(t as any).local_path"
              placeholder="本地路径"
              style="margin-left: 8px; flex: 1"
            />
            <el-button :icon="Delete" link type="danger" @click="form.targets.splice(i, 1)" style="margin-left: 4px" />
          </div>
          <el-button :icon="Plus" link @click="form.targets.push({ kind: 'netdisk', remote_path: '/', save_fs_id: 0 } as any)">
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
          <el-descriptions-item label="错误" v-if="currentRun.error">
            <span style="color: #f56c6c">{{ currentRun.error }}</span>
          </el-descriptions-item>
        </el-descriptions>
        <h4 style="margin-top: 16px">文件动作（{{ currentRun.items.length }}）</h4>
        <el-table :data="currentRun.items" size="small" max-height="400">
          <el-table-column prop="path" label="路径" />
          <el-table-column prop="action" label="动作" width="80" />
          <el-table-column prop="target" label="目标" width="80" />
          <el-table-column prop="status" label="状态" width="100" />
          <el-table-column prop="error" label="错误" />
        </el-table>
      </div>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted, computed } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Plus, Edit, Delete, Refresh, ArrowRight, Link,
} from '@element-plus/icons-vue'
import {
  type ShareSubscription,
  type SyncTarget,
  type RunRecord,
  type RunDetail,
  type ConflictStrategy,
  type PollConfig,
  listSubscriptions, getSubscription, createSubscription, updateSubscription,
  deleteSubscription, setSubscriptionEnabled, triggerSubscription, listRuns, getRun,
} from '@/api/shareSync'
import { getWebSocketClient, connectWebSocket } from '@/utils/websocket'

const subscriptions = ref<ShareSubscription[]>([])
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

const defaultForm = () => ({
  name: '',
  share_url: '',
  password: '',
  include_paths: [] as string[],
  exclude_patterns: [] as string[],
  targets: [{ kind: 'netdisk', remote_path: '/', save_fs_id: 0 }] as any[],
  conflict_strategy: 'overwrite' as ConflictStrategy,
  delete_missing: false,
  poll_config: { enabled: true, mode: 'interval', interval_secs: 1800, schedule_hour: null, schedule_minute: null } as PollConfig,
})

const form = ref(defaultForm())

const formRules = {
  name: [{ required: true, message: '请输入名称', trigger: 'blur' }],
  share_url: [
    { required: true, message: '请输入分享链接', trigger: 'blur' },
    { pattern: /pan\.baidu\.com/, message: '必须是 pan.baidu.com 链接', trigger: 'blur' },
  ],
}

// ==================== 数据加载 ====================

async function refresh() {
  subscriptions.value = await listSubscriptions()
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
    console.error('load runs failed', e)
  }
}

function openCreate() {
  form.value = defaultForm()
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
    targets: selected.value.targets.map(t => ({ ...t })) as any,
    conflict_strategy: selected.value.conflict_strategy,
    delete_missing: selected.value.delete_missing,
    poll_config: { ...selected.value.poll_config },
  }
  if (selected.value.poll_config.mode === 'scheduled') {
    const h = String(selected.value.poll_config.schedule_hour || 0).padStart(2, '0')
    const m = String(selected.value.poll_config.schedule_minute || 0).padStart(2, '0')
    scheduledTime.value = `${h}:${m}`
  }
  dialogMode.value = 'edit'
  dialogVisible.value = true
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
  saving.value = true
  try {
    if (dialogMode.value === 'create') {
      const req: any = {
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
      const req: any = {
        name: form.value.name,
        share_url: form.value.share_url,
        password: form.value.password || null,
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
  } catch (e: any) {
    ElMessage.error(`保存失败: ${e?.response?.data?.error || e.message}`)
  } finally {
    saving.value = false
  }
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
  await deleteSubscription(selected.value.id)
  ElMessage.success('已删除')
  selected.value = null
  runs.value = []
  await refresh()
}

async function toggleEnabled() {
  if (!selected.value) return
  await setSubscriptionEnabled(selected.value.id, !selected.value.enabled)
  ElMessage.success('已切换启用状态')
  await refresh()
}

async function triggerNow() {
  if (!selected.value) return
  triggering.value = true
  try {
    await triggerSubscription(selected.value.id)
    ElMessage.success('已触发同步，结果将稍后出现在运行历史')
    setTimeout(() => selected.value && loadRuns(selected.value.id), 1500)
  } catch (e: any) {
    ElMessage.error(`触发失败: ${e?.response?.data?.error || e.message}`)
  } finally {
    triggering.value = false
  }
}

async function openRun(runId: string) {
  try {
    currentRun.value = await getRun(runId)
    runDialogVisible.value = true
  } catch (e: any) {
    ElMessage.error(`加载运行详情失败: ${e?.message}`)
  }
}

function onTargetKindChange(t: any) {
  if (t.kind === 'netdisk' && !t.remote_path) t.remote_path = '/'
  if (t.kind === 'local' && !t.local_path) t.local_path = ''
}

function onScheduledChange(val: string | null) {
  if (!val) return
  const [h, m] = val.split(':').map(Number)
  form.value.poll_config.schedule_hour = h
  form.value.poll_config.schedule_minute = m
}

// ==================== 辅助显示 ====================

function describeInterval(p: PollConfig): string {
  if (!p.enabled) return '已禁用'
  if (p.mode === 'interval') {
    const m = Math.floor(p.interval_secs / 60)
    return `每 ${m} 分钟`
  }
  if (p.mode === 'scheduled') {
    const h = String(p.schedule_hour || 0).padStart(2, '0')
    const m = String(p.schedule_minute || 0).padStart(2, '0')
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
  const handler = (event: any) => {
    const evt = event?.detail ?? event
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
</style>
