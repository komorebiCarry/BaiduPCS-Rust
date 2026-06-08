<script setup lang="ts">
/**
 * BudgetPanel — 多账号资源配额面板
 *
 * 设置页资源配额面板：
 *   1. 机器总上限（下载 / 上传，auto / manual 切换 + 数字输入）
 *   2. VIP 推荐配置表（3 等级 × 4 字段）—— 用户可改，超代码值黄色警告（不拦截）
 *   3. 每账号实际配额（三段进度条 used / base / vip_cap）
 *
 * 状态来源：
 *   - 首屏：`useBudgetStore().fetchSnapshot()` → `GET /api/v1/budget`
 *   - 实时：在 onMounted 显式订阅 `budget:*` WS topic，事件透传到 store
 *
 * 写入：
 *   - 机器总上限 → `PUT /api/v1/config/multi_account_budget`
 *   - VIP 推荐    → `PUT /api/v1/config/vip_recommended`
 *   - 单账号配置 → `PUT /api/v1/accounts/:uid/custom_config`
 *
 * Readonly 模式：所有 PUT 按钮 disabled + 黄色提示
 */

import { computed, onMounted, onUnmounted, reactive, ref, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Refresh, InfoFilled, WarningFilled, RefreshLeft } from '@element-plus/icons-vue'
import { useBudgetStore } from '@/stores/budget'
import { useAuthStore } from '@/stores/auth'
import {
  updateMultiAccountBudget,
  updateVipRecommended,
  updateAccountCustomConfig,
  type MultiAccountBudgetConfig,
  type VipRecommendedConfig,
  type AccountCustomConfig,
  type AccountBudgetView,
} from '@/api/budget'
import { getConfig } from '@/api/config'
import { getWebSocketClient } from '@/utils/websocket'
import type { AccountSummary } from '@/api/accounts'
import type { BudgetEvent } from '@/types/events'

const budgetStore = useBudgetStore()
const authStore = useAuthStore()

// ===========================================================================
// VIP 等级常量（与后端 VipType / recommended_for_vip 对齐）
// ===========================================================================

/** VIP 等级 → 显示名 */
function vipLevelName(vipType?: number | null): string {
  switch (vipType) {
    case 2: return '超级会员（SVIP）'
    case 1: return '普通会员（VIP）'
    default: return '普通用户'
  }
}

function vipLevelTagType(vipType?: number | null): 'success' | 'warning' | 'info' {
  switch (vipType) {
    case 2: return 'warning' // SVIP — 金色
    case 1: return 'success'
    default: return 'info'
  }
}

/** VIP 等级对应的推荐线程数 key（VipType: 0=Normal, 1=VIP, 2=SVIP） */
type VipKey = 'normal' | 'vip' | 'svip'
function vipKey(vipType?: number | null): VipKey {
  if (vipType === 2) return 'svip'
  if (vipType === 1) return 'vip'
  return 'normal'
}

// ===========================================================================
// Section 1：机器总上限
// ===========================================================================

interface MachineForm {
  download_mode: 'auto' | 'manual'
  download_value: number
  upload_mode: 'auto' | 'manual'
  upload_value: number
  weight_normal: number
  weight_vip: number
  weight_svip: number
}

const machineForm = reactive<MachineForm>({
  download_mode: 'auto',
  download_value: 32,
  upload_mode: 'auto',
  upload_value: 32,
  weight_normal: 1,
  weight_vip: 3,
  weight_svip: 5,
})

const machineSaving = ref(false)

/** 当前生效的机器总上限（来自快照），用于 "auto" 模式下的展示提示 */
const effectiveMachineDownload = computed(() => budgetStore.machineBudgetDownload)
const effectiveMachineUpload = computed(() => budgetStore.machineBudgetUpload)

async function saveMachineConfig() {
  if (authStore.readonlyMode) {
    ElMessage.warning('当前处于只读模式，不能修改机器配额')
    return
  }
  machineSaving.value = true
  try {
    const payload: MultiAccountBudgetConfig = {
      download_machine_budget:
          machineForm.download_mode === 'auto' ? 'auto' : machineForm.download_value,
      upload_machine_budget:
          machineForm.upload_mode === 'auto' ? 'auto' : machineForm.upload_value,
      weight_normal: machineForm.weight_normal,
      weight_vip: machineForm.weight_vip,
      weight_svip: machineForm.weight_svip,
    }
    await updateMultiAccountBudget(payload)
    ElMessage.success('机器配额已保存')
    // 后端 recompute_budget() 会推送 BudgetRecomputed；此处再主动拉取一次保险
    await budgetStore.fetchSnapshot()
  } catch (e) {
    handleApiError(e, '保存机器配额失败')
  } finally {
    machineSaving.value = false
  }
}

// ===========================================================================
// Section 2：VIP 推荐配置表
// ===========================================================================

/** 代码默认推荐值（与后端 `recommended_for_vip()` 对齐，用于"超过推荐"黄色警告基线） */
const VIP_RECOMMENDED_DEFAULTS: VipRecommendedConfig = {
  normal: { threads: 1, chunk_size_mb: 4, max_concurrent_tasks: 1 },
  vip: { threads: 10, chunk_size_mb: 4, max_concurrent_tasks: 3 },
  svip: { threads: 15, chunk_size_mb: 5, max_concurrent_tasks: 5 },
}

const vipForm = reactive<VipRecommendedConfig>({
  normal: { ...VIP_RECOMMENDED_DEFAULTS.normal },
  vip: { ...VIP_RECOMMENDED_DEFAULTS.vip },
  svip: { ...VIP_RECOMMENDED_DEFAULTS.svip },
})

const vipSaving = ref(false)

const vipLevels: Array<{ key: VipKey; label: string }> = [
  { key: 'normal', label: '普通用户' },
  { key: 'vip', label: '普通会员' },
  { key: 'svip', label: '超级会员' },
]

/**
 * 是否超过代码默认推荐值 — 黄色警告（不拦截）。
 *
 * 仅对 `threads` 字段做对比（「超过 VIP 等级上限，最大可用 ${vip_cap}」）。
 */
function vipThreadsOverDefault(level: VipKey): boolean {
  return vipForm[level].threads > VIP_RECOMMENDED_DEFAULTS[level].threads
}

function resetVipDefaults() {
  vipForm.normal = { ...VIP_RECOMMENDED_DEFAULTS.normal }
  vipForm.vip = { ...VIP_RECOMMENDED_DEFAULTS.vip }
  vipForm.svip = { ...VIP_RECOMMENDED_DEFAULTS.svip }
  ElMessage.info('已恢复默认值（未保存，请点击"保存"提交）')
}

async function saveVipRecommended() {
  if (authStore.readonlyMode) {
    ElMessage.warning('当前处于只读模式，不能修改 VIP 推荐配置')
    return
  }
  vipSaving.value = true
  try {
    await updateVipRecommended({
      normal: { ...vipForm.normal },
      vip: { ...vipForm.vip },
      svip: { ...vipForm.svip },
    })
    ElMessage.success('VIP 推荐配置已保存')
    await budgetStore.fetchSnapshot()
  } catch (e) {
    handleApiError(e, '保存 VIP 推荐配置失败')
  } finally {
    vipSaving.value = false
  }
}

/**
 * 从后端拉取真实生效的「机器总上限 + VIP 推荐表」并填充表单。
 *
 * 之前 `machineForm` / `vipForm` 仅用代码内置默认值（普通会员10/超级会员15）初始化，
 * 而后端真实生效值来自 `app.toml [multi_account_budget]` /
 * `[multi_account_vip_recommended]`，二者可能不一致 → UI 显示与实际生效脱节。
 * 这里在首屏用 `GET /config` 把真实值灌进表单，保证「看到的=生效的」。
 */
async function loadConfigForms() {
  try {
    const cfg = await getConfig()
    const mb = cfg.multi_account_budget
    if (mb) {
      machineForm.download_mode = mb.download_machine_budget === 'auto' ? 'auto' : 'manual'
      if (typeof mb.download_machine_budget === 'number') {
        machineForm.download_value = mb.download_machine_budget
      }
      machineForm.upload_mode = mb.upload_machine_budget === 'auto' ? 'auto' : 'manual'
      if (typeof mb.upload_machine_budget === 'number') {
        machineForm.upload_value = mb.upload_machine_budget
      }
      machineForm.weight_normal = mb.weight_normal
      machineForm.weight_vip = mb.weight_vip
      machineForm.weight_svip = mb.weight_svip
    }
    const vr = cfg.multi_account_vip_recommended
    if (vr) {
      vipForm.normal = { ...vr.normal }
      vipForm.vip = { ...vr.vip }
      vipForm.svip = { ...vr.svip }
    }
  } catch {
    // 拉取失败时维持代码内置默认值（不阻塞面板其它功能）
  }
}

// ===========================================================================
// Section 3：每账号实际配额
// ===========================================================================

/**
 * 单账号 UI 模型 —— 合并 authStore.accounts + budgetStore.accountViews。
 *
 * 自定义模式表单状态独立 per-uid，避免编辑过程中 store 推送 UsageSnapshot 触发抖动。
 */
interface AccountUi {
  account: AccountSummary
  view: {
    base_download: number
    vip_cap_download: number
    used_download: number
    base_upload: number
    vip_cap_upload: number
    used_upload: number
  }
}

const accountUis = computed<AccountUi[]>(() => {
  const viewMap = new Map<number, AccountBudgetView>()
  for (const v of budgetStore.accountViews) {
    viewMap.set(v.uid, v)
  }
  return authStore.accounts.map((acc) => {
    const v = viewMap.get(acc.uid)
    return {
      account: acc,
      view: {
        base_download: v?.base_download ?? 0,
        vip_cap_download: v?.vip_cap_download ?? 0,
        used_download: v?.used_download ?? 0,
        base_upload: v?.base_upload ?? 0,
        vip_cap_upload: v?.vip_cap_upload ?? 0,
        used_upload: v?.used_upload ?? 0,
      },
    }
  })
})

/** 单账号自定义表单（key: uid） */
interface AccountFormState {
  auto_apply_recommended: boolean
  download: {
    max_global_threads: number
    chunk_size_mb: number
    max_concurrent_tasks: number
    max_retries: number
  }
  upload: {
    max_global_threads: number
    chunk_size_mb: number
    max_concurrent_tasks: number
    max_retries: number
    skip_hidden_files: boolean
  }
  saving: boolean
  /** 用户编辑过未保存的脏表单标记 */
  dirty: boolean
  /** 上次同步的 custom_config 序列化指纹，用于精确判断"该 uid 配置真的变了" */
  lastSyncedFingerprint: string
}

const accountForms = reactive<Record<number, AccountFormState>>({})

/** 初始化单账号表单 — 优先使用持久化的 custom_config，缺失则按 VIP 推荐值兜底
 *
 * 表单重建逻辑 (以账号实际配置为准)：旧账号
 * `auto_apply_recommended=false` + 自定义 threads/concurrent/retries/skip_hidden_files
 * 在用户点保存时会被覆盖。修复：后端 `AccountSummary` 现在携带持久化的
 * `custom_config`，前端优先采用，未带则维持原 VIP 推荐兜底（兼容旧后端）。
 */
function initAccountForm(account: AccountSummary): AccountFormState {
  const k = vipKey(account.vip_type)
  const rec = vipForm[k]
  const cc = account.custom_config
  if (cc) {
    return {
      auto_apply_recommended: cc.auto_apply_recommended,
      download: {
        max_global_threads: cc.download.max_global_threads,
        // chunk_size_mb 为可选 — 缺省时回退到 VIP 推荐值（仅展示用）
        chunk_size_mb: cc.download.chunk_size_mb ?? rec.chunk_size_mb,
        max_concurrent_tasks: cc.download.max_concurrent_tasks,
        max_retries: cc.download.max_retries,
      },
      upload: {
        max_global_threads: cc.upload.max_global_threads,
        chunk_size_mb: cc.upload.chunk_size_mb ?? rec.chunk_size_mb,
        max_concurrent_tasks: cc.upload.max_concurrent_tasks,
        max_retries: cc.upload.max_retries,
        skip_hidden_files: cc.upload.skip_hidden_files,
      },
      saving: false,
      dirty: false,
      lastSyncedFingerprint: JSON.stringify(cc),
    }
  }
  // 旧后端兼容路径（无 custom_config）：按 VIP 推荐填充
  return {
    auto_apply_recommended: true,
    download: {
      max_global_threads: rec.threads,
      chunk_size_mb: rec.chunk_size_mb,
      max_concurrent_tasks: rec.max_concurrent_tasks,
      max_retries: 3,
    },
    upload: {
      max_global_threads: rec.threads,
      chunk_size_mb: rec.chunk_size_mb,
      max_concurrent_tasks: rec.max_concurrent_tasks,
      max_retries: 3,
      skip_hidden_files: false,
    },
    saving: false,
    dirty: false,
    lastSyncedFingerprint: JSON.stringify(null),
  }
}

/**
 * 当账号列表或某个账号的 `custom_config` 变化时，刷新对应 form。
 *
 * 表单同步策略（watch 触发源 + 干净表单保护）：
 *
 * watch 触发源是"所有账号 uid + 所有账号 custom_config
 * 整体指纹"，只要任一账号变更（如新增/删除）就会对所有 form 重建。本轮发现
 * 这种粗粒度刷新会**覆盖未保存的编辑**：用户在账号 A 表单上改了线程数没保存，
 * 此时账号 B 被新增/删除 → A 的指纹其实没变，但 watch 触发后 A 的 form
 * 被 `initAccountForm(acc)` 整体重建，用户编辑丢失。
 *
 * 增量同步逻辑：
 * - watcher 仍触发于"列表整体序列化"，但内部按 **per-uid 指纹** 精确判断
 * - 每个 form 维护 `lastSyncedFingerprint`，仅当当前账号的 custom_config
 *   指纹与上次同步值不同时才重建
 * - 引入 `dirty` 标记：用户改过未保存（非空 dirty）时即使指纹变了也不覆盖，
 *   只 warn 提示一次（避免静默覆盖编辑）
 * - `saving=true` 仍然跳过（避免打断保存）
 *
 * 触发源依旧是聚合指纹（用一个表达式即可触发整个循环），但写入端按 uid
 * 粒度比较 lastSyncedFingerprint。
 */
watch(
    () =>
        authStore.accounts.map((a) => `${a.uid}:${JSON.stringify(a.custom_config ?? null)}`).join('|'),
    () => {
      for (const acc of authStore.accounts) {
        const existing = accountForms[acc.uid]
        const fp = JSON.stringify(acc.custom_config ?? null)
        if (!existing) {
          // 新增 uid → 直接初始化
          accountForms[acc.uid] = initAccountForm(acc)
          continue
        }
        if (existing.saving) {
          // 当前正在保存 → 不打断，等保存完毕（保存后 fetchAccountList 会再次触发本 watch）
          continue
        }
        if (existing.lastSyncedFingerprint === fp) {
          // 该 uid 自身指纹未变（其它 uid 触发的本次 watch）→ 不动表单
          continue
        }
        if (existing.dirty) {
          // 用户改过未保存 → 不覆盖，记录警告（保留用户编辑值）
          // 同时更新 lastSyncedFingerprint 防止反复 warn
          // 注：不更新表单内容，但允许下次保存覆盖；如需"强提示"可改为 ElMessageBox.confirm
          // eslint-disable-next-line no-console
          console.warn(
            `账号 ${acc.uid} 自定义配置在其它端被修改，但本端表单有未保存编辑，跳过自动同步`,
          )
          existing.lastSyncedFingerprint = fp
          continue
        }
        // 同 uid + 非脏 + 指纹变化 → 重建表单（与最新持久化值同步）
        const refreshed = initAccountForm(acc)
        existing.auto_apply_recommended = refreshed.auto_apply_recommended
        existing.download = refreshed.download
        existing.upload = refreshed.upload
        existing.lastSyncedFingerprint = fp
        existing.dirty = false
      }
    },
    { immediate: true },
)

/**
 * 监听每个 form 的实际编辑动作 → 自动设置 dirty=true
 *
 * 用一个 deep watch 监视整个 `accountForms` reactive object，把当前每个 form 的
 * 编辑相关字段（不含 saving / dirty / lastSyncedFingerprint 三个 meta）序列化
 * 后与 `lastSyncedFingerprint` 比较；不一致即标记 dirty。
 *
 * 这样使用 Element Plus `el-input-number` / `el-switch` / `el-select` 等组件
 * 时不需要逐个绑定 `@change`，dirty 检测自动维护。
 */
function formContentFingerprint(f: AccountFormState): string {
  return JSON.stringify({
    auto_apply_recommended: f.auto_apply_recommended,
    download: f.download,
    upload: f.upload,
  })
}
watch(
    accountForms,
    (cur) => {
      for (const uidStr of Object.keys(cur)) {
        const uid = Number(uidStr)
        const f = cur[uid]
        if (!f || f.saving) continue
        const cur_fp = formContentFingerprint(f)
        if (cur_fp !== f.lastSyncedFingerprint && !f.dirty) {
          f.dirty = true
        }
      }
    },
    { deep: true },
)

/**
 * 检查账号自定义 threads 是否超过其 VIP 硬上限（vip_cap_download / vip_cap_upload）。
 *
 * 「自定义 threads 超过 vip_cap → 前端即时校验拦截 + 红字提示」。
 */
function downloadOverCap(uid: number): boolean {
  const form = accountForms[uid]
  if (!form || form.auto_apply_recommended) return false
  const view = accountUis.value.find((u) => u.account.uid === uid)?.view
  if (!view || view.vip_cap_download === 0) return false
  return form.download.max_global_threads > view.vip_cap_download
}

function uploadOverCap(uid: number): boolean {
  const form = accountForms[uid]
  if (!form || form.auto_apply_recommended) return false
  const view = accountUis.value.find((u) => u.account.uid === uid)?.view
  if (!view || view.vip_cap_upload === 0) return false
  return form.upload.max_global_threads > view.vip_cap_upload
}

async function saveAccountCustomConfig(uid: number) {
  if (authStore.readonlyMode) {
    ElMessage.warning('当前处于只读模式，不能修改账号配置')
    return
  }
  const form = accountForms[uid]
  if (!form) return
  // 自定义模式硬校验
  if (!form.auto_apply_recommended) {
    if (downloadOverCap(uid) || uploadOverCap(uid)) {
      ElMessage.error('自定义线程数超过该账号 VIP 硬上限，请先调整')
      return
    }
  }
  form.saving = true
  try {
    // `chunk_size_mb` 已在 UI 层标为只读说明（运行时由
    // 文件大小 + VIP 等级自适应计算，不由账号配置驱动）。这里把它从保存
    // payload 中剥离，避免持续往后端发送一个"持久化但不生效"的字段。
    const { chunk_size_mb: _dlCs, ...dlOut } = form.download
    const { chunk_size_mb: _upCs, ...upOut } = form.upload
    void _dlCs
    void _upCs
    const payload: AccountCustomConfig = {
      auto_apply_recommended: form.auto_apply_recommended,
      download: dlOut,
      upload: upOut,
    }
    await updateAccountCustomConfig(uid, payload)
    ElMessage.success('账号配置已保存')
    // 保存成功 → 表单不再脏，更新 lastSyncedFingerprint
    // 防止后续 fetchAccountList 拿回服务端归一化后的 custom_config 又把 form
    // 重建（值相同，只是规范化）；保存后 form 的 fingerprint 必须严格等于
    // 服务端将要返回的 custom_config 的 fingerprint。
    form.dirty = false
    form.lastSyncedFingerprint = JSON.stringify({
      auto_apply_recommended: form.auto_apply_recommended,
      download: form.download,
      upload: form.upload,
    })
    await budgetStore.fetchSnapshot()
    // 🔥 保存成功后刷新账号列表：
    //
    // 保存成功后刷新账号列表，让 AccountSummary.custom_config
    //   与最新持久化值同步。
    //
    // 先把 `form.saving=false` 再 `fetchAccountList()`，
    //   否则 fetchAccountList 触发的 watcher 看到 `existing.saving=true` 直接
    //   `continue`，跳过本 uid 的同步 → 表单和 lastSyncedFingerprint 短暂停在
    //   乐观值，如果服务端做了字段归一化（默认值补齐 / 字段省略 / 边界值
    //   裁剪），后续保存会用错的 fingerprint 推导 dirty。
    //
    //   把 saving=false 放在 fetchAccountList 之前；保存路径正常退出时不再
    //   依赖 finally 的 saving 重置，finally 仅作为异常路径兜底。
    form.saving = false
    try {
      await authStore.fetchAccountList()
      // fetch 后显式对当前 uid 执行一次同步（不依赖 watcher 重触发）：
      // 如果服务端归一化过 custom_config，立刻拿最新值同步表单 +
      // lastSyncedFingerprint。
      const refreshedAcc = authStore.accounts.find((a) => a.uid === uid)
      if (refreshedAcc) {
        const fp = JSON.stringify(refreshedAcc.custom_config ?? null)
        if (fp !== form.lastSyncedFingerprint) {
          const refreshed = initAccountForm(refreshedAcc)
          form.auto_apply_recommended = refreshed.auto_apply_recommended
          form.download = refreshed.download
          form.upload = refreshed.upload
          form.lastSyncedFingerprint = fp
          form.dirty = false
        }
      }
    } catch {
      // 静默：保存成功是主诉求，列表刷新失败不影响本次操作
    }
  } catch (e) {
    handleApiError(e, '保存账号配置失败')
  } finally {
    // 异常路径兜底；正常路径已在 try block 末尾把 saving 设为 false
    form.saving = false
  }
}

// ===========================================================================
// 三段进度条计算
// ===========================================================================

/**
 * 三段进度条（用于 used / base / vip_cap 的可视化）。
 *
 * 返回宽度百分比：
 *   - usedPct：used / vip_cap × 100（最低 0，最高 100）
 *   - basePct：base / vip_cap × 100（用于在底色上画一条 base 标记线）
 *   - status："empty" | "below_base" | "above_base" | "full"
 */
function progressMeta(used: number, base: number, vipCap: number) {
  if (vipCap <= 0) {
    return { usedPct: 0, basePct: 0, status: 'empty' as const }
  }
  const usedPct = Math.min(100, Math.max(0, (used / vipCap) * 100))
  const basePct = Math.min(100, Math.max(0, (base / vipCap) * 100))
  let status: 'empty' | 'below_base' | 'above_base' | 'full' = 'below_base'
  if (used <= 0) status = 'empty'
  else if (used >= vipCap) status = 'full'
  else if (used > base) status = 'above_base'
  return { usedPct, basePct, status }
}

// ===========================================================================
// 错误处理
// ===========================================================================

function handleApiError(e: unknown, fallbackMsg: string) {
  const err = e as {
    message?: string
    response?: {
      status?: number
      data?: { message?: string; code?: string; error?: string }
    }
  }
  const status = err?.response?.status
  const data = err?.response?.data
  if (status === 503) {
    ElMessage.error('系统当前处于只读模式，无法保存')
    return
  }
  if (status === 400 && data?.error === 'account_not_available') {
    ElMessage.error('该账号已不存在，请刷新账号列表')
    return
  }
  ElMessage.error(data?.message || err?.message || fallbackMsg)
}

// ===========================================================================
// 生命周期：WS 订阅 + 首屏拉取
// ===========================================================================

let unsubscribeBudget: (() => void) | null = null

async function refreshSnapshot() {
  try {
    await budgetStore.fetchSnapshot()
  } catch {
    // 错误信息已经记录在 budgetStore.lastError，UI 中展示
  }
}

onMounted(async () => {
  // 1. 订阅 budget WS topic
  const wsClient = getWebSocketClient()
  wsClient.subscribe(['budget:*'])
  unsubscribeBudget = wsClient.onBudgetEvent((event: BudgetEvent) => {
    budgetStore.applyBudgetEvent(event)
  })

  // 2. 先确保账号列表加载（用于渲染每账号 section）
  if (!authStore.accountsLoaded) {
    try {
      await authStore.fetchAccountList()
    } catch {
      // 静默；BudgetPanel 仍可显示全局部分
    }
  }

  // 3. 拉取一次 snapshot
  await refreshSnapshot()

  // 4. 拉取真实生效的机器总上限 + VIP 推荐表，覆盖表单内置默认值
  await loadConfigForms()
})

onUnmounted(() => {
  if (unsubscribeBudget) {
    unsubscribeBudget()
    unsubscribeBudget = null
  }
  const wsClient = getWebSocketClient()
  wsClient.unsubscribe?.(['budget:*'])
})

// 公开给 SettingsView 的强制刷新（如果以后用得上）
defineExpose({ refresh: refreshSnapshot })
</script>

<template>
  <div class="budget-panel">
    <!-- 标题 + 全局只读提示 -->
    <div class="panel-header">
      <h3>多账号资源配额</h3>
      <el-button :icon="Refresh" :loading="budgetStore.loading" @click="refreshSnapshot">
        刷新
      </el-button>
    </div>

    <el-alert
        v-if="authStore.readonlyMode"
        type="warning"
        :closable="false"
        show-icon
        title="服务端已进入只读保护模式"
        description="服务端检测到数据异常，已进入只读保护模式以避免数据损坏。请先重启后端再次尝试自动修复；若重启后仍未恢复，请还原升级前备份的 config/ 与 wal/ 目录后重启；系统也会在升级迁移前自动备份到 config/backups/pre_migration_<时间戳>/。"
        class="readonly-alert"
    />

    <!-- ========================= Section 1：机器总上限 ========================= -->
    <el-card class="setting-card" shadow="never">
      <template #header>
        <div class="card-header">
          <span>机器总上限</span>
          <el-tooltip
              effect="dark"
              placement="top"
              content="所有账号下载/上传线程总和的硬上限。auto 模式 = cpu_cores × 8。"
          >
            <el-icon class="info-icon"><InfoFilled /></el-icon>
          </el-tooltip>
        </div>
      </template>

      <el-form label-position="left" label-width="120px" :disabled="authStore.readonlyMode">
        <el-form-item label="下载总线程数">
          <el-radio-group v-model="machineForm.download_mode">
            <el-radio value="auto">
              自动 (当前生效: {{ effectiveMachineDownload }})
            </el-radio>
            <el-radio value="manual">手动</el-radio>
          </el-radio-group>
          <el-input-number
              v-if="machineForm.download_mode === 'manual'"
              v-model="machineForm.download_value"
              :min="1"
              :max="9999"
              size="small"
              class="value-input"
          />
        </el-form-item>

        <el-form-item label="上传总线程数">
          <el-radio-group v-model="machineForm.upload_mode">
            <el-radio value="auto">
              自动 (当前生效: {{ effectiveMachineUpload }})
            </el-radio>
            <el-radio value="manual">手动</el-radio>
          </el-radio-group>
          <el-input-number
              v-if="machineForm.upload_mode === 'manual'"
              v-model="machineForm.upload_value"
              :min="1"
              :max="9999"
              size="small"
              class="value-input"
          />
        </el-form-item>

        <el-divider content-position="left">VIP 权重（多账号线程分配比例）</el-divider>

        <el-alert type="info" :closable="false" show-icon style="margin-bottom: 16px">
          <template #title>
            这是做什么的？（仅在登录多个账号时生效）
          </template>
          <template #default>
            <div style="line-height: 1.8">
              <strong>只有同时登录多个账号</strong>、且机器线程总额不够分时，系统才会按这里的权重<strong>比例</strong>把线程分给各账号——权重越高，分到的线程越多。<br />
              举例：机器共 16 个下载线程，1 个普通账号 + 1 个超级会员账号，按权重 1 : 5 分配 →
              普通账号约分到 <strong>16 × 1/(1+5) ≈ 2~3 个</strong>，超级会员约分到 <strong>16 × 5/(1+5) ≈ 13 个</strong>（各账号不会超过其 VIP 等级的线程上限）。<br />
              这样能让会员/超级会员账号优先拿到更多线程、下得更快，普通账号也保底有线程可用。<br />
              <span style="color: #909399">提示：只有一个账号时此设置不影响速度（不用分配）；数值只看相对比例，1 : 3 : 5 与 2 : 6 : 10 效果相同。</span>
            </div>
          </template>
        </el-alert>

        <!-- 权重输入仅在多账号时显示；单账号无需分配 -->
        <template v-if="authStore.hasMultipleAccounts">
          <el-form-item label="普通用户权重">
            <el-input-number v-model="machineForm.weight_normal" :min="1" :max="100" size="small" />
            <span class="weight-hint">普通账号每份线程的相对比例</span>
          </el-form-item>
          <el-form-item label="普通会员权重">
            <el-input-number v-model="machineForm.weight_vip" :min="1" :max="100" size="small" />
            <span class="weight-hint">VIP 账号每份线程的相对比例</span>
          </el-form-item>
          <el-form-item label="超级会员权重">
            <el-input-number v-model="machineForm.weight_svip" :min="1" :max="100" size="small" />
            <span class="weight-hint">SVIP 账号每份线程的相对比例</span>
          </el-form-item>
        </template>
        <el-alert
            v-else
            type="success"
            :closable="false"
            show-icon
            style="margin-bottom: 16px"
            title="当前只有一个账号，无需按权重分配——该账号会直接使用其 VIP 等级对应的线程上限。登录第 2 个账号后，权重设置才会出现并生效。"
        />

        <el-form-item>
          <el-button
              type="primary"
              :loading="machineSaving"
              :disabled="authStore.readonlyMode"
              @click="saveMachineConfig"
          >
            保存机器配额
          </el-button>
        </el-form-item>
      </el-form>
    </el-card>

    <!-- ========================= Section 2：VIP 推荐配置表 ========================= -->
    <el-card class="setting-card" shadow="never">
      <template #header>
        <div class="card-header">
          <span>VIP 推荐配置（新账号按等级应用）</span>
          <el-tooltip
              effect="dark"
              placement="top"
              content="新账号登录时按其 VIP 等级读取此表中的值作为初始配置。已绑定 auto_apply 的账号会跟随热更新。"
          >
            <el-icon class="info-icon"><InfoFilled /></el-icon>
          </el-tooltip>
        </div>
      </template>

      <el-table :data="vipLevels" stripe :show-header="true" size="default">
        <el-table-column prop="label" label="等级" width="120" />
        <el-table-column label="线程数" min-width="160">
          <template #default="{ row }">
            <el-input-number
                v-model="vipForm[row.key as VipKey].threads"
                :min="1"
                :max="100"
                size="small"
                :disabled="authStore.readonlyMode"
            />
            <el-tooltip
                v-if="vipThreadsOverDefault(row.key)"
                effect="dark"
                placement="top"
                :content="`已超过代码默认值 ${VIP_RECOMMENDED_DEFAULTS[row.key as VipKey].threads}（不拦截，仅提醒）`"
            >
              <el-icon class="warning-icon"><WarningFilled /></el-icon>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column label="分片 MB" min-width="160">
          <template #header>
            <span>分片 MB</span>
            <el-tooltip
                effect="dark"
                placement="top"
                content="分片大小由运行时按文件大小 + VIP 等级自适应计算（详见 design.md §6.7.4），此列仅记录推荐值"
            >
              <el-icon style="margin-left: 4px;"><WarningFilled /></el-icon>
            </el-tooltip>
          </template>
          <template #default="{ row }">
            <el-input-number
                v-model="vipForm[row.key as VipKey].chunk_size_mb"
                :min="1"
                :max="32"
                size="small"
                disabled
            />
          </template>
        </el-table-column>
        <el-table-column label="最大任务数" min-width="140">
          <template #default="{ row }">
            <el-input-number
                v-model="vipForm[row.key as VipKey].max_concurrent_tasks"
                :min="1"
                :max="50"
                size="small"
                :disabled="authStore.readonlyMode"
            />
          </template>
        </el-table-column>
      </el-table>

      <div class="card-actions">
        <el-button :icon="RefreshLeft" @click="resetVipDefaults" :disabled="authStore.readonlyMode">
          恢复默认值
        </el-button>
        <el-button
            type="primary"
            :loading="vipSaving"
            :disabled="authStore.readonlyMode"
            @click="saveVipRecommended"
        >
          保存 VIP 推荐配置
        </el-button>
      </div>
    </el-card>

    <!-- ========================= Section 3：每账号实际配额 ========================= -->
    <el-card class="setting-card" shadow="never">
      <template #header>
        <div class="card-header">
          <span>每账号实际配额（实时）</span>
          <el-tag v-if="budgetStore.isMultiAccount" size="small" type="success">
            实时刷新 (1s)
          </el-tag>
          <el-tag v-else size="small" type="info">单账号 — 只更新一次</el-tag>
        </div>
      </template>

      <el-empty
          v-if="!budgetStore.loaded && !budgetStore.loading"
          description="尚未加载配额数据"
      >
        <el-button type="primary" @click="refreshSnapshot">点击加载</el-button>
      </el-empty>

      <div v-else-if="accountUis.length === 0" class="muted">没有可显示的账号</div>

      <div v-else class="account-list">
        <el-card
            v-for="ui in accountUis"
            :key="ui.account.uid"
            class="account-card"
            shadow="hover"
        >
          <!-- 账号头部 -->
          <div class="account-header">
            <el-avatar :src="ui.account.avatar_url ?? undefined" :size="32">
              {{ (ui.account.nickname || ui.account.username || '?').charAt(0) }}
            </el-avatar>
            <div class="account-id">
              <span class="account-name">
                {{ ui.account.nickname || ui.account.username }}
              </span>
              <el-tag size="small" :type="vipLevelTagType(ui.account.vip_type)">
                {{ vipLevelName(ui.account.vip_type) }}
              </el-tag>
              <el-tag v-if="ui.account.is_active" size="small" type="primary">活跃</el-tag>
            </div>
          </div>

          <!-- auto / custom 切换 -->
          <el-radio-group
              v-if="accountForms[ui.account.uid]"
              v-model="accountForms[ui.account.uid].auto_apply_recommended"
              :disabled="authStore.readonlyMode"
              class="mode-switch"
          >
            <el-radio :value="true">自动按等级推荐</el-radio>
            <el-radio :value="false">自定义</el-radio>
          </el-radio-group>

          <!-- 自定义表单（仅 custom 模式） -->
          <div v-if="accountForms[ui.account.uid] && !accountForms[ui.account.uid].auto_apply_recommended" class="custom-form">
            <el-form label-position="left" label-width="100px" :disabled="authStore.readonlyMode">
              <el-divider content-position="left">下载</el-divider>
              <el-form-item label="线程数">
                <el-input-number
                    v-model="accountForms[ui.account.uid].download.max_global_threads"
                    :min="1"
                    :max="100"
                    size="small"
                />
                <span class="rec-hint">（推荐: {{ vipForm[vipKey(ui.account.vip_type)].threads }} 个）</span>
                <el-tag v-if="downloadOverCap(ui.account.uid)" type="danger" size="small">
                  超过 VIP 硬上限 {{ ui.view.vip_cap_download }}
                </el-tag>
                <div
                    v-if="ui.account.vip_type === 0 && accountForms[ui.account.uid].download.max_global_threads > vipForm.normal.threads"
                    class="thread-warn"
                >
                  ⚠️ 普通用户建议保持 1 个线程，调大可能触发限速！
                </div>
              </el-form-item>
              <el-form-item label="分片 MB">
                <el-input-number
                    v-model="accountForms[ui.account.uid].download.chunk_size_mb"
                    :min="1"
                    :max="32"
                    size="small"
                    disabled
                />
                <el-text size="small" type="info" style="margin-left: 8px;">
                  下载分片由文件大小 + VIP 等级自适应计算，此处仅记录，不影响实际下载
                </el-text>
              </el-form-item>
              <el-form-item label="最大任务数">
                <el-input-number
                    v-model="accountForms[ui.account.uid].download.max_concurrent_tasks"
                    :min="1"
                    :max="50"
                    size="small"
                />
              </el-form-item>

              <el-divider content-position="left">上传</el-divider>
              <el-form-item label="线程数">
                <el-input-number
                    v-model="accountForms[ui.account.uid].upload.max_global_threads"
                    :min="1"
                    :max="100"
                    size="small"
                />
                <span class="rec-hint">（推荐: {{ vipForm[vipKey(ui.account.vip_type)].threads }} 个）</span>
                <el-tag v-if="uploadOverCap(ui.account.uid)" type="danger" size="small">
                  超过 VIP 硬上限 {{ ui.view.vip_cap_upload }}
                </el-tag>
                <div
                    v-if="ui.account.vip_type === 0 && accountForms[ui.account.uid].upload.max_global_threads > vipForm.normal.threads"
                    class="thread-warn"
                >
                  ⚠️ 普通用户建议保持 1 个线程，调大可能触发限速！
                </div>
              </el-form-item>
              <el-form-item label="分片 MB">
                <el-input-number
                    v-model="accountForms[ui.account.uid].upload.chunk_size_mb"
                    :min="1"
                    :max="32"
                    size="small"
                    disabled
                />
                <el-text size="small" type="info" style="margin-left: 8px;">
                  上传分片由文件大小 + VIP 等级自适应计算，此处仅记录，不影响实际上传
                </el-text>
              </el-form-item>
              <el-form-item label="最大任务数">
                <el-input-number
                    v-model="accountForms[ui.account.uid].upload.max_concurrent_tasks"
                    :min="1"
                    :max="50"
                    size="small"
                />
              </el-form-item>
            </el-form>
          </div>

          <!-- 自动按等级推荐模式：明确展示 recompute 算出的实际可用线程 -->
          <div
              v-if="accountForms[ui.account.uid] && accountForms[ui.account.uid].auto_apply_recommended"
              class="auto-effective"
          >
            <div class="auto-effective-grid">
              <div class="ae-row">
                <span class="ae-label">下载实际可用</span>
                <span class="ae-value"><b>{{ ui.view.base_download }}</b> 个线程</span>
                <span class="ae-cap">上限 {{ ui.view.vip_cap_download }}</span>
              </div>
              <div class="ae-row">
                <span class="ae-label">上传实际可用</span>
                <span class="ae-value"><b>{{ ui.view.base_upload }}</b> 个线程</span>
                <span class="ae-cap">上限 {{ ui.view.vip_cap_upload }}</span>
              </div>
            </div>
            <div class="ae-hint">
              <el-icon><InfoFilled /></el-icon>
              <span>
                「实际可用」为按机器总线程 + 会员权重分配后的保底线程数；机器线程富余时最多可冲到「上限」（该 VIP 等级硬上限）。
              </span>
            </div>
          </div>

          <!-- 三段进度条 -->
          <div class="progress-section">
            <div class="progress-label">
              <span class="prog-title">下载</span>
              <span class="prog-meta">
                used <b>{{ ui.view.used_download }}</b> / base <b>{{ ui.view.base_download }}</b>
                / vip_cap <b>{{ ui.view.vip_cap_download }}</b>
              </span>
            </div>
            <div class="triple-bar">
              <div
                  class="triple-bar-fill"
                  :class="`status-${progressMeta(ui.view.used_download, ui.view.base_download, ui.view.vip_cap_download).status}`"
                  :style="{ width: `${progressMeta(ui.view.used_download, ui.view.base_download, ui.view.vip_cap_download).usedPct}%` }"
              />
              <div
                  v-if="ui.view.vip_cap_download > 0"
                  class="triple-bar-base-marker"
                  :style="{ left: `${progressMeta(ui.view.used_download, ui.view.base_download, ui.view.vip_cap_download).basePct}%` }"
                  title="base 标记"
              />
            </div>

            <div class="progress-label">
              <span class="prog-title">上传</span>
              <span class="prog-meta">
                used <b>{{ ui.view.used_upload }}</b> / base <b>{{ ui.view.base_upload }}</b>
                / vip_cap <b>{{ ui.view.vip_cap_upload }}</b>
              </span>
            </div>
            <div class="triple-bar">
              <div
                  class="triple-bar-fill"
                  :class="`status-${progressMeta(ui.view.used_upload, ui.view.base_upload, ui.view.vip_cap_upload).status}`"
                  :style="{ width: `${progressMeta(ui.view.used_upload, ui.view.base_upload, ui.view.vip_cap_upload).usedPct}%` }"
              />
              <div
                  v-if="ui.view.vip_cap_upload > 0"
                  class="triple-bar-base-marker"
                  :style="{ left: `${progressMeta(ui.view.used_upload, ui.view.base_upload, ui.view.vip_cap_upload).basePct}%` }"
                  title="base 标记"
              />
            </div>
          </div>

          <div class="card-actions">
            <el-button
                v-if="accountForms[ui.account.uid]"
                type="primary"
                size="small"
                :loading="accountForms[ui.account.uid].saving"
                :disabled="authStore.readonlyMode || downloadOverCap(ui.account.uid) || uploadOverCap(ui.account.uid)"
                @click="saveAccountCustomConfig(ui.account.uid)"
            >
              保存
            </el-button>
          </div>
        </el-card>
      </div>
    </el-card>
  </div>
</template>

<style scoped lang="scss">
.budget-panel {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.panel-header {
  display: flex;
  align-items: center;
  justify-content: space-between;

  h3 {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
  }
}

.readonly-alert {
  margin-bottom: 8px;
}

.setting-card {
  border-radius: 6px;
}

.card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  font-weight: 600;
}

.info-icon {
  color: #909399;
  cursor: help;
}

.warning-icon {
  color: #e6a23c;
  margin-left: 8px;
  cursor: help;
}

.value-input {
  margin-left: 12px;
}

.weight-hint {
  margin-left: 12px;
  font-size: 12px;
  color: #909399;
}

.rec-hint {
  margin-left: 12px;
  font-size: 12px;
  color: #909399;
}

.thread-warn {
  margin-top: 4px;
  font-size: 12px;
  color: #e6a23c;
  line-height: 1.5;
}

.card-actions {
  display: flex;
  gap: 8px;
  justify-content: flex-end;
  margin-top: 12px;
}

.account-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.account-card {
  border-radius: 6px;
}

.account-header {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 12px;
}

.account-id {
  display: flex;
  align-items: center;
  gap: 8px;

  .account-name {
    font-weight: 500;
  }
}

.mode-switch {
  margin-bottom: 12px;
}

.custom-form {
  background: #fafafa;
  padding: 12px;
  border-radius: 4px;
  margin-bottom: 12px;
}

.auto-effective {
  background: #f0f9eb;
  border: 1px solid #e1f3d8;
  padding: 10px 12px;
  border-radius: 4px;
  margin-bottom: 12px;
}

.auto-effective-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 24px;
}

.ae-row {
  display: flex;
  align-items: baseline;
  gap: 8px;

  .ae-label {
    font-size: 13px;
    color: #606266;
  }

  .ae-value {
    font-size: 13px;
    color: #303133;

    b {
      font-size: 16px;
      color: #67c23a;
    }
  }

  .ae-cap {
    font-size: 12px;
    color: #909399;
  }
}

.ae-hint {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-top: 8px;
  font-size: 12px;
  color: #909399;
  line-height: 1.5;
}

.progress-section {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.progress-label {
  display: flex;
  justify-content: space-between;
  font-size: 12px;
  color: #606266;

  .prog-title {
    font-weight: 500;
  }

  .prog-meta b {
    font-weight: 600;
    color: #303133;
  }
}

/**
 * 三段进度条：
 *   - 整条 = vip_cap（背景灰）
 *   - 填充宽度 = used / vip_cap × 100%
 *   - status 切换填充色（empty / below_base / above_base / full）
 *   - base 位置画一根竖线（虚线）做标记
 */
.triple-bar {
  position: relative;
  width: 100%;
  height: 12px;
  background: #ebeef5;
  border-radius: 6px;
  overflow: hidden;
  margin-bottom: 6px;
}

.triple-bar-fill {
  height: 100%;
  border-radius: 6px 0 0 6px;
  transition: width 0.4s ease, background 0.4s ease;

  &.status-empty {
    background: transparent;
  }

  &.status-below_base {
    background: #67c23a; // 绿色：还在 base 之下，处于 P0 阶段
  }

  &.status-above_base {
    background: #e6a23c; // 橙色：在 base 之上 vip_cap 之下，处于 P1 借调阶段
  }

  &.status-full {
    background: #f56c6c; // 红色：撞 vip_cap，已满载
  }
}

.triple-bar-base-marker {
  position: absolute;
  top: -2px;
  bottom: -2px;
  width: 0;
  border-left: 2px dashed rgba(0, 0, 0, 0.45);
  pointer-events: none;
}

.muted {
  color: #909399;
  font-size: 14px;
  padding: 12px;
}
</style>
