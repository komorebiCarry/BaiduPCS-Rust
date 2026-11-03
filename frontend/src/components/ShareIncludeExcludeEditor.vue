<template>
  <div class="share-include-exclude-editor">
    <!-- 同步内容（include_paths） -->
    <div class="field-block">
      <div class="field-header">
        <span class="field-label">同步内容</span>
        <span v-if="includePaths.length === 0" class="field-state">整个分享</span>
        <div class="path-actions">
          <el-tooltip
              :disabled="ownerLoggedIn !== false"
              content="订阅所属账号未登录，请先登录该账号再预览目录树"
              placement="top"
          >
            <span>
              <el-button
                  size="small"
                  :icon="FolderOpened"
                  :loading="previewing"
                  :disabled="!shareUrl || ownerLoggedIn === false"
                  @click="openPicker"
              >从分享浏览</el-button>
            </span>
          </el-tooltip>
          <el-button
              size="small"
              text
              type="primary"
              :aria-expanded="showIncludeHelp"
              aria-label="同步内容使用说明"
              @click="showIncludeHelp = !showIncludeHelp"
          >
            使用说明
            <el-icon class="help-caret" aria-hidden="true">
              <ArrowUp v-if="showIncludeHelp" />
              <ArrowDown v-else />
            </el-icon>
          </el-button>
        </div>
      </div>
      <div v-if="includePaths.length > 0" class="path-tags">
        <el-tag
            v-for="(p, i) in includePaths"
            :key="p + i"
            closable
            @close="removeInclude(i)"
        >{{ p }}</el-tag>
      </div>
      <p v-if="showIncludeHelp" class="field-help">
        不选则同步整个分享。选择文件夹会包含其全部内容及后续新增内容；选择文件则只同步该文件。
      </p>
      <div v-if="ownerLoggedIn === false" class="owner-offline-hint">
        订阅所属账号未登录，目录树预览不可用，请先登录该账号
      </div>
    </div>

    <!-- 排除目录（exclude_paths）：精确目录排除 -->
    <div class="field-block">
      <div class="field-header">
        <span class="field-label">排除目录</span>
        <div class="path-actions">
          <el-tooltip
              :disabled="ownerLoggedIn !== false"
              content="订阅所属账号未登录，请先登录该账号再预览目录树"
              placement="top"
          >
            <span>
              <el-button
                  size="small"
                  :icon="FolderOpened"
                  :loading="previewing"
                  :disabled="!shareUrl || ownerLoggedIn === false"
                  @click="openExcludePicker"
              >从分享浏览</el-button>
            </span>
          </el-tooltip>
          <el-button
              size="small"
              text
              type="primary"
              :aria-expanded="showExcludeFolderHelp"
              aria-label="排除目录使用说明"
              @click="showExcludeFolderHelp = !showExcludeFolderHelp"
          >
            使用说明
            <el-icon class="help-caret" aria-hidden="true">
              <ArrowUp v-if="showExcludeFolderHelp" />
              <ArrowDown v-else />
            </el-icon>
          </el-button>
        </div>
      </div>
      <div v-if="excludePaths.length > 0" class="path-tags">
        <el-tag
            v-for="(p, i) in excludePaths"
            :key="p + i"
            closable
            type="warning"
            @close="removeExcludePath(i)"
        >{{ p }}</el-tag>
      </div>
      <p v-if="showExcludeFolderHelp" class="field-help">
        所选目录及其全部内容均不扫描、不参与同步。
      </p>
    </div>

    <!-- 排除规则（exclude_patterns 通配符 / exclude_regex_patterns 正则） -->
    <div class="field-block">
      <div class="field-header">
        <span class="field-label">排除规则</span>
        <div class="path-actions">
          <el-button
              size="small"
              text
              type="primary"
              :aria-expanded="showRuleTest"
              aria-label="测试已添加规则"
              @click="toggleRuleTest"
          >测试规则
            <el-icon class="help-caret" aria-hidden="true">
              <ArrowUp v-if="showRuleTest" />
              <ArrowDown v-else />
            </el-icon>
          </el-button>
          <el-button
              size="small"
              text
              type="primary"
              :aria-expanded="showExcludeHelp"
              aria-label="排除规则使用说明"
              @click="showExcludeHelp = !showExcludeHelp"
          >
            使用说明
            <el-icon class="help-caret" aria-hidden="true">
              <ArrowUp v-if="showExcludeHelp" />
              <ArrowDown v-else />
            </el-icon>
          </el-button>
        </div>
      </div>
      <div v-if="excludePatterns.length > 0 || regexPatterns.length > 0" class="path-tags">
        <el-tag
            v-for="(p, i) in excludePatterns"
            :key="'glob-' + p + i"
            closable
            type="info"
            @close="removeExclude(i)"
        >{{ p }}</el-tag>
        <el-tag
            v-for="(p, i) in regexPatterns"
            :key="'regex-' + p + i"
            closable
            type="warning"
            @close="removeRegex(i)"
        ><span class="rule-kind">正则</span>{{ p }}</el-tag>
      </div>
      <div class="exclude-input-row">
        <div class="rule-input-group">
          <el-select
              v-model="ruleKind"
              class="rule-kind-select"
              size="small"
              aria-label="新增规则类型"
          >
            <el-option label="通配符" value="glob" />
            <el-option label="正则表达式" value="regex" :disabled="!regexSupported" />
          </el-select>
          <el-input
              v-model="excludeInput"
              class="exclude-input"
              type="textarea"
              :autosize="{ minRows: 1, maxRows: 4 }"
              resize="none"
              size="small"
              :placeholder="ruleKind === 'regex' ? '输入正则，每行一条' : '输入规则，每行一条'"
              :aria-label="ruleKind === 'regex' ? '输入正则排除规则' : '输入通配符排除规则'"
              @keydown.enter="onExcludeEnter"
          />
        </div>
        <el-button size="small" plain type="primary" :loading="validating" @click="addExclude">添加</el-button>
      </div>
      <div v-if="!regexSupported && regexReady" class="rule-kind-hint">当前后端仅支持通配符</div>
      <p v-if="ruleError" class="rule-error" role="alert">{{ ruleError }}</p>
      <div v-if="showRuleTest" class="rule-test">
        <div class="rule-test-row">
          <span class="rule-test-label">相对分享根目录</span>
          <el-input
              v-model="testPath"
              class="rule-test-input"
              size="small"
              placeholder="如 /股票/按月归档/2025-01"
              aria-label="测试路径"
          />
          <el-button size="small" :loading="testing" aria-label="测试已添加规则" @click="runRuleTest">测试</el-button>
        </div>
        <p v-if="trialResult !== null" class="rule-test-result">
          {{ trialResult ? '会排除该路径' : '不排除该路径' }}
        </p>
        <p v-if="trialError" class="rule-error" role="alert">{{ trialError }}</p>
      </div>
      <ExcludeRuleHelp v-if="showExcludeHelp" :kind="ruleKind" />
      <!-- 常用规则词表：来自系统设置（后端配置），点击即添加到当前规则类型。 -->
      <div class="preset-row">
        <template v-if="activePresets.length > 0">
          <span class="preset-label">常用：</span>
          <el-tag
              v-for="p in activePresets"
              :key="ruleKind + p"
              size="small"
              effect="plain"
              class="preset-tag"
              :type="activeRuleList.includes(p) ? 'success' : 'info'"
              @click="applyPreset(p)"
          >{{ p }}</el-tag>
        </template>
        <el-button
            size="small"
            text
            :icon="Star"
            :disabled="activeRuleList.length === 0 || savingPresets"
            :loading="savingPresets"
            @click="saveCurrentAsPresets"
        >存为常用</el-button>
        <el-button size="small" text :icon="Setting" @click="goManagePresets">管理常用规则</el-button>
      </div>
    </div>

    <!-- 从分享选择子路径：复用转存同款 ShareFileSelector -->
    <el-dialog
        v-model="pickerVisible"
        :title="pickerMode === 'exclude' ? '从分享中选择要排除的目录' : '从分享中选择要同步的子路径'"
        width="680px"
        :close-on-click-modal="false"
        append-to-body
    >
      <ShareFileSelector
          :files="previewFiles"
          :loading="previewing"
          :share-info="previewShareInfo"
          :share-url="shareUrl"
          :share-password="password || undefined"
          :folder-only="pickerMode === 'exclude'"
          @update:selected-files="onSelectedFiles"
      />
      <template #footer>
        <span class="picker-hint">已选 {{ pendingSelected.length }} 项</span>
        <el-button @click="pickerVisible = false">取消</el-button>
        <el-button type="primary" @click="confirmPicker">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import type { AxiosError } from 'axios'
import { ArrowDown, ArrowUp, FolderOpened, Setting, Star } from '@element-plus/icons-vue'
import ShareFileSelector from './ShareFileSelector.vue'
import ExcludeRuleHelp from './ExcludeRuleHelp.vue'
import { getConfig, updateConfig } from '@/api/config'
import {
  getExcludeRuleCapabilities,
  validateExcludeRules,
} from '@/api/shareSync'
import {
  describeRuleError,
  describeRuleErrorFrom,
  mergeRules,
  normalizeRuleList,
  parseRuleInput,
  type ExcludeRuleKind as RuleKind,
} from '@/utils/excludeRules'
import {
  previewShareFiles,
  type SharedFileInfo,
  type PreviewShareInfo,
} from '@/api/transfer'

const props = defineProps<{
  shareUrl: string
  password?: string | null
  includePaths: string[]
  excludePatterns: string[]
  /** 排除正则（Rust 语法，整条路径匹配）；缺省按空处理 */
  excludeRegexPatterns?: string[]
  /** 精确排除的目录路径（相对分享根） */
  excludePaths: string[]
  // 订阅所属账号：编辑订阅时传订阅自己的 owner_uid，预览目录树按该账号路由 client
  // （不传则后端回退当前 active 账号——转存对话框创建场景 owner=active）
  ownerUid?: number | null
  // 订阅所属账号是否已登录：显式 false 时禁用目录树预览并提示登录该账号。
  // undefined（创建场景）= 默认按已登录处理。
  ownerLoggedIn?: boolean
}>()

const emit = defineEmits<{
  'update:includePaths': [value: string[]]
  'update:excludePatterns': [value: string[]]
  'update:excludeRegexPatterns': [value: string[]]
  'update:excludePaths': [value: string[]]
}>()

const router = useRouter()
const excludeInput = ref('')

const regexPatterns = computed<string[]>(() => props.excludeRegexPatterns ?? [])
const activeRuleList = computed<string[]>(() =>
    ruleKind.value === 'regex' ? regexPatterns.value : props.excludePatterns,
)

// 新增规则的类型：默认通配符（历史行为）
const ruleKind = ref<RuleKind>('glob')

// 后端是否支持正则；未确认前正则选项不可选，老服务端保持纯 glob 编辑
const regexSupported = ref(false)
const regexReady = ref(false)

const ruleError = ref<string | null>(null)
const validating = ref(false)

// 分享文件选择器（与转存对话框同款体验）
const pickerVisible = ref(false)
// 选择器用途：同步子路径 / 排除目录（后者只允许选文件夹）
const pickerMode = ref<'include' | 'exclude'>('include')
const previewing = ref(false)
const previewFiles = ref<SharedFileInfo[]>([])
const previewShareInfo = ref<PreviewShareInfo | null>(null)
const pendingSelected = ref<SharedFileInfo[]>([])

function normalizePath(v: string): string {
  const s = v.trim().replace(/\/+/g, '/')
  if (!s) return ''
  const prefixed = s.startsWith('/') ? s : `/${s}`
  if (prefixed.length === 1) return '/'
  return prefixed.endsWith('/') ? prefixed.slice(0, -1) : prefixed
}

function removeInclude(i: number) {
  const next = [...props.includePaths]
  next.splice(i, 1)
  emit('update:includePaths', next)
}

function removeExcludePath(i: number) {
  const next = [...props.excludePaths]
  next.splice(i, 1)
  emit('update:excludePaths', next)
}

// 各字段的使用说明展开态（彼此独立，默认全部折叠）
const showIncludeHelp = ref(false)
const showExcludeFolderHelp = ref(false)
const showExcludeHelp = ref(false)

// ==================== 规则新增（后端权威校验） ====================

function validationUnavailable(e: unknown): string {
  const ax = e as AxiosError<{ message?: string; error?: string }>
  const detail = ax?.response?.data?.message || ax?.response?.data?.error || (e as Error)?.message
  return detail
      ? `无法校验排除规则：${detail}`
      : '无法校验排除规则：当前后端不支持正则校验'
}

function candidateLists(kind: RuleKind, incoming: string[]) {
  const globs = kind === 'glob'
      ? mergeRules(props.excludePatterns, incoming)
      : [...props.excludePatterns]
  const regexes = kind === 'regex'
      ? mergeRules(regexPatterns.value, incoming)
      : [...regexPatterns.value]
  return { globs, regexes }
}

/**
 * 正则（或已存在正则）先经后端整批校验；异步返回时要求来源输入、类型与两份规则列表
 * 仍是发起快照，避免过期批次覆盖新输入。词表点选是旁路操作，保留 textarea 未提交草稿。
 */
async function commitRules(
    kind: RuleKind,
    incoming: string[],
    source: 'input' | 'preset' = 'input',
) {
  if (validating.value || incoming.length === 0) return
  const inputSnapshot = source === 'input' ? excludeInput.value : null
  const sourceInputUnchanged = () =>
      source !== 'input' || excludeInput.value === inputSnapshot
  const clearSourceInput = () => {
    if (source === 'input' && excludeInput.value === inputSnapshot) excludeInput.value = ''
  }
  const globsSnapshot = [...props.excludePatterns]
  const regexesSnapshot = [...regexPatterns.value]
  const { globs, regexes } = candidateLists(kind, incoming)
  const changed = kind === 'glob'
      ? globs.length !== globsSnapshot.length
      : regexes.length !== regexesSnapshot.length
  if (!changed) {
    clearSourceInput()
    return
  }

  if (regexes.length > 0) {
    validating.value = true
    try {
      const resp = await validateExcludeRules({
        exclude_patterns: globs,
        exclude_regex_patterns: regexes,
      })
      const stale = !sourceInputUnchanged()
          || ruleKind.value !== kind
          || !sameList(props.excludePatterns, globsSnapshot)
          || !sameList(regexPatterns.value, regexesSnapshot)
      if (stale) return
      if (!resp.valid) {
        const baseLength = kind === 'glob' ? globsSnapshot.length : regexesSnapshot.length
        ruleError.value = resp.errors.length
            ? describeRuleErrorFrom(resp.errors[0], kind, baseLength)
            : '排除规则校验失败'
        return
      }
    } catch (e) {
      if (sourceInputUnchanged() && ruleKind.value === kind) {
        ruleError.value = validationUnavailable(e)
      }
      return
    } finally {
      validating.value = false
    }
  }

  ruleError.value = null
  clearTrial()
  if (kind === 'glob') emit('update:excludePatterns', globs)
  else emit('update:excludeRegexPatterns', regexes)
  clearSourceInput()
}

function sameList(a: readonly string[], b: readonly string[]) {
  return a.length === b.length && a.every((v, i) => v === b[i])
}

/** 提交输入框内容：按行拆成规则（行内逗号等标点属于规则本身，不切分） */
function addExclude() {
  void commitRules(ruleKind.value, parseRuleInput(excludeInput.value))
}

/**
 * textarea 里回车 = 提交（逐条输入的自然节奏），而不是换行。
 * 中文输入法组词期间的回车（选字确认）不触发提交。
 * 粘贴多行文本无需拦截 —— textarea 原生保留换行，提交时按行拆分。
 */
function onExcludeEnter(e: Event) {
  const ke = e as KeyboardEvent
  if (ke.isComposing) return // 输入法组词中，回车是选字不是提交
  ke.preventDefault()
  addExclude()
}

function removeExclude(i: number) {
  const next = [...props.excludePatterns]
  next.splice(i, 1)
  ruleError.value = null
  clearTrial()
  emit('update:excludePatterns', next)
}

function removeRegex(i: number) {
  const next = [...regexPatterns.value]
  next.splice(i, 1)
  ruleError.value = null
  clearTrial()
  emit('update:excludeRegexPatterns', next)
}

// ==================== 试匹配 ====================
// 结果与错误都绑定到产生它们的那一次请求快照（测试路径 + 两类已添加规则）。
// 路径或规则列表一变就立即失效；在飞的旧请求即使后到也不能覆盖现状。
const showRuleTest = ref(false)
const testPath = ref('')
const testing = ref(false)
/** 显示中的结果（仅当快照仍与当前状态一致时才渲染） */
const trialOutcome = ref<{
  path: string
  globs: string[]
  regexes: string[]
  excluded: boolean
} | null>(null)
const trialError = ref<string | null>(null)
/** 每次路径/规则变化或新测试自增；在飞响应必须与发起时一致才生效 */
const trialGeneration = ref(0)

const trialResult = computed<boolean | null>(() => {
  const outcome = trialOutcome.value
  if (!outcome) return null
  if (outcome.path !== testPath.value) return null
  if (!sameList(outcome.globs, props.excludePatterns)) return null
  if (!sameList(outcome.regexes, regexPatterns.value)) return null
  return outcome.excluded
})

/** 作废显示中的结果与错误，并让在飞请求失效 */
function clearTrial() {
  trialOutcome.value = null
  trialError.value = null
  trialGeneration.value += 1
}

watch(testPath, () => clearTrial())
watch(
    [() => props.excludePatterns, regexPatterns],
    () => clearTrial(),
    { deep: true },
)

function toggleRuleTest() {
  showRuleTest.value = !showRuleTest.value
  if (!showRuleTest.value) clearTrial()
}

/** 只测试已添加的两类规则（与 UI 标注一致），试匹配语义由后端决定 */
async function runRuleTest() {
  if (testing.value) return
  clearTrial()
  const generation = trialGeneration.value
  const globsSnapshot = [...props.excludePatterns]
  const regexesSnapshot = [...regexPatterns.value]
  const pathSnapshot = testPath.value
  testing.value = true
  try {
    const resp = await validateExcludeRules({
      exclude_patterns: globsSnapshot,
      exclude_regex_patterns: regexesSnapshot,
      test_path: pathSnapshot,
    })
    if (generation !== trialGeneration.value) return
    if (!sameList(props.excludePatterns, globsSnapshot)
        || !sameList(regexPatterns.value, regexesSnapshot)
        || testPath.value !== pathSnapshot) return
    if (!resp.valid) {
      trialError.value = resp.errors.length
          ? describeRuleError(resp.errors[0])
          : '排除规则校验失败'
      return
    }
    trialOutcome.value = {
      path: pathSnapshot,
      globs: globsSnapshot,
      regexes: regexesSnapshot,
      excluded: resp.excluded === true,
    }
  } catch (e) {
    if (generation === trialGeneration.value && testPath.value === pathSnapshot) {
      trialError.value = validationUnavailable(e)
    }
  } finally {
    testing.value = false
  }
}

// ==================== 常用规则词表 ====================
// 词表存在后端配置（系统设置 → 分享同步 中维护），跨浏览器/设备一致。
// 通配符与正则各有一份词表，互不写入对方字段。
const globPresets = ref<string[]>([])
const regexPresets = ref<string[]>([])
const savingPresets = ref(false)

const activePresets = computed<string[]>(() =>
    ruleKind.value === 'regex' ? regexPresets.value : globPresets.value,
)

onMounted(async () => {
  // 能力探测与词表加载互不阻塞：老服务端缺 capabilities 也要能正常加载词表
  const capsPromise = getExcludeRuleCapabilities()
      .then((caps) => {
        regexSupported.value = caps.supports_regex === true
      })
      .catch(() => {
        // 404 / 网络失败：保持纯 glob 编辑；已有正则仍可见、可删，但保存会被后端校验拦住
        regexSupported.value = false
      })
      .finally(() => {
        regexReady.value = true
      })

  const presetsPromise = getConfig()
      .then((cfg) => {
        globPresets.value = normalizeRuleList(cfg.share_sync?.exclude_preset_rules)
        regexPresets.value = normalizeRuleList(cfg.share_sync?.exclude_regex_preset_rules)
      })
      .catch(() => {
        // 配置加载失败不阻断编辑器；词表行为空态，不影响手输规则
      })

  await Promise.all([capsPromise, presetsPromise])
})

/** 点击词表 chip → 添加到当前规则类型（已存在则忽略）；旁路操作保留未提交草稿。 */
function applyPreset(p: string) {
  if (activeRuleList.value.includes(p)) return
  void commitRules(ruleKind.value, [p], 'preset')
}

/** 把当前类型的规则并入对应词表并写回后端配置，方便下次建订阅时直接点选 */
async function saveCurrentAsPresets() {
  const kind = ruleKind.value
  const current = [...activeRuleList.value]
  if (current.length === 0 || savingPresets.value) return
  savingPresets.value = true
  try {
    // 读最新配置再合并，避免覆盖设置页刚保存的其它字段
    const cfg = await getConfig()
    const storedGlobs = normalizeRuleList(cfg.share_sync?.exclude_preset_rules)
    const storedRegexes = normalizeRuleList(cfg.share_sync?.exclude_regex_preset_rules)
    const nextGlobs = kind === 'glob' ? mergeRules(storedGlobs, current) : storedGlobs
    const nextRegexes = kind === 'regex' ? mergeRules(storedRegexes, current) : storedRegexes
    cfg.share_sync = {
      ...(cfg.share_sync ?? {}),
      exclude_preset_rules: nextGlobs,
      exclude_regex_preset_rules: nextRegexes,
    }
    await updateConfig(cfg)
    globPresets.value = nextGlobs
    regexPresets.value = nextRegexes
    ElMessage.success('已存为常用规则（可在系统设置中管理）')
  } catch (e) {
    ElMessage.error(`保存常用规则失败: ${(e as Error)?.message || '未知错误'}`)
  } finally {
    savingPresets.value = false
  }
}

/** 跳转到系统设置的分享同步区块管理词表（设置页支持 ?tab=xxx 定位 section） */
function goManagePresets() {
  router.push({ path: '/settings', query: { tab: 'sharesync' } })
}

/**
 * 供父组件在保存前调用：存在正则规则时必须由后端确认整份配置合法。
 * 后端不可用 / 校验失败都返回不通过，绝不静默当作成功。
 */
async function validateRules(): Promise<{ ok: boolean; message?: string }> {
  const regexes = [...regexPatterns.value]
  if (regexes.length === 0) return { ok: true }
  try {
    const resp = await validateExcludeRules({
      exclude_patterns: [...props.excludePatterns],
      exclude_regex_patterns: regexes,
    })
    if (!resp.valid) {
      return {
        ok: false,
        message: resp.errors.length ? describeRuleError(resp.errors[0]) : '排除规则校验失败',
      }
    }
    return { ok: true }
  } catch (e) {
    return { ok: false, message: validationUnavailable(e) }
  }
}

defineExpose({ validateRules })

// ==================== 分享目录选择 ====================

async function loadPickerFiles() {
  previewing.value = true
  try {
    const resp = await previewShareFiles({
      share_url: props.shareUrl.trim(),
      password: props.password || undefined,
    })
    previewFiles.value = resp.files || []
    previewShareInfo.value = resp.share_info ?? null
    pendingSelected.value = []
    pickerVisible.value = true
  } catch (e) {
    const ax = e as AxiosError<{ message?: string; error?: string }>
    ElMessage.error(
        ax?.response?.data?.message ||
        ax?.response?.data?.error ||
        (e as Error)?.message ||
        '加载分享文件失败',
    )
  } finally {
    previewing.value = false
  }
}

async function openPicker() {
  if (!props.shareUrl || !props.shareUrl.trim()) {
    ElMessage.warning('请先填写分享链接')
    return
  }
  if (props.ownerLoggedIn === false) {
    ElMessage.warning('订阅所属账号未登录，请先登录该账号再预览目录树')
    return
  }
  pickerMode.value = 'include'
  await loadPickerFiles()
}

async function openExcludePicker() {
  if (!props.shareUrl || !props.shareUrl.trim()) {
    ElMessage.warning('请先填写分享链接')
    return
  }
  if (props.ownerLoggedIn === false) {
    ElMessage.warning('订阅所属账号未登录，请先登录该账号再预览目录树')
    return
  }
  pickerMode.value = 'exclude'
  await loadPickerFiles()
}

function onSelectedFiles(files: SharedFileInfo[]) {
  pendingSelected.value = files
}

function confirmPicker() {
  const selectedPaths = pendingSelected.value
      .map((f) => normalizePath(f.path))
      .filter((p) => p.length > 0)
  if (pickerMode.value === 'exclude') {
    // 精确排除：合并去重；祖先目录已覆盖的后代不必单独保留
    const merged: string[] = []
    const seen = new Set<string>()
    for (const p of [...props.excludePaths, ...selectedPaths]) {
      if (seen.has(p)) continue
      seen.add(p)
      merged.push(p)
    }
    const trimmed = merged.filter(
        (p) => !merged.some((a) => a !== p && p.startsWith(`${a}/`)),
    )
    emit('update:excludePaths', trimmed)
    pickerVisible.value = false
    return
  }
  // 与已有路径合并去重；勾选目录即代表整棵子树，裁剪掉被祖先覆盖的冗余后代
  const merged: string[] = []
  const seen = new Set<string>()
  for (const p of [...props.includePaths, ...selectedPaths]) {
    if (seen.has(p)) continue
    seen.add(p)
    merged.push(p)
  }
  const trimmed = merged.filter(
      (p) => !merged.some((a) => a !== p && p.startsWith(`${a}/`)),
  )
  emit('update:includePaths', trimmed)
  pickerVisible.value = false
}
</script>

<style scoped lang="scss">
// 占满父级内容宽度并允许收缩。width:100% 固定宽度基准 —— 否则展开说明会改变
// 固有宽度，右对齐的操作区随之横移；min-width:0 解除 flex 子项的固有宽度下限。
.share-include-exclude-editor {
  box-sizing: border-box;
  width: 100%;
  min-width: 0;
  max-width: 100%;
}

.field-block {
  margin-bottom: 12px;
}

// 最后一个字段块不再叠加底部外边距，避免与父级表单间距重复。
.field-block:last-of-type {
  margin-bottom: 0;
}

// 头部一行：标题 + 状态 + 操作。窄屏允许换行，不强制单行不断。
.field-header {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px 8px;
}

.field-label {
  font-size: 13px;
  color: var(--el-text-color-regular);
}

.field-state {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.owner-offline-hint {
  font-size: 12px;
  color: var(--el-color-warning);
  margin-top: 6px;
}

.path-actions {
  margin-left: auto;
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px;
}

.help-caret {
  margin-left: 2px;
  font-size: 12px;
}

.path-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-top: 6px;

  // 长路径折行，避免撑破窄容器
  :deep(.el-tag) {
    max-width: 100%;
    height: auto;
    line-height: 1.6;
    white-space: normal;
    word-break: break-all;
  }
}

.rule-kind {
  margin-right: 4px;
  padding: 0 3px;
  border-radius: 2px;
  background: rgba(0, 0, 0, 0.06);
  font-size: 11px;
}

.field-help {
  margin: 6px 0 0;
  font-size: 12px;
  line-height: 1.6;
  color: var(--el-text-color-secondary);
}

.rule-kind-hint {
  margin-top: 4px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.rule-error {
  margin: 6px 0 0;
  font-size: 12px;
  line-height: 1.5;
  color: var(--el-color-danger);
  word-break: break-word;
}

.rule-test {
  margin-top: 6px;
  padding: 8px 10px;
  border-radius: 6px;
  background: var(--el-fill-color-lighter);
}

.rule-test-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px 8px;
  margin-top: 4px;
}

.rule-test-label {
  font-size: 12px;
  color: var(--el-text-color-regular);
  flex-shrink: 0;
}

.rule-test-input {
  flex: 1 1 200px;
  min-width: 0;
  max-width: 100%;
}

.rule-test-result {
  margin: 6px 0 0;
  font-size: 12px;
  color: var(--el-text-color-primary);
}

.picker-hint {
  margin-right: auto;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.exclude-input-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px;
  margin-top: 6px;
}

// 类型下拉框与规则输入框始终并排：窄屏只允许「添加」换行，二者不拆开。
.rule-input-group {
  display: flex;
  align-items: center;
  flex: 1 1 200px;
  gap: 4px;
  min-width: 0;
  max-width: 100%;
}

.rule-kind-select {
  flex: 0 0 auto;
  width: 104px;
}

// 下拉框、输入框与添加按钮在桌面端同高对齐
.rule-kind-select :deep(.el-select__wrapper) {
  min-height: 28px;
}

.exclude-input {
  flex: 1 1 0;
  width: auto;
  min-width: 0;
  max-width: 100%;
}

.exclude-input-row > .el-button {
  min-height: 28px;
}

.preset-row {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px;
  margin-top: 6px;
}

.preset-label {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  flex-shrink: 0;
}

// 词表 chip 可点击添加，给出手型反馈
.preset-tag {
  cursor: pointer;
  user-select: none;
}

</style>
