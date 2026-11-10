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
            <span class="browse-action">
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
        >{{ formatSharePathForDisplay(p) }}</el-tag>
      </div>
      <p v-if="showIncludeHelp" class="field-help">{{ INCLUDE_HELP_TEXT }}</p>
      <div v-if="ownerLoggedIn === false" class="owner-offline-hint">
        订阅所属账号未登录，目录树预览不可用，请先登录该账号
      </div>
    </div>

    <!-- 包含规则（include_patterns 通配符 / include_regex_patterns 正则） -->
    <div class="field-block">
      <div class="field-header">
        <span class="field-label">包含规则</span>
        <div class="path-actions">
          <el-button
              size="small"
              text
              type="primary"
              :aria-expanded="showIncludeRuleHelp"
              aria-label="包含规则使用说明"
              @click="showIncludeRuleHelp = !showIncludeRuleHelp"
          >
            使用说明
            <el-icon class="help-caret" aria-hidden="true">
              <ArrowUp v-if="showIncludeRuleHelp" />
              <ArrowDown v-else />
            </el-icon>
          </el-button>
        </div>
      </div>
      <div v-if="includePatternList.length > 0 || includeRegexList.length > 0" class="path-tags">
        <el-tag
            v-for="(p, i) in includePatternList"
            :key="'inc-glob-' + p + i"
            closable
            type="success"
            @close="removeIncludePattern(i)"
        >{{ p }}</el-tag>
        <el-tag
            v-for="(p, i) in includeRegexList"
            :key="'inc-regex-' + p + i"
            closable
            type="success"
            @close="removeIncludeRegex(i)"
        ><span class="rule-kind">正则</span>{{ p }}</el-tag>
      </div>
      <div class="exclude-input-row">
        <div class="rule-input-group">
          <el-select
              v-model="includeRuleKind"
              class="rule-kind-select"
              size="small"
              aria-label="新增包含规则类型"
          >
            <el-option label="通配符" value="glob" />
            <el-option label="正则表达式" value="regex" :disabled="!regexSupported" />
          </el-select>
          <el-input
              v-model="includeInput"
              class="exclude-input"
              type="textarea"
              :autosize="{ minRows: 1, maxRows: 4 }"
              resize="none"
              size="small"
              :placeholder="includeRuleKind === 'regex' ? '输入正则，每行一条' : '输入规则，每行一条'"
              :aria-label="includeRuleKind === 'regex' ? '输入正则包含规则' : '输入通配符包含规则'"
              @keydown.enter="onIncludeEnter"
          />
        </div>
        <el-button size="small" plain type="primary" :loading="includeValidating" @click="addIncludeRule">添加</el-button>
      </div>
      <p v-if="includeRuleError" class="rule-error" role="alert">{{ includeRuleError }}</p>
      <ExcludeRuleHelp v-if="showIncludeRuleHelp" :kind="includeRuleKind" mode="include" />
    </div>

    <!-- 排除内容（exclude_paths）：精确排除文件或目录 -->
    <div class="field-block">
      <div class="field-header">
        <span class="field-label">排除内容</span>
        <div class="path-actions">
          <el-tooltip
              :disabled="ownerLoggedIn !== false"
              content="订阅所属账号未登录，请先登录该账号再预览目录树"
              placement="top"
          >
            <span class="browse-action">
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
              aria-label="排除内容使用说明"
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
        >{{ formatSharePathForDisplay(p) }}</el-tag>
      </div>
      <p v-if="showExcludeFolderHelp" class="field-help">
        选择文件只排除该文件；选择目录则连同其中全部内容一起排除，均不参与同步。
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

    <!-- 从分享选择内容：复用转存同款 ShareFileSelector（路径选择模式） -->
    <el-dialog
        v-model="pickerVisible"
        :title="pickerMode === 'exclude' ? '从分享中选择要排除的内容' : '从分享中选择要同步的内容'"
        width="680px"
        :close-on-click-modal="false"
        destroy-on-close
        append-to-body
    >
      <el-alert
          v-if="followLostNotice"
          class="follow-lost-alert"
          type="warning"
          :closable="false"
          show-icon
          :title="followLostNotice.title"
      >
        <template #default>
          <span class="follow-lost-desc">{{ followLostNotice.description }}</span>
        </template>
      </el-alert>
      <ShareFileSelector
          :key="pickerKey"
          :files="previewFiles"
          :loading="previewing"
          :share-info="previewShareInfo"
          :share-url="shareUrl"
          :share-password="password || undefined"
          path-selection
          :initial-selected-paths="pickerInitialPaths"
          :empty-selection-means-all="pickerMode === 'include'"
          @update:selected-paths="onSelectedPaths"
          @update:selection-ready="onSelectionReady"
          @update:follow-lost-dirs="onFollowLostDirs"
      />
      <template #footer>
        <span class="picker-hint">{{ pickerHint }}</span>
        <el-button @click="pickerVisible = false">取消</el-button>
        <el-button type="primary" :disabled="!selectionReady" @click="confirmPicker">确定</el-button>
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
import { formatSharePathForDisplay } from '@/utils/shareSelection'
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
  /** 包含通配符；缺省按空处理（不额外限制） */
  includePatterns?: string[]
  /** 包含正则（Rust 语法，整条路径匹配）；缺省按空处理 */
  includeRegexPatterns?: string[]
  /** 精确排除的文件或目录路径（相对分享根） */
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
  'update:includePatterns': [value: string[]]
  'update:includeRegexPatterns': [value: string[]]
  'update:excludePaths': [value: string[]]
}>()

const router = useRouter()
const excludeInput = ref('')

const regexPatterns = computed<string[]>(() => props.excludeRegexPatterns ?? [])
const includePatternList = computed<string[]>(() => props.includePatterns ?? [])
const includeRegexList = computed<string[]>(() => props.includeRegexPatterns ?? [])
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
// 选择器用途：同步内容 / 排除内容（两者都按路径选择，文件与目录都可选）
const pickerMode = ref<'include' | 'exclude'>('include')
const previewing = ref(false)
const previewFiles = ref<SharedFileInfo[]>([])
const previewShareInfo = ref<PreviewShareInfo | null>(null)
/** 打开时的已存路径快照：只作初始种子，不随待确认状态变化 */
const pickerInitialPaths = ref<string[]>([])
/** 选择器当前待确认的路径 */
const pendingPaths = ref<string[]>([])
/** 选择器是否可以确认（清空同步内容时为 false） */
const selectionReady = ref(true)
/** 选择器会话代号：预览响应回来时会话已变则丢弃，避免改动已重开的对话框 */
let pickerSession = 0
/** 选择器实例代号：每次打开都换新实例，保证回到的是已存内容而非上次的临时改动 */
const pickerKey = ref(0)

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
const showIncludeRuleHelp = ref(false)
const showExcludeFolderHelp = ref(false)
const showExcludeHelp = ref(false)

const includeInput = ref('')
const includeRuleKind = ref<RuleKind>('glob')
const includeRuleError = ref<string | null>(null)
const includeValidating = ref(false)

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
function includeCandidateLists(kind: RuleKind, incoming: string[]) {
  const globs = kind === 'glob'
      ? mergeRules(includePatternList.value, incoming)
      : [...includePatternList.value]
  const regexes = kind === 'regex'
      ? mergeRules(includeRegexList.value, incoming)
      : [...includeRegexList.value]
  return { globs, regexes }
}

async function commitIncludeRules(kind: RuleKind, incoming: string[]) {
  if (includeValidating.value || incoming.length === 0) return
  const inputSnapshot = includeInput.value
  const globsSnapshot = [...includePatternList.value]
  const regexesSnapshot = [...includeRegexList.value]
  const { globs, regexes } = includeCandidateLists(kind, incoming)
  const changed = kind === 'glob'
      ? globs.length !== globsSnapshot.length
      : regexes.length !== regexesSnapshot.length
  if (!changed) {
    if (includeInput.value === inputSnapshot) includeInput.value = ''
    return
  }
  if (regexes.length > 0) {
    includeValidating.value = true
    try {
      const resp = await validateExcludeRules({
        exclude_patterns: globs,
        exclude_regex_patterns: regexes,
      })
      if (includeInput.value !== inputSnapshot || includeRuleKind.value !== kind) return
      if (!resp.valid) {
        const baseLength = kind === 'glob' ? globsSnapshot.length : regexesSnapshot.length
        includeRuleError.value = resp.errors.length
            ? describeRuleErrorFrom(resp.errors[0], kind, baseLength)
            : '包含规则校验失败'
        return
      }
    } catch (e) {
      if (includeInput.value === inputSnapshot && includeRuleKind.value === kind) {
        includeRuleError.value = validationUnavailable(e).replace('排除规则', '包含规则')
      }
      return
    } finally {
      includeValidating.value = false
    }
  }
  includeRuleError.value = null
  if (kind === 'glob') emit('update:includePatterns', globs)
  else emit('update:includeRegexPatterns', regexes)
  if (includeInput.value === inputSnapshot) includeInput.value = ''
}

function addIncludeRule() {
  void commitIncludeRules(includeRuleKind.value, parseRuleInput(includeInput.value))
}

function onIncludeEnter(e: Event) {
  const ke = e as KeyboardEvent
  if (ke.isComposing) return
  ke.preventDefault()
  addIncludeRule()
}

function removeIncludePattern(i: number) {
  const next = [...includePatternList.value]
  next.splice(i, 1)
  includeRuleError.value = null
  emit('update:includePatterns', next)
}

function removeIncludeRegex(i: number) {
  const next = [...includeRegexList.value]
  next.splice(i, 1)
  includeRuleError.value = null
  emit('update:includeRegexPatterns', next)
}

async function validateRules(): Promise<{ ok: boolean; message?: string }> {
  const excludeRegexes = [...regexPatterns.value]
  const includeRegexes = [...includeRegexList.value]
  if (excludeRegexes.length === 0 && includeRegexes.length === 0) return { ok: true }
  try {
    if (excludeRegexes.length > 0) {
      const resp = await validateExcludeRules({
        exclude_patterns: [...props.excludePatterns],
        exclude_regex_patterns: excludeRegexes,
      })
      if (!resp.valid) {
        return {
          ok: false,
          message: resp.errors.length ? describeRuleError(resp.errors[0]) : '排除规则校验失败',
        }
      }
    }
    if (includeRegexes.length > 0) {
      const resp = await validateExcludeRules({
        exclude_patterns: [...includePatternList.value],
        exclude_regex_patterns: includeRegexes,
      })
      if (!resp.valid) {
        return {
          ok: false,
          message: resp.errors.length ? describeRuleError(resp.errors[0]) : '包含规则校验失败',
        }
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
  const session = ++pickerSession
  try {
    const resp = await previewShareFiles({
      share_url: props.shareUrl.trim(),
      password: props.password || undefined,
    })
    if (session !== pickerSession) return
    previewFiles.value = resp.files || []
    previewShareInfo.value = resp.share_info ?? null
    pickerVisible.value = true
  } catch (e) {
    if (session !== pickerSession) return
    const ax = e as AxiosError<{ message?: string; error?: string }>
    ElMessage.error(
        ax?.response?.data?.message ||
        ax?.response?.data?.error ||
        (e as Error)?.message ||
        '加载分享文件失败',
    )
  } finally {
    if (session === pickerSession) previewing.value = false
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
  // 打开时快照已存路径：选择器以它为初始种子，待确认状态不参与
  pickerInitialPaths.value = [...props.includePaths]
  pendingPaths.value = [...props.includePaths]
  selectionReady.value = true
  pickerKey.value += 1
  followLostDirs.value = []
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
  pickerInitialPaths.value = [...props.excludePaths]
  pendingPaths.value = [...props.excludePaths]
  selectionReady.value = true
  pickerKey.value += 1
  followLostDirs.value = []
  await loadPickerFiles()
}

function onSelectedPaths(paths: string[]) {
  pendingPaths.value = [...paths]
}

function onSelectionReady(ready: boolean) {
  selectionReady.value = ready
}

// 中文文案之间不能有模板换行带来的空格，因此拼接成一段
const INCLUDE_HELP_TEXT = [
  '不选则同步整个分享。选择文件夹会包含其全部内容及后续新增内容；选择文件则只同步该文件。',
  '在已整体选中的文件夹（或整个分享）里取消个别项目，会把它改为逐项选择，之后新增的内容不再自动同步；',
  '只想跳过个别内容时，建议保留整体选择，改用「排除内容」。',
].join('')

/** 本次选择中被拆成逐项选择、且尚未重新整体勾选的目录（'' = 整个分享） */
const followLostDirs = ref<string[]>([])

function onFollowLostDirs(dirs: string[]) {
  followLostDirs.value = [...dirs]
}

/** 最多列 3 个位置，其余折叠为「等 N 处」 */
function describeFollowLostDirs(dirs: string[]): string {
  const names = dirs.map((d) => (d === '' ? '整个分享' : `「${formatSharePathForDisplay(d)}」`))
  if (names.length <= 3) return names.join('、')
  return `${names.slice(0, 3).join('、')} 等 ${names.length} 处`
}

/**
 * 取消整体选中目录里的个别项目后的提示：该目录被拆成逐项选择，之后新增的内容不会
 * 再被自动同步 / 排除。目录被重新整体勾选后提示自动消失；不弹窗、不打断操作。
 */
const followLostNotice = computed<{ title: string; description: string } | null>(() => {
  const dirs = followLostDirs.value
  if (dirs.length === 0) return null
  const where = describeFollowLostDirs(dirs)
  if (pickerMode.value === 'exclude') {
    return {
      title: `${where}已改为逐项排除`,
      description: '这些目录之后新增的内容不会再被自动排除。如需继续整体排除，请重新勾选该目录。',
    }
  }
  return {
    title: `${where}已改为逐项选择`,
    description: '这些位置之后新增的内容将不再自动同步。如果只是想跳过个别内容、并让目录继续自动包含新内容，'
        + '请点「取消」放弃本次修改，保留整体选择，改用「排除内容」排除不需要的项目。',
  }
})

/** 选择器底部提示：整体分享、清空待确认或已选条目数 */
const pickerHint = computed(() => {
  if (!selectionReady.value) return '请选择要同步的内容'
  if (pickerMode.value === 'include' && pendingPaths.value.length === 0) return '整个分享'
  return `已选 ${pendingPaths.value.length} 项`
})

/**
 * 确认后**替换**对应模型：选择器给出的就是最终路径集合，不再与旧路径合并，
 * 避免已删除的排除项被旧数据合并回来。取消 / 关闭不写任何模型。
 */
function confirmPicker() {
  if (!selectionReady.value) return
  const paths = [...pendingPaths.value]
  pickerVisible.value = false
  if (pickerMode.value === 'exclude') {
    emit('update:excludePaths', paths)
    return
  }
  emit('update:includePaths', paths)
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

.follow-lost-alert {
  margin-bottom: 10px;
}

.follow-lost-desc {
  line-height: 1.5;
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


// ===== 手机端（与全局 mobile.scss 断点一致）=====
// 头部统一：「使用说明」等文字按钮始终在标题行右端；「从分享浏览」独占下一行并撑满，
// 各字段块位置一致。规则输入：类型下拉独占一行，输入框与「添加」同一行等高（32px）。
@media (max-width: 767px) {
  .field-header {
    gap: 6px 8px;
    min-height: 24px;
  }

  // 让操作按钮直接参与 .field-header 的弹性布局，便于分别安排位置
  .path-actions {
    display: contents;
  }

  .path-actions > .el-button {
    margin-left: 0;
  }

  .path-actions > .el-button.is-text {
    padding-left: 4px;
    padding-right: 0;
  }

  // 第一个文字按钮推到行尾，多个文字按钮（测试规则 + 使用说明）依次靠右
  .path-actions > .el-button.is-text:first-of-type {
    margin-left: auto;
  }

  .browse-action {
    order: 10;
    flex: 0 0 100%;
    display: flex;

    :deep(.el-button) {
      flex: 1 1 auto;
      height: 32px;
    }
  }

  .exclude-input-row {
    align-items: flex-start;
    gap: 6px;
  }

  .rule-input-group {
    display: contents;
  }

  .rule-kind-select {
    flex: 0 0 100%;
    width: 100%;
  }

  .rule-kind-select :deep(.el-select__wrapper) {
    min-height: 32px;
  }

  .exclude-input {
    flex: 1 1 0;
  }

  // 单行时 5 + 20 + 5 + 边框 = 32px，与下拉 / 按钮同高；autosize 按同一样式计算多行高度
  .exclude-input :deep(.el-textarea__inner) {
    padding-top: 5px;
    padding-bottom: 5px;
    line-height: 20px;
    // autosize 以行内样式写入单行高度，这里保证单行时不矮于下拉与按钮
    min-height: 32px !important;
  }

  .exclude-input-row > .el-button {
    flex: 0 0 auto;
    height: 32px;
    min-height: 32px;
  }

  // 「存为常用 / 管理常用规则」文字按钮与内容区左缘对齐
  .preset-row {
    gap: 4px 16px;
  }

  .preset-row > .el-button.is-text {
    margin-left: 0;
    padding-left: 0;
    padding-right: 0;
  }
}
</style>
