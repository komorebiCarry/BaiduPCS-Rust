<template>
  <el-dialog
      v-model="visible"
      title="转存分享链接"
      :width="isMobile ? '95%' : '550px'"
      :close-on-click-modal="false"
      @open="handleOpen"
      @close="handleClose"
      :class="{ 'is-mobile': isMobile }"
  >
    <!-- 步骤1: 输入表单 -->
    <template v-if="step === 'input'">
      <el-form
          ref="formRef"
          :model="form"
          :rules="rules"
          label-width="100px"
          @submit.prevent
      >
        <!-- 分享链接 -->
        <el-form-item label="分享链接" prop="shareUrl">
          <el-input
              v-model="form.shareUrl"
              placeholder="请粘贴百度网盘分享链接"
              clearable
              @paste="handlePaste"
          >
            <template #prefix>
              <el-icon><Link /></el-icon>
            </template>
          </el-input>
          <div class="form-tip">
            支持格式: pan.baidu.com/s/xxx 或 pan.baidu.com/share/init?surl=xxx
          </div>
        </el-form-item>

        <!-- 提取码 -->
        <el-form-item label="提取码" prop="password">
          <el-input
              v-model="form.password"
              placeholder="如有提取码请输入（4位）"
              maxlength="4"
              show-word-limit
              clearable
              :class="{ 'password-error': passwordError }"
          >
            <template #prefix>
              <el-icon><Key /></el-icon>
            </template>
          </el-input>
          <div v-if="passwordError" class="error-tip">{{ passwordError }}</div>
        </el-form-item>

        <!-- 保存位置（开启“保持同步”且关闭“转存到网盘”时隐藏，仅本地/分享直下订阅无需网盘目录） -->
        <el-form-item
            v-if="!form.keepSync || form.syncToNetdisk"
            label="保存到"
            prop="savePath"
        >
          <NetdiskPathSelector
              v-model="form.savePath"
              v-model:fs-id="form.saveFsId"
          />
        </el-form-item>

        <!-- 转存后下载（开启“保持同步”时隐藏） -->
        <el-form-item v-if="!form.keepSync" label="转存后下载">
          <el-switch v-model="form.autoDownload" />
          <span class="switch-tip">开启后将自动下载到本地</span>
        </el-form-item>

        <!-- 保持同步：勾选后不是一次性转存，而是在「分享同步」创建一个订阅 -->
        <!-- 从「分享同步」页进入时锁定为开并隐藏开关（该入口只用于创建订阅） -->
        <el-form-item v-if="!lockKeepSync" label="保持同步">
          <el-switch v-model="form.keepSync" />
          <span class="switch-tip">开启后创建订阅，持续监听该分享的新增/变更并自动转存</span>
        </el-form-item>

        <!-- 保持同步展开块：订阅独有字段 -->
        <template v-if="form.keepSync">
          <el-form-item label="订阅名称" prop="syncName">
            <el-input
                v-model="form.syncName"
                placeholder="如：剧集合集同步"
                maxlength="60"
            />
          </el-form-item>
          <el-form-item label="轮询间隔">
            <el-input-number
                v-model="form.syncIntervalSecs"
                :min="600"
                :max="86400"
                :step="300"
            />
            <span class="switch-tip">秒（最小 600 = 10 分钟）</span>
          </el-form-item>
          <el-form-item label="同步范围">
            <ShareIncludeExcludeEditor
                :share-url="form.shareUrl"
                :password="form.password || null"
                :owner-uid="authStore.activeUid ?? null"
                :owner-logged-in="true"
                v-model:include-paths="form.includePaths"
                v-model:exclude-patterns="form.excludePatterns"
            />
          </el-form-item>
          <el-form-item label="转存到网盘">
            <el-switch v-model="form.syncToNetdisk" />
            <span class="switch-tip">把分享内容转存到上方“保存到”的网盘目录</span>
          </el-form-item>
          <el-form-item label="同步到本地">
            <el-switch v-model="form.syncToLocal" />
            <span class="switch-tip">把分享内容下载到本地目录</span>
          </el-form-item>
          <el-form-item v-if="form.syncToLocal" label="本地目录">
            <div class="local-target-row">
              <span
                  class="local-target-value"
                  :class="{ 'is-placeholder': !form.syncLocalPath }"
                  @click="showSyncLocalPicker = true"
              >
                {{ form.syncLocalPath || '点击选择本地目录' }}
              </span>
              <el-button @click="showSyncLocalPicker = true">选择目录</el-button>
            </div>
          </el-form-item>
          <div class="sync-hint">
            不选同步路径＝订阅<strong>整个分享</strong>；选中目录＝该子树（含未来新增）同步，选中文件＝只同步该文件。
            「转存到网盘」「同步到本地」<strong>至少开一个</strong>：只开网盘＝转存到网盘；只开本地＝分享直下到本地；都开＝转存一次后从网盘副本下载。更多高级项可创建后在「分享同步」页编辑。
          </div>
        </template>
      </el-form>
    </template>

    <!-- 步骤2: 文件选择 -->
    <template v-if="step === 'select'">
      <div class="step-back">
        <el-button link type="primary" @click="goBackToInput">
          <el-icon><ArrowLeft /></el-icon>
          返回修改
        </el-button>
      </div>
      <ShareFileSelector
          :files="previewFiles"
          :loading="previewing"
          :share-info="shareInfo"
          :share-url="form.shareUrl"
          :share-password="form.password || undefined"
          @update:selected-fs-ids="handleSelectionChange"
          @update:selected-files="handleSelectedFilesChange"
      />
    </template>

    <!-- 错误提示 -->
    <el-alert
        v-if="errorMessage"
        :title="errorMessage"
        type="error"
        show-icon
        :closable="false"
        class="error-alert"
    />

    <template #footer>
      <div class="dialog-footer">
        <el-button @click="handleClose">取消</el-button>
        <!-- 输入步骤：保持同步时只显示“创建同步订阅”；否则是“选择分享文件/转存全部” -->
        <template v-if="step === 'input'">
          <el-button
              v-if="form.keepSync"
              type="primary"
              :loading="submitting"
              @click="createSyncSubscription"
          >
            {{ submitting ? '创建中...' : '创建同步订阅' }}
          </el-button>
          <template v-else>
            <el-button
                type="primary"
                :loading="previewing"
                :disabled="submitting"
                @click="handlePreview"
            >
              {{ previewing ? '加载中...' : '选择分享文件' }}
            </el-button>
            <el-button
                type="success"
                :loading="submitting"
                :disabled="previewing"
                @click="handleTransferAll"
            >
              {{ submitting ? '转存中...' : '转存全部' }}
            </el-button>
          </template>
        </template>
        <!-- 选择步骤：显示开始转存按钮 -->
        <el-button
            v-if="step === 'select'"
            type="primary"
            :loading="submitting"
            :disabled="selectedFsIds.length === 0"
            @click="handleSubmit"
        >
          {{ submitting ? '转存中...' : '开始转存' }}
        </el-button>
      </div>
    </template>
  </el-dialog>

  <!-- 下载目录选择弹窗 -->
  <FilePickerModal
      v-model="showDownloadPicker"
      mode="download"
      select-type="directory"
      title="选择下载目录"
      :initial-path="downloadConfig?.recent_directory || downloadConfig?.default_directory || downloadConfig?.download_dir"
      :default-download-dir="downloadConfig?.default_directory || downloadConfig?.download_dir"
      @confirm-download="handleConfirmDownload"
      @use-default="handleUseDefaultDownload"
  />

  <!-- 本地同步目标目录选择弹窗（创建订阅时可选本地目标） -->
  <FilePickerModal
      v-model="showSyncLocalPicker"
      mode="download"
      select-type="directory"
      title="选择本地同步目录"
      :initial-path="downloadConfig?.recent_directory || downloadConfig?.default_directory || downloadConfig?.download_dir"
      :default-download-dir="downloadConfig?.default_directory || downloadConfig?.download_dir"
      @confirm-download="handleSyncLocalConfirm"
      @use-default="handleSyncLocalUseDefault"
  />
</template>

<script setup lang="ts">
import { ref, reactive, watch, computed } from 'vue'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'
import { Link, Key, ArrowLeft } from '@element-plus/icons-vue'
import { useIsMobile } from '@/utils/responsive'
import NetdiskPathSelector from './NetdiskPathSelector.vue'
import ShareFileSelector from './ShareFileSelector.vue'
import ShareIncludeExcludeEditor from './ShareIncludeExcludeEditor.vue'
import { FilePickerModal } from '@/components/FilePicker'

// 响应式检测
const isMobile = useIsMobile()
import {
  createTransfer,
  previewShareFiles,
  TransferErrorCodes,
  type CreateTransferRequest,
  type SharedFileInfo,
  type PreviewShareInfo
} from '@/api/transfer'
import {
  getTransferConfig,
  getConfig,
  updateRecentDirDebounced,
  setDefaultDownloadDir,
  type TransferConfig,
  type DownloadConfig
} from '@/api/config'
import { createSubscription, type CreateShareSubscriptionRequest, type SyncTarget } from '@/api/shareSync'
import { useAuthStore } from '@/stores/auth'

const authStore = useAuthStore()

const props = defineProps<{
  modelValue: boolean
  currentPath?: string    // FilesView 当前浏览的目录路径
  currentFsId?: number    // FilesView 当前浏览的目录 fs_id
  defaultKeepSync?: boolean  // 打开时是否默认勾选“保持同步”（分享同步页入口）
  lockKeepSync?: boolean     // 锁定“保持同步”为开并隐藏开关（分享同步页「新增」入口专用）
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'success': [taskId: string]
  'sync-created': [subscriptionId: string]
}>()

// 对话框可见性
const visible = computed({
  get: () => props.modelValue,
  set: (val) => emit('update:modelValue', val),
})

// 表单引用
const formRef = ref<FormInstance>()

// 表单数据
const form = reactive({
  shareUrl: '',
  password: '',
  savePath: '/',
  saveFsId: 0,
  autoDownload: false,
  // 保持同步（创建订阅）
  keepSync: false,
  syncName: '',
  syncIntervalSecs: 1800,
  includePaths: [] as string[],
  excludePatterns: [] as string[],
  // 订阅目标开关（至少开一个）：转存到网盘 / 同步到本地
  syncToNetdisk: true,
  syncToLocal: false,
  syncLocalPath: '',
})

// 对话框步骤状态
const step = ref<'input' | 'select'>('input')

// 预览相关状态
const previewing = ref(false)
const previewFiles = ref<SharedFileInfo[]>([])
const selectedFsIds = ref<number[]>([])
const selectedFiles = ref<SharedFileInfo[]>([])
const shareInfo = ref<PreviewShareInfo | null>(null)

// 状态
const submitting = ref(false)
const errorMessage = ref('')
const passwordError = ref('')
const transferConfig = ref<TransferConfig | null>(null)
const downloadConfig = ref<DownloadConfig | null>(null)
const showDownloadPicker = ref(false)
const showSyncLocalPicker = ref(false)
const transferAllMode = ref(false)

// 表单验证规则
const rules: FormRules = {
  shareUrl: [
    { required: true, message: '请输入分享链接', trigger: 'blur' },
    {
      validator: (_, value, callback) => {
        if (!value) {
          callback()
          return
        }
        if (!value.includes('pan.baidu.com')) {
          callback(new Error('请输入有效的百度网盘分享链接'))
          return
        }
        callback()
      },
      trigger: 'blur'
    }
  ],
  password: [
    {
      validator: (_, value, callback) => {
        if (value && value.length !== 4) {
          callback(new Error('提取码必须是4位'))
          return
        }
        callback()
      },
      trigger: 'blur'
    }
  ],
  savePath: [
    {
      validator: (_, value, callback) => {
        // 仅本地/分享直下订阅（保持同步且关闭转存到网盘）无需网盘目录
        if (form.keepSync && !form.syncToNetdisk) {
          callback()
          return
        }
        if (!value || !String(value).trim()) {
          callback(new Error('请选择保存位置'))
          return
        }
        callback()
      },
      trigger: 'change'
    }
  ],
  syncName: [
    {
      validator: (_, value, callback) => {
        if (form.keepSync && (!value || !String(value).trim())) {
          callback(new Error('请填写订阅名称'))
          return
        }
        callback()
      },
      trigger: 'blur'
    }
  ]
}

// 对话框打开时初始化
async function handleOpen() {
  errorMessage.value = ''
  passwordError.value = ''

  // 不依赖配置请求的默认值先设好（保证锁定/默认“保持同步”即使配置加载失败也生效）
  form.keepSync = props.lockKeepSync ? true : (props.defaultKeepSync ?? false)
  form.syncName = ''
  form.syncIntervalSecs = 1800
  form.includePaths = []
  form.excludePatterns = []

  try {
    const [transferCfg, appConfig] = await Promise.all([
      getTransferConfig(),
      getConfig()
    ])

    transferConfig.value = transferCfg
    downloadConfig.value = appConfig.download

    form.autoDownload = transferConfig.value?.default_behavior === 'transfer_and_download'
    await setDefaultSavePath()
  } catch (error) {
    console.error('加载转存配置失败:', error)
    setCurrentDirAsDefault()
  }
}

async function setDefaultSavePath() {
  if (transferConfig.value?.recent_save_fs_id && transferConfig.value?.recent_save_path) {
    form.saveFsId = transferConfig.value.recent_save_fs_id
    form.savePath = transferConfig.value.recent_save_path
    return
  }
  setCurrentDirAsDefault()
}

function setCurrentDirAsDefault() {
  if (props.currentPath) {
    form.savePath = props.currentPath
    form.saveFsId = props.currentFsId || 0
  } else {
    form.savePath = '/'
    form.saveFsId = 0
  }
}

// 对话框关闭时重置所有状态
function handleClose() {
  visible.value = false
  form.shareUrl = ''
  form.password = ''
  form.savePath = '/'
  form.saveFsId = 0
  form.autoDownload = false
  form.keepSync = false
  form.syncName = ''
  form.syncIntervalSecs = 1800
  form.includePaths = []
  form.excludePatterns = []
  form.syncToNetdisk = true
  form.syncToLocal = false
  form.syncLocalPath = ''
  errorMessage.value = ''
  passwordError.value = ''
  // 重置文件选择状态
  step.value = 'input'
  previewFiles.value = []
  selectedFsIds.value = []
  selectedFiles.value = []
  shareInfo.value = null
  transferAllMode.value = false
  formRef.value?.resetFields()
}

// 返回输入步骤
function goBackToInput() {
  step.value = 'input'
  errorMessage.value = ''
}

// 处理文件选择变化
function handleSelectionChange(fsIds: number[]) {
  selectedFsIds.value = fsIds
}

// 处理选中文件完整信息变化
function handleSelectedFilesChange(files: SharedFileInfo[]) {
  selectedFiles.value = files
}

// 处理粘贴事件，自动提取提取码
function handlePaste(event: ClipboardEvent) {
  const pastedText = event.clipboardData?.getData('text') || ''
  const pwdMatch = pastedText.match(/(?:提取码[：:]\s*|pwd=)([a-zA-Z0-9]{4})/)
  if (pwdMatch) {
    form.password = pwdMatch[1]
  }
}

// 预览文件列表（只验证 shareUrl 和 password，不验证 savePath）
async function handlePreview() {
  try {
    await formRef.value?.validateField(['shareUrl', 'password'])
  } catch {
    return
  }

  previewing.value = true
  errorMessage.value = ''
  passwordError.value = ''

  try {
    const response = await previewShareFiles({
      share_url: form.shareUrl.trim(),
      password: form.password || undefined,
    })

    previewFiles.value = response.files
    shareInfo.value = response.share_info || null
    step.value = 'select'
  } catch (error: any) {
    handlePreviewError(error)
  } finally {
    previewing.value = false
  }
}

// 处理预览错误
function handlePreviewError(error: any) {
  const code = error.code as number
  const message = error.message as string

  switch (code) {
    case TransferErrorCodes.NEED_PASSWORD:
      if (form.password && form.password.trim().length > 0) {
        passwordError.value = '提取码可能不正确，请检查后重新输入'
      } else {
        passwordError.value = '该分享需要提取码，请输入'
      }
      break
    case TransferErrorCodes.INVALID_PASSWORD:
      passwordError.value = '提取码错误，请重新输入'
      form.password = ''
      break
    case TransferErrorCodes.SHARE_EXPIRED:
      errorMessage.value = '分享链接已失效'
      break
    case TransferErrorCodes.SHARE_NOT_FOUND:
      errorMessage.value = '分享链接不存在或已被删除'
      break
    case TransferErrorCodes.MANAGER_NOT_READY:
      errorMessage.value = '转存服务未就绪，请先登录'
      break
    default:
      if (message && (message.includes('timeout') || message.includes('网络错误'))) {
        errorMessage.value = '预览超时，网络可能不稳定，请稍后重试'
      } else {
        errorMessage.value = message || '预览失败，请稍后重试'
      }
  }
}

// 保持同步：创建订阅（而非一次性转存）
// include_paths / exclude_patterns 由 ShareIncludeExcludeEditor 收集，
// 其树选择器产出的是“分享根相对路径”命名空间（与后端 snapshot 匹配一致），
// 不填则同步整个分享。
async function createSyncSubscription() {
  // 复用表单校验（分享链接/提取码/保存位置 + 订阅名称）
  try {
    await formRef.value?.validate()
  } catch {
    return
  }

  const name = form.syncName.trim()
  const interval = Number(form.syncIntervalSecs)
  if (!Number.isFinite(interval) || interval < 600) {
    ElMessage.error('轮询间隔不能小于 600 秒')
    return
  }
  if (!form.syncToNetdisk && !form.syncToLocal) {
    ElMessage.error('请至少开启“转存到网盘”或“同步到本地”其中一个')
    return
  }
  if (form.syncToLocal && !form.syncLocalPath.trim()) {
    ElMessage.error('已开启“同步到本地”，请选择本地目录')
    return
  }

  submitting.value = true
  errorMessage.value = ''
  try {
    const req: CreateShareSubscriptionRequest = {
      name,
      share_url: form.shareUrl.trim(),
      password: form.password || null,
      include_paths: form.includePaths,
      exclude_patterns: form.excludePatterns,
      targets: buildSyncTargets(),
      poll_config: {
        enabled: true,
        mode: 'interval',
        interval_secs: interval,
        schedule_hour: null,
        schedule_minute: null,
      },
      // 显式归属当前活跃账号（缺省由后端回退）
      owner_uid: authStore.activeUid ?? undefined,
    }
    const sub = await createSubscription(req)
    ElMessage.success('已创建同步订阅，可在「分享同步」页管理')
    emit('sync-created', sub.id)
    handleClose()
  } catch (e: unknown) {
    const ax = e as { response?: { data?: { message?: string; error?: string; msg?: string } }; message?: string }
    errorMessage.value =
        ax?.response?.data?.message ||
        ax?.response?.data?.error ||
        ax?.response?.data?.msg ||
        ax?.message ||
        '创建订阅失败'
  } finally {
    submitting.value = false
  }
}

// 提交转存（选择文件后）
async function handleSubmit() {
  if (form.autoDownload && downloadConfig.value?.ask_each_time) {
    showDownloadPicker.value = true
    return
  }
  await executeTransfer()
}

// 转存全部（不经过文件选择，直接转存所有文件）
async function handleTransferAll() {
  try {
    await formRef.value?.validate()
  } catch {
    return
  }

  if (form.autoDownload && downloadConfig.value?.ask_each_time) {
    // 标记为转存全部模式，在下载目录确认后执行
    transferAllMode.value = true
    showDownloadPicker.value = true
    return
  }
  await executeTransfer(undefined, true)
}

// 执行转存任务
async function executeTransfer(localDownloadPath?: string, transferAll: boolean = false) {
  submitting.value = true
  errorMessage.value = ''

  try {
    const request: CreateTransferRequest = {
      share_url: form.shareUrl.trim(),
      password: form.password || undefined,
      save_path: form.savePath,
      save_fs_id: form.saveFsId,
      auto_download: form.autoDownload,
      local_download_path: localDownloadPath,
      selected_fs_ids: transferAll ? undefined : (selectedFsIds.value.length > 0 ? selectedFsIds.value : undefined),
      selected_files: transferAll ? undefined : (selectedFiles.value.length > 0 ? selectedFiles.value : undefined),
    }

    const response = await createTransfer(request)

    if (response.task_id) {
      ElMessage.success('转存任务创建成功')
      emit('success', response.task_id)
      handleClose()
    }
  } catch (error: any) {
    handleTransferError(error)
  } finally {
    submitting.value = false
  }
}

// 处理下载目录确认
async function handleConfirmDownload(payload: { path: string; setAsDefault: boolean }) {
  const { path, setAsDefault } = payload
  showDownloadPicker.value = false

  if (setAsDefault) {
    try {
      await setDefaultDownloadDir({ path })
      if (downloadConfig.value) {
        downloadConfig.value.default_directory = path
      }
    } catch (error: any) {
      console.error('设置默认下载目录失败:', error)
    }
  }

  updateRecentDirDebounced({ dir_type: 'download', path })
  if (downloadConfig.value) {
    downloadConfig.value.recent_directory = path
  }

  const isTransferAll = transferAllMode.value
  transferAllMode.value = false
  await executeTransfer(path, isTransferAll)
}

// 本地同步目标：选定目录（path 原样，不做归一化，保留 Windows D:\ 等原样交后端）
function handleSyncLocalConfirm(payload: { path: string; setAsDefault: boolean }) {
  showSyncLocalPicker.value = false
  form.syncLocalPath = String(payload.path || '').trim()
}
function handleSyncLocalUseDefault() {
  showSyncLocalPicker.value = false
  form.syncLocalPath = String(
      downloadConfig.value?.default_directory || downloadConfig.value?.download_dir || ''
  ).trim()
}

// 构造订阅目标：按“转存到网盘/同步到本地”两个开关推导
//   仅网盘     = 仅 Netdisk target
//   网盘+本地  = Netdisk + Local{ mode: transfer_and_download }（后端合并成一条腿：转存一次→从副本下载）
//   仅本地     = 仅 Local{ mode: share_direct }（分享直下：转临时目录→下载→清理）
function buildSyncTargets(): SyncTarget[] {
  const targets: SyncTarget[] = []
  if (form.syncToNetdisk) {
    targets.push({ kind: 'netdisk', remote_path: form.savePath, save_fs_id: form.saveFsId })
  }
  if (form.syncToLocal) {
    // 有网盘目标＝转存并下载（复用网盘副本）；无＝分享直下。后端最终按目标存在性推导，这里保持一致。
    targets.push({
      kind: 'local',
      local_path: form.syncLocalPath.trim(),
      mode: form.syncToNetdisk ? 'transfer_and_download' : 'share_direct',
    })
  }
  return targets
}

// 处理使用默认目录下载
async function handleUseDefaultDownload() {
  showDownloadPicker.value = false
  const targetDir = downloadConfig.value?.default_directory || downloadConfig.value?.download_dir || 'downloads'

  const isTransferAll = transferAllMode.value
  transferAllMode.value = false
  await executeTransfer(targetDir, isTransferAll)
}

// 处理转存错误
function handleTransferError(error: any) {
  const code = error.code as number
  const message = error.message as string

  switch (code) {
    case TransferErrorCodes.NEED_PASSWORD:
      if (form.password && form.password.trim().length > 0) {
        passwordError.value = '提取码可能不正确，请检查后重新输入'
      } else {
        passwordError.value = '该分享需要提取码，请输入'
      }
      break
    case TransferErrorCodes.INVALID_PASSWORD:
      passwordError.value = '提取码错误，请重新输入'
      form.password = ''
      break
    case TransferErrorCodes.SHARE_EXPIRED:
      errorMessage.value = '分享链接已失效'
      break
    case TransferErrorCodes.SHARE_NOT_FOUND:
      errorMessage.value = '分享链接不存在或已被删除'
      break
    case TransferErrorCodes.MANAGER_NOT_READY:
      errorMessage.value = '转存服务未就绪，请先登录'
      break
    default:
      errorMessage.value = message || '转存失败，请稍后重试'
  }
}

// 监听 savePath 变化，清除错误
watch(() => form.savePath, () => {
  if (errorMessage.value) {
    errorMessage.value = ''
  }
})

// 监听 password 变化，清除密码错误
watch(() => form.password, () => {
  if (passwordError.value) {
    passwordError.value = ''
  }
})
</script>

<style scoped lang="scss">
.form-tip {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-top: 4px;
  line-height: 1.4;
}

.error-tip {
  font-size: 12px;
  color: var(--el-color-danger);
  margin-top: 4px;
}

.password-error {
  :deep(.el-input__wrapper) {
    box-shadow: 0 0 0 1px var(--el-color-danger) inset;
  }
}

.switch-tip {
  margin-left: 12px;
  font-size: 13px;
  color: var(--el-text-color-secondary);
}

.sync-hint {
  margin: 0 0 8px 100px;
  font-size: 12px;
  line-height: 1.5;
  color: var(--el-text-color-secondary);
}

.local-target-row {
  display: flex;
  align-items: center;
  gap: 8px;
  width: 100%;

  .local-target-value {
    flex: 1;
    min-width: 0;
    padding: 0 11px;
    height: 32px;
    line-height: 32px;
    border: 1px solid var(--el-border-color);
    border-radius: 4px;
    font-size: 14px;
    color: var(--el-text-color-primary);
    cursor: pointer;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    &:hover { border-color: var(--el-color-primary); }
    &.is-placeholder { color: var(--el-text-color-placeholder); }
  }
}

.step-back {
  margin-bottom: 12px;
}

.error-alert {
  margin-top: 16px;
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
}

/* 移动端样式适配 */
@media (max-width: 767px) {
  .is-mobile :deep(.el-form-item__label) {
    font-size: 14px;
  }

  .is-mobile :deep(.el-input__inner) {
    font-size: 15px;
  }

  .dialog-footer {
    flex-direction: column;

    .el-button {
      width: 100%;
    }
  }

  .form-tip {
    font-size: 11px;
  }

  .switch-tip {
    font-size: 12px;
  }
}
</style>
