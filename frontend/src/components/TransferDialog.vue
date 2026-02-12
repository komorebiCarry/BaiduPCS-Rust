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

        <!-- 保存位置 -->
        <el-form-item label="保存到" prop="savePath">
          <NetdiskPathSelector
              v-model="form.savePath"
              v-model:fs-id="form.saveFsId"
          />
        </el-form-item>

        <!-- 转存后下载 -->
        <el-form-item label="转存后下载">
          <el-switch v-model="form.autoDownload" />
          <span class="switch-tip">开启后将自动下载到本地</span>
        </el-form-item>
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
        <!-- 输入步骤：显示"选择分享文件"和"转存全部"按钮 -->
        <template v-if="step === 'input'">
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
</template>

<script setup lang="ts">
import { ref, reactive, watch, computed } from 'vue'
import { ElMessage, type FormInstance, type FormRules } from 'element-plus'
import { Link, Key, ArrowLeft } from '@element-plus/icons-vue'
import { useIsMobile } from '@/utils/responsive'
import NetdiskPathSelector from './NetdiskPathSelector.vue'
import ShareFileSelector from './ShareFileSelector.vue'
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

const props = defineProps<{
  modelValue: boolean
  currentPath?: string    // FilesView 当前浏览的目录路径
  currentFsId?: number    // FilesView 当前浏览的目录 fs_id
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
  'success': [taskId: string]
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
    { required: true, message: '请选择保存位置', trigger: 'change' }
  ]
}

// 对话框打开时初始化
async function handleOpen() {
  errorMessage.value = ''
  passwordError.value = ''

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
