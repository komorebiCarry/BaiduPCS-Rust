<template>
  <el-dialog
      v-model="visible"
      title="分享直下"
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

        <!-- 下载目录 -->
        <el-form-item label="下载到" prop="localDownloadPath">
          <el-input
              v-model="form.localDownloadPath"
              placeholder="选择本地下载目录"
              readonly
              @click="showDownloadPicker = true"
          >
            <template #prefix>
              <el-icon><Folder /></el-icon>
            </template>
            <template #suffix>
              <el-button link type="primary" @click.stop="showDownloadPicker = true">
                选择
              </el-button>
            </template>
          </el-input>
        </el-form-item>

        <!-- 说明 -->
        <el-alert
            title="分享直下说明"
            type="info"
            :closable="false"
            class="info-alert"
        >
          <template #default>
            <div class="info-content">
              分享直下会自动将文件转存到网盘临时目录，下载完成后自动清理临时文件。
            </div>
          </template>
        </el-alert>
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
        <!-- 输入步骤：显示"选择分享文件"和"直下全部"按钮 -->
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
              @click="handleDirectDownloadAll"
          >
            {{ submitting ? '处理中...' : '直下全部' }}
          </el-button>
        </template>
        <!-- 选择步骤：显示开始下载按钮 -->
        <el-button
            v-if="step === 'select'"
            type="primary"
            :loading="submitting"
            :disabled="selectedFsIds.length === 0"
            @click="handleSubmit"
        >
          {{ submitting ? '处理中...' : '开始下载' }}
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
import { Link, Key, Folder, ArrowLeft } from '@element-plus/icons-vue'
import { useIsMobile } from '@/utils/responsive'
import ShareFileSelector from './ShareFileSelector.vue'
import { FilePickerModal } from '@/components/FilePicker'
import {
  createTransfer,
  previewShareFiles,
  TransferErrorCodes,
  type CreateTransferRequest,
  type SharedFileInfo,
  type PreviewShareInfo
} from '@/api/transfer'
import {
  getConfig,
  updateRecentDirDebounced,
  setDefaultDownloadDir,
  type DownloadConfig
} from '@/api/config'

// 响应式检测
const isMobile = useIsMobile()

const props = defineProps<{
  modelValue: boolean
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
  localDownloadPath: '',
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
const downloadConfig = ref<DownloadConfig | null>(null)
const showDownloadPicker = ref(false)

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
  localDownloadPath: [
    { required: true, message: '请选择下载目录', trigger: 'change' }
  ]
}

// 对话框打开时初始化
async function handleOpen() {
  errorMessage.value = ''
  passwordError.value = ''

  try {
    const appConfig = await getConfig()
    downloadConfig.value = appConfig.download

    form.localDownloadPath = downloadConfig.value?.default_directory
        || downloadConfig.value?.download_dir
        || 'downloads'
  } catch (error) {
    console.error('加载下载配置失败:', error)
    form.localDownloadPath = 'downloads'
  }
}

// 对话框关闭时重置所有状态
function handleClose() {
  visible.value = false
  form.shareUrl = ''
  form.password = ''
  form.localDownloadPath = ''
  errorMessage.value = ''
  passwordError.value = ''
  // 重置文件选择状态
  step.value = 'input'
  previewFiles.value = []
  selectedFsIds.value = []
  selectedFiles.value = []
  shareInfo.value = null
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

// 预览文件列表（只验证 shareUrl 和 password，不验证 localDownloadPath）
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
      errorMessage.value = '服务未就绪，请先登录'
      break
    default:
      if (message && (message.includes('timeout') || message.includes('网络错误'))) {
        errorMessage.value = '预览超时，网络可能不稳定，请稍后重试'
      } else {
        errorMessage.value = message || '预览失败，请稍后重试'
      }
  }
}

// 提交
async function handleSubmit() {
  await executeTransfer()
}

// 直下全部（不经过文件选择，直接下载所有文件）
async function handleDirectDownloadAll() {
  try {
    await formRef.value?.validate()
  } catch {
    return
  }
  await executeTransfer(true)
}

// 执行分享直下任务
async function executeTransfer(downloadAll: boolean = false) {
  submitting.value = true
  errorMessage.value = ''

  try {
    const request: CreateTransferRequest = {
      share_url: form.shareUrl.trim(),
      password: form.password || undefined,
      save_fs_id: 0,
      auto_download: true,
      local_download_path: form.localDownloadPath,
      is_share_direct_download: true,
      selected_fs_ids: downloadAll ? undefined : (selectedFsIds.value.length > 0 ? selectedFsIds.value : undefined),
      selected_files: downloadAll ? undefined : (selectedFiles.value.length > 0 ? selectedFiles.value : undefined),
    }

    const response = await createTransfer(request)

    if (response.task_id) {
      ElMessage.success('分享直下任务创建成功')
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

  form.localDownloadPath = path

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
}

// 处理使用默认目录下载
function handleUseDefaultDownload() {
  showDownloadPicker.value = false
  form.localDownloadPath = downloadConfig.value?.default_directory
      || downloadConfig.value?.download_dir
      || 'downloads'
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
      errorMessage.value = '服务未就绪，请先登录'
      break
    case TransferErrorCodes.INSUFFICIENT_SPACE:
      errorMessage.value = '网盘空间不足，请清理后重试'
      break
    case TransferErrorCodes.TRANSFER_FAILED:
      errorMessage.value = '转存失败，请稍后重试'
      break
    case TransferErrorCodes.DOWNLOAD_FAILED:
      errorMessage.value = '下载失败，请稍后重试'
      break
    default:
      errorMessage.value = message || '操作失败，请稍后重试'
  }
}

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

.info-alert {
  margin-top: 16px;

  .info-content {
    font-size: 12px;
    color: var(--el-text-color-secondary);
    line-height: 1.5;
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
}
</style>
