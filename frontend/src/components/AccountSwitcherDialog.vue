<template>
  <el-dialog
      v-model="visible"
      title="百度账号"
      width="680px"
      class="account-switcher-dialog"
      :close-on-click-modal="false"
      @closed="handleClosed"
  >
    <div class="account-toolbar">
      <el-segmented v-model="mode" :options="modeOptions" @change="handleModeChange" />
      <el-button :icon="Refresh" circle :loading="accountsLoading" @click="refreshAccounts" />
    </div>

    <div v-if="mode === 'list'" class="account-list">
      <el-empty v-if="!authStore.accounts.length" description="暂无已保存账号" />

      <div
          v-for="account in authStore.accounts"
          :key="account.uid"
          class="account-row"
          :class="{ active: account.is_active }"
      >
        <el-avatar :size="44" :src="account.avatar_url">
          <el-icon><User /></el-icon>
        </el-avatar>

        <div class="account-body">
          <div class="account-title">
            <span class="account-name">{{ displayName(account) }}</span>
            <el-tag v-if="account.is_active" size="small" type="success">当前</el-tag>
            <el-tag v-if="account.vip_type === 2" size="small" type="warning">SVIP</el-tag>
            <el-tag v-else-if="account.vip_type === 1" size="small" type="warning">VIP</el-tag>
          </div>

          <div class="account-meta">
            <span>UID {{ account.uid }}</span>
            <span>{{ formatLoginTime(account.login_time) }}</span>
            <span v-if="account.total_space">{{ formatSpace(account.used_space, account.total_space) }}</span>
          </div>

          <div class="account-tags">
            <el-tag size="small" :type="account.has_ptoken ? 'success' : 'info'" effect="plain">
              {{ account.has_ptoken ? 'PTOKEN' : '无 PTOKEN' }}
            </el-tag>
            <el-tag size="small" :type="account.is_warmed_up ? 'success' : 'warning'" effect="plain">
              {{ account.is_warmed_up ? '已预热' : '未预热' }}
            </el-tag>
          </div>
        </div>

        <div class="account-actions">
          <el-button
              v-if="!account.is_active"
              type="primary"
              plain
              :loading="switchingUid === account.uid"
              @click="switchToAccount(account.uid)"
          >
            切换
          </el-button>
          <el-button
              :icon="Delete"
              circle
              type="danger"
              plain
              :loading="removingUid === account.uid"
              @click="removeSavedAccount(account)"
          />
        </div>
      </div>
    </div>

    <div v-else-if="mode === 'qrcode'" class="qr-add-panel">
      <div class="qr-frame">
        <div v-if="qrLoading" class="qr-placeholder">
          <el-icon class="is-loading" :size="32"><Loading /></el-icon>
          <span>生成中</span>
        </div>

        <div v-else-if="qrError" class="qr-placeholder error">
          <el-icon :size="36"><CircleClose /></el-icon>
          <span>{{ qrError }}</span>
          <el-button type="primary" plain @click="startQrLogin">重试</el-button>
        </div>

        <template v-else-if="qrcodeUrl">
          <img :src="qrcodeUrl" alt="添加账号二维码" />
          <div v-if="qrScanned" class="qr-mask">
            <el-icon :size="42"><SuccessFilled /></el-icon>
            <span>等待确认</span>
          </div>
          <div v-else-if="qrExpired" class="qr-mask">
            <el-icon :size="42"><RefreshRight /></el-icon>
            <span>已过期</span>
            <el-button type="primary" plain @click="startQrLogin">刷新</el-button>
          </div>
        </template>
      </div>

      <div class="qr-status">
        <span>{{ qrScanned ? '手机端确认后完成添加' : '使用百度网盘 App 扫码' }}</span>
        <el-tag v-if="!qrExpired" size="small" effect="plain">{{ countdown }}s</el-tag>
      </div>
    </div>

    <div v-else class="cookie-add-panel">
      <el-input
          v-model="cookieInput"
          type="textarea"
          :rows="6"
          placeholder="BDUSS=xxxx; PTOKEN=yyyy; STOKEN=zzzz; BAIDUID=aaaa"
          resize="none"
          :disabled="cookieLoading"
      />
      <div v-if="cookieError" class="cookie-error">
        <el-icon><CircleClose /></el-icon>
        <span>{{ cookieError }}</span>
      </div>
      <el-button
          type="primary"
          :icon="Key"
          :loading="cookieLoading"
          :disabled="!cookieInput.trim()"
          @click="addCookieAccount"
      >
        添加账号
      </el-button>
    </div>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  CircleClose,
  Delete,
  Key,
  Loading,
  Refresh,
  RefreshRight,
  SuccessFilled,
  User,
} from '@element-plus/icons-vue'
import { useAuthStore } from '@/stores/auth'
import type { AccountSummary } from '@/api/auth'

const props = defineProps<{
  modelValue: boolean
}>()

const emit = defineEmits<{
  (event: 'update:modelValue', value: boolean): void
}>()

const router = useRouter()
const authStore = useAuthStore()

const visible = computed({
  get: () => props.modelValue,
  set: (value: boolean) => emit('update:modelValue', value),
})

const mode = ref<'list' | 'qrcode' | 'cookie'>('list')
const modeOptions = [
  { label: '账号列表', value: 'list' },
  { label: '扫码添加', value: 'qrcode' },
  { label: 'Cookie 添加', value: 'cookie' },
]

const accountsLoading = ref(false)
const switchingUid = ref<number | null>(null)
const removingUid = ref<number | null>(null)

const qrLoading = ref(false)
const qrError = ref('')
const qrScanned = ref(false)
const qrExpired = ref(false)
const countdown = ref(120)
let countdownTimer: number | null = null

const cookieInput = ref('')
const cookieLoading = ref(false)
const cookieError = ref('')

const qrcodeUrl = computed(() => authStore.qrcode?.image_base64 || '')

watch(
    () => props.modelValue,
    async (open) => {
      if (open) {
        mode.value = 'list'
        await refreshAccounts()
      } else {
        cleanupQrPolling()
      }
    }
)

async function refreshAccounts() {
  accountsLoading.value = true
  try {
    await authStore.fetchAccounts()
  } catch (error: any) {
    ElMessage.error(error?.message || '获取账号列表失败')
  } finally {
    accountsLoading.value = false
  }
}

function handleModeChange(value: string | number | boolean) {
  if (value === 'qrcode') {
    startQrLogin()
  } else {
    cleanupQrPolling()
  }

  cookieError.value = ''
}

async function startQrLogin() {
  cleanupQrPolling()
  qrLoading.value = true
  qrError.value = ''
  qrScanned.value = false
  qrExpired.value = false

  try {
    await authStore.generateQRCode()
    authStore.startPolling(
        () => {
          stopCountdown()
          qrScanned.value = false
          qrExpired.value = false
          mode.value = 'list'
          ElMessage.success('账号已添加')
        },
        (error: any) => {
          qrError.value = error?.message || '添加失败'
          stopCountdown()
        },
        () => {
          qrScanned.value = true
        },
        true
    )
    startCountdown()
  } catch (error: any) {
    qrError.value = error?.message || '二维码生成失败'
  } finally {
    qrLoading.value = false
  }
}

function startCountdown() {
  countdown.value = 120
  qrExpired.value = false
  stopCountdown()

  countdownTimer = window.setInterval(() => {
    countdown.value -= 1
    if (countdown.value <= 0) {
      qrExpired.value = true
      authStore.stopPolling()
      stopCountdown()
    }
  }, 1000)
}

function stopCountdown() {
  if (countdownTimer) {
    clearInterval(countdownTimer)
    countdownTimer = null
  }
}

function cleanupQrPolling() {
  authStore.stopPolling()
  stopCountdown()
  qrLoading.value = false
  qrError.value = ''
  qrScanned.value = false
  qrExpired.value = false
}

async function addCookieAccount() {
  cookieError.value = ''
  if (!cookieInput.value.trim()) return

  cookieLoading.value = true
  try {
    const result = await authStore.loginWithCookies(cookieInput.value.trim())
    cookieInput.value = ''
    mode.value = 'list'

    if (result.message && !result.message.includes('预热完成')) {
      ElMessage({
        type: 'warning',
        message: result.message,
        duration: 8000,
        showClose: true,
      })
    } else {
      ElMessage.success('账号已添加')
    }
  } catch (error: any) {
    cookieError.value = error?.message || 'Cookie 登录失败'
  } finally {
    cookieLoading.value = false
  }
}

async function switchToAccount(uid: number) {
  switchingUid.value = uid
  try {
    await authStore.switchAccount(uid)
    ElMessage.success('账号已切换')
    visible.value = false
  } catch (error: any) {
    ElMessage.error(error?.message || '切换账号失败')
  } finally {
    switchingUid.value = null
  }
}

async function removeSavedAccount(account: AccountSummary) {
  try {
    await ElMessageBox.confirm(
        `确定移除账号 ${displayName(account)} 吗？`,
        '移除账号',
        {
          confirmButtonText: '移除',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )
  } catch {
    return
  }

  removingUid.value = account.uid
  try {
    const result = await authStore.removeAccount(account.uid)
    ElMessage.success('账号已移除')

    if (!result.active_user) {
      visible.value = false
      await router.push('/login')
    }
  } catch (error: any) {
    ElMessage.error(error?.message || '移除账号失败')
  } finally {
    removingUid.value = null
  }
}

function handleClosed() {
  mode.value = 'list'
  cleanupQrPolling()
  cookieError.value = ''
}

function displayName(account: AccountSummary) {
  return account.nickname || account.username || String(account.uid)
}

function formatLoginTime(timestamp: number) {
  if (!timestamp) return '未知时间'
  return new Date(timestamp * 1000).toLocaleString()
}

function formatSpace(used?: number, total?: number) {
  if (!total) return ''
  return `${formatBytes(used || 0)} / ${formatBytes(total)}`
}

function formatBytes(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
  let size = value
  let index = 0
  while (size >= 1024 && index < units.length - 1) {
    size /= 1024
    index += 1
  }
  return `${size.toFixed(size >= 10 || index === 0 ? 0 : 1)} ${units[index]}`
}

onBeforeUnmount(() => {
  cleanupQrPolling()
})
</script>

<style scoped lang="scss">
.account-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 16px;
}

.account-list {
  display: flex;
  flex-direction: column;
  gap: 10px;
  max-height: min(60vh, 520px);
  overflow-y: auto;
  padding-right: 2px;
}

.account-row {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 12px;
  border: 1px solid #e4e7ed;
  border-radius: 8px;
  background: #fff;

  &.active {
    border-color: #67c23a;
    background: #f0f9eb;
  }
}

.account-body {
  flex: 1;
  min-width: 0;
}

.account-title,
.account-meta,
.account-tags,
.account-actions,
.qr-status {
  display: flex;
  align-items: center;
}

.account-title {
  gap: 8px;
  margin-bottom: 6px;
}

.account-name {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-weight: 600;
  color: #303133;
}

.account-meta {
  flex-wrap: wrap;
  gap: 6px 10px;
  color: #606266;
  font-size: 12px;
  line-height: 1.5;
  margin-bottom: 6px;
}

.account-tags {
  gap: 6px;
}

.account-actions {
  flex-shrink: 0;
  gap: 8px;
}

.qr-add-panel,
.cookie-add-panel {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 14px;
}

.qr-frame {
  position: relative;
  width: 240px;
  height: 240px;
  border: 1px solid #e4e7ed;
  border-radius: 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  background: #fff;

  img {
    width: 220px;
    height: 220px;
    display: block;
  }
}

.qr-placeholder,
.qr-mask {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 10px;
  color: #606266;
}

.qr-placeholder.error {
  color: #f56c6c;
  text-align: center;
  padding: 16px;
}

.qr-mask {
  position: absolute;
  inset: 0;
  background: rgba(48, 49, 51, 0.72);
  color: white;
}

.qr-status {
  justify-content: center;
  gap: 10px;
  color: #606266;
  font-size: 14px;
}

.cookie-add-panel {
  align-items: stretch;
}

.cookie-error {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #f56c6c;
  font-size: 13px;
}

@media (max-width: 767px) {
  :deep(.account-switcher-dialog) {
    width: calc(100vw - 24px) !important;
  }

  .account-toolbar {
    align-items: stretch;
  }

  .account-row {
    align-items: flex-start;
  }

  .account-actions {
    flex-direction: column;
  }
}
</style>
