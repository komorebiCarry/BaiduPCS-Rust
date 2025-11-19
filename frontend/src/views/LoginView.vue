<template>
  <div class="login-container">
    <div class="login-card">
      <!-- Logo 和标题 -->
      <div class="header">
        <div class="logo">
          <el-icon :size="48" color="#409eff">
            <FolderOpened />
          </el-icon>
        </div>
        <h1>百度网盘 Rust 客户端</h1>
        <p class="subtitle">Baidu Netdisk Rust Client</p>
      </div>

      <!-- 二维码区域 -->
      <div class="qrcode-section">
        <div v-if="loading" class="loading">
          <el-icon class="is-loading" :size="32">
            <Loading />
          </el-icon>
          <p>正在生成二维码...</p>
        </div>

        <div v-else-if="error" class="error-state">
          <el-icon :size="48" color="#f56c6c">
            <CircleClose />
          </el-icon>
          <p class="error-text">{{ error }}</p>
          <el-button type="primary" @click="refreshQRCode">
            <el-icon><Refresh /></el-icon>
            重新获取
          </el-button>
        </div>

        <div v-else-if="qrcode" class="qrcode-content">
          <!-- 二维码图片 -->
          <div class="qrcode-image">
            <img :src="qrcodeUrl" alt="登录二维码" />
            
            <!-- 过期遮罩 -->
            <div v-if="isExpired" class="expired-mask">
              <el-icon :size="48" color="#ffffff">
                <RefreshRight />
              </el-icon>
              <p>二维码已过期</p>
              <el-button type="primary" size="large" @click="refreshQRCode">
                刷新二维码
              </el-button>
            </div>
          </div>

          <!-- 扫码提示 -->
          <div class="scan-tips">
            <div v-if="!isScanned" class="tip-item">
              <el-icon :size="20" color="#409eff">
                <Camera />
              </el-icon>
              <span>请使用百度APP扫描二维码登录</span>
            </div>
            <div v-else class="tip-item success">
              <el-icon :size="20" color="#67c23a">
                <SuccessFilled />
              </el-icon>
              <span>扫描成功，请在手机上确认登录</span>
            </div>
          </div>

          <!-- 倒计时 -->
          <div class="countdown">
            <span>{{ countdown }}秒后自动刷新</span>
          </div>
        </div>
      </div>

      <!-- 底部信息 -->
      <div class="footer">
        <p>基于 Rust + Axum + Vue 3 构建</p>
        <p class="version">v1.0.0</p>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { useAuthStore } from '@/stores/auth'
import {
  FolderOpened,
  Loading,
  CircleClose,
  Refresh,
  RefreshRight,
  Camera,
  SuccessFilled,
} from '@element-plus/icons-vue'

const router = useRouter()
const authStore = useAuthStore()

// 状态
const loading = ref(false)
const error = ref('')
const qrcode = computed(() => authStore.qrcode)
const isScanned = ref(false)
const isExpired = ref(false)
const countdown = ref(120)

// 计算二维码URL
const qrcodeUrl = computed(() => {
  if (!qrcode.value) return ''
  return qrcode.value.image_base64
})

// 倒计时定时器
let countdownTimer: number | null = null

// 开始倒计时
function startCountdown() {
  countdown.value = 120
  isExpired.value = false

  if (countdownTimer) {
    clearInterval(countdownTimer)
  }

  countdownTimer = window.setInterval(() => {
    countdown.value--
    if (countdown.value <= 0) {
      stopCountdown()
      isExpired.value = true
      authStore.stopPolling()
    }
  }, 1000)
}

// 停止倒计时
function stopCountdown() {
  if (countdownTimer) {
    clearInterval(countdownTimer)
    countdownTimer = null
  }
}

// 生成二维码
async function generateQRCode() {
  loading.value = true
  error.value = ''
  isScanned.value = false
  isExpired.value = false

  try {
    await authStore.generateQRCode()
    
    // 开始轮询
    authStore.startPolling(
      // 成功回调
      () => {
        ElMessage.success('登录成功')
        stopCountdown()
        router.push('/files')
      },
      // 错误回调
      (err: any) => {
        error.value = err.message || '登录失败，请重试'
        stopCountdown()
      }
    )

    // 开始倒计时
    startCountdown()
  } catch (err: any) {
    error.value = err.message || '生成二维码失败，请重试'
  } finally {
    loading.value = false
  }
}

// 刷新二维码
async function refreshQRCode() {
  authStore.stopPolling()
  stopCountdown()
  await generateQRCode()
}

// 组件挂载
onMounted(async () => {
  // 检查是否已登录
  if (authStore.isLoggedIn) {
    router.push('/files')
    return
  }

  // 生成二维码
  await generateQRCode()
})

// 组件卸载
onUnmounted(() => {
  authStore.stopPolling()
  stopCountdown()
})
</script>

<style scoped lang="scss">
.login-container {
  display: flex;
  justify-content: center;
  align-items: center;
  min-height: 100vh;
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

.login-card {
  width: 100%;
  max-width: 480px;
  padding: 40px;
  background: white;
  border-radius: 16px;
  box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
}

.header {
  text-align: center;
  margin-bottom: 40px;

  .logo {
    margin-bottom: 20px;
  }

  h1 {
    margin: 0 0 8px 0;
    font-size: 28px;
    font-weight: 600;
    color: #333;
  }

  .subtitle {
    margin: 0;
    font-size: 14px;
    color: #999;
  }
}

.qrcode-section {
  min-height: 400px;
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
}

.loading {
  text-align: center;

  p {
    margin-top: 16px;
    color: #666;
  }
}

.error-state {
  text-align: center;

  .error-text {
    margin: 20px 0;
    color: #f56c6c;
    font-size: 14px;
  }
}

.qrcode-content {
  width: 100%;
  display: flex;
  flex-direction: column;
  align-items: center;
}

.qrcode-image {
  position: relative;
  width: 280px;
  height: 280px;
  padding: 20px;
  background: white;
  border: 2px solid #e0e0e0;
  border-radius: 12px;
  margin-bottom: 20px;

  img {
    width: 100%;
    height: 100%;
    display: block;
  }

  .expired-mask {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(0, 0, 0, 0.8);
    border-radius: 12px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    color: white;

    p {
      margin: 16px 0 24px 0;
      font-size: 16px;
    }
  }
}

.scan-tips {
  width: 100%;
  margin-bottom: 16px;

  .tip-item {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    padding: 12px;
    background: #f0f9ff;
    border-radius: 8px;
    font-size: 14px;
    color: #409eff;

    &.success {
      background: #f0f9f0;
      color: #67c23a;
    }
  }
}

.countdown {
  text-align: center;
  font-size: 13px;
  color: #999;
}

.footer {
  margin-top: 40px;
  text-align: center;
  font-size: 12px;
  color: #999;

  p {
    margin: 4px 0;
  }

  .version {
    font-weight: 600;
    color: #666;
  }
}
</style>
