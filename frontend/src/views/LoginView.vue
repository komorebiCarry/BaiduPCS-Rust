<template>
  <div class="login-container">
    <!-- 右上角小贴士 -->
    <div class="tips-card" :class="{ collapsed: tipsCollapsed }">
      <div class="tips-header" @click="tipsCollapsed = !tipsCollapsed">
        <div class="tips-title">
          <el-icon :size="18" color="#409eff">
            <InfoFilled />
          </el-icon>
          <span>登录小贴士</span>
        </div>
        <el-icon :size="16" class="collapse-icon" :class="{ rotated: tipsCollapsed }">
          <ArrowDown />
        </el-icon>
      </div>

      <div v-show="!tipsCollapsed" class="tips-content">
        <div class="tips-section">
          <h4>📱 操作步骤</h4>
          <ol>
            <li>打开百度网盘 APP</li>
            <li>点击"扫一扫"功能</li>
            <li>扫描页面上的二维码</li>
            <li><strong>在 APP 中点击确认登录</strong></li>
          </ol>
        </div>

        <div class="tips-section warning">
          <h4>⚠️ 重要提示</h4>
          <ul>
            <li><strong>确认后，APP 必须保持打开</strong></li>
            <li>不能关闭或切换到后台</li>
            <li>等待网页自动跳转（1-3秒）</li>
          </ul>
        </div>

        <div class="tips-section info">
          <p class="small-text">
            <el-icon :size="14"><Warning /></el-icon>
            如果 APP 关闭，登录会失败。请点击"重新扫码"按钮重试。
          </p>
        </div>
      </div>
    </div>

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

            <!-- 扫描成功遮罩 -->
            <div v-else-if="isScanned" class="scanned-mask">
              <el-icon :size="48" color="#ffffff">
                <SuccessFilled />
              </el-icon>
              <p class="success-text">扫描成功！</p>
              <p class="hint-text">请在手机上确认登录</p>
              <el-button type="success" size="large" plain @click="refreshQRCode">
                <el-icon><Refresh /></el-icon>
                重新扫码
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
              <div class="success-header">
                <el-icon :size="24" color="#67c23a">
                  <SuccessFilled />
                </el-icon>
                <span class="success-title">扫描成功！</span>
              </div>
              <div class="important-notes">
                <div class="note-item highlight">
                  <el-icon :size="18" color="#e6a23c">
                    <Warning />
                  </el-icon>
                  <span>请在手机百度APP中点击"确认登录"</span>
                </div>
                <div class="note-item highlight">
                  <el-icon :size="18" color="#e6a23c">
                    <Warning />
                  </el-icon>
                  <span>确认后，APP不能立即关闭或切换到后台，否则会登入失败</span>
                </div>
              </div>
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
  Warning,
  InfoFilled,
  ArrowDown,
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
const tipsCollapsed = ref(false) // 贴士折叠状态

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
        },
        // 扫码回调
        () => {
          isScanned.value = true
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
  position: relative;
}

.tips-card {
  position: fixed;
  top: 20px;
  right: 20px;
  width: 320px;
  background: white;
  border-radius: 12px;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.15);
  overflow: hidden;
  transition: all 0.3s ease;
  z-index: 100;

  &.collapsed {
    width: auto;
  }

  .tips-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 12px 16px;
    background: linear-gradient(135deg, #409eff 0%, #1890ff 100%);
    color: white;
    cursor: pointer;
    user-select: none;

    .tips-title {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 14px;
      font-weight: 600;
    }

    .collapse-icon {
      transition: transform 0.3s ease;

      &.rotated {
        transform: rotate(-90deg);
      }
    }

    &:hover {
      background: linear-gradient(135deg, #2080ff 0%, #0070ff 100%);
    }
  }

  .tips-content {
    padding: 16px;
    max-height: calc(100vh - 120px);
    overflow-y: auto;

    .tips-section {
      margin-bottom: 16px;

      &:last-child {
        margin-bottom: 0;
      }

      h4 {
        margin: 0 0 8px 0;
        font-size: 14px;
        font-weight: 600;
        color: #333;
      }

      ol, ul {
        margin: 0;
        padding-left: 20px;
        font-size: 13px;
        line-height: 1.8;
        color: #666;

        li {
          margin-bottom: 4px;

          strong {
            color: #333;
            font-weight: 600;
          }
        }
      }

      &.warning {
        background: #fff7e6;
        padding: 12px;
        border-radius: 8px;
        border-left: 3px solid #fa8c16;

        h4 {
          color: #d46b08;
        }

        ul li {
          color: #ad6800;

          strong {
            color: #ad4e00;
          }
        }
      }

      &.info {
        .small-text {
          display: flex;
          align-items: flex-start;
          gap: 6px;
          margin: 0;
          font-size: 12px;
          color: #666;
          line-height: 1.6;
          padding: 10px;
          background: #f0f9ff;
          border-radius: 6px;
        }
      }
    }

    /* 自定义滚动条 */
    &::-webkit-scrollbar {
      width: 6px;
    }

    &::-webkit-scrollbar-track {
      background: #f1f1f1;
      border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb {
      background: #c1c1c1;
      border-radius: 3px;

      &:hover {
        background: #a8a8a8;
      }
    }
  }
}

/* 小屏幕适配 */
@media (max-width: 768px) {
  .tips-card {
    top: 10px;
    right: 10px;
    width: 280px;
    font-size: 12px;
  }
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

  .scanned-mask {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    bottom: 0;
    background: rgba(103, 194, 58, 0.95);
    border-radius: 12px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    color: white;
    animation: fadeIn 0.3s ease-in-out;

    .success-text {
      margin: 16px 0 8px 0;
      font-size: 18px;
      font-weight: 600;
    }

    .hint-text {
      margin: 0 0 24px 0;
      font-size: 14px;
      opacity: 0.9;
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
      flex-direction: column;
      gap: 12px;
      background: linear-gradient(135deg, #f0f9f0 0%, #e8f5e9 100%);
      border: 2px solid #67c23a;
      padding: 16px;
      animation: pulse 2s ease-in-out infinite;

      .success-header {
        display: flex;
        align-items: center;
        gap: 8px;

        .success-title {
          font-size: 16px;
          font-weight: 600;
          color: #67c23a;
        }
      }

      .important-notes {
        width: 100%;
        display: flex;
        flex-direction: column;
        gap: 10px;
        margin-top: 4px;

        .note-item {
          display: flex;
          align-items: center;
          gap: 8px;
          padding: 10px 12px;
          background: #fff;
          border-radius: 6px;
          border-left: 3px solid #e6a23c;
          font-size: 13px;
          color: #333;
          box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);

          &.highlight {
            font-weight: 500;
          }

          span {
            flex: 1;
            line-height: 1.5;
          }
        }
      }
    }
  }
}

@keyframes pulse {
  0%, 100% {
    box-shadow: 0 0 0 0 rgba(103, 194, 58, 0.4);
  }
  50% {
    box-shadow: 0 0 0 8px rgba(103, 194, 58, 0);
  }
}

@keyframes fadeIn {
  from {
    opacity: 0;
    transform: scale(0.9);
  }
  to {
    opacity: 1;
    transform: scale(1);
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
