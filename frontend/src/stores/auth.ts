import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import { generateQRCode as apiGenerateQRCode, getQRCodeStatus, getCurrentUser, logout as apiLogout } from '@/api/auth'
import type { QRCode, UserAuth } from '@/api/auth'

export const useAuthStore = defineStore('auth', () => {
  // 状态
  const user = ref<UserAuth | null>(null)
  const qrcode = ref<QRCode | null>(null)
  const isPolling = ref(false)
  const pollingTimer = ref<number | null>(null)

  // 计算属性
  const isLoggedIn = computed(() => user.value !== null)
  const username = computed(() => user.value?.nickname || user.value?.username || '')
  const avatar = computed(() => user.value?.avatar_url || '')

  // 生成二维码
  async function generateQRCode(): Promise<QRCode> {
    try {
      qrcode.value = await apiGenerateQRCode()
      return qrcode.value
    } catch (error) {
      console.error('生成二维码失败:', error)
      throw error
    }
  }

  // 开始轮询二维码状态
  function startPolling(onSuccess: () => void, onError: (error: any) => void) {
    if (isPolling.value || !qrcode.value) return

    isPolling.value = true

    const poll = async () => {
      try {
        if (!qrcode.value) {
          stopPolling()
          return
        }

        const status = await getQRCodeStatus(qrcode.value.sign)

        switch (status.status) {
          case 'success':
            // 登录成功
            stopPolling()
            try {
              await fetchUserInfo()
            } catch (error) {
              console.error('获取用户信息失败，但登录成功:', error)
              // 即使获取失败，也设置基本用户信息
              if (status.user) {
                user.value = status.user
              }
            }
            onSuccess()
            break
          case 'expired':
            // 二维码过期
            stopPolling()
            onError(new Error('二维码已过期'))
            break
          case 'failed':
            // 登录失败
            stopPolling()
            onError(new Error(status.reason || '登录失败'))
            break
          case 'scanned':
            // 已扫码，等待确认
            console.log('已扫码，等待确认...')
            break
          case 'waiting':
            // 等待扫码
            console.log('等待扫码...')
            break
        }
      } catch (error) {
        console.error('轮询失败:', error)
        // 继续轮询，不停止
      }
    }

    // 开始轮询
    poll()
    pollingTimer.value = window.setInterval(poll, 3000)
  }

  // 停止轮询
  function stopPolling() {
    if (pollingTimer.value) {
      clearInterval(pollingTimer.value)
      pollingTimer.value = null
    }
    isPolling.value = false
  }

  // 获取用户信息
  async function fetchUserInfo() {
    try {
      user.value = await getCurrentUser()
    } catch (error) {
      console.error('获取用户信息失败:', error)
      throw error
    }
  }

  // 登出
  async function logout() {
    try {
      await apiLogout()
      user.value = null
      qrcode.value = null
      stopPolling()
    } catch (error) {
      console.error('登出失败:', error)
      throw error
    }
  }

  return {
    // 状态
    user,
    qrcode,
    isPolling,
    // 计算属性
    isLoggedIn,
    username,
    avatar,
    // 方法
    generateQRCode,
    startPolling,
    stopPolling,
    fetchUserInfo,
    logout
  }
})
