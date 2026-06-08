<template>
  <div id="app">
    <!-- 全局 readonly_mode 横幅 -->
    <transition name="el-fade-in">
      <div v-if="authStore.readonlyMode" class="readonly-banner">
        <el-icon class="readonly-banner__icon"><WarningFilled /></el-icon>
        <span class="readonly-banner__text">
          服务端处于<strong>只读模式</strong>，新建/修改/删除请求会被拒绝。
        </span>
      </div>
    </transition>

    <router-view />
  </div>
</template>

<script setup lang="ts">
/**
 * 多账号 / WS bootstrap。
 *
 * 责任：
 *   1. 应用启动后向 WebSocket 订阅 `account:*` 主题，并在收到 AccountEvent / Snapshot
 *      时同步到 `useAuthStore`。
 *   2. 在收到 `Switched`（活跃账号切换）后向窗口广播一个跨视图自定义事件
 *      `multi-account:active-changed`，供 DownloadsView/UploadsView/TransfersView 等
 *      监听后强制重拉列表（仅请求 `?owner_uid=<新>` 的当账号视图）。
 *   3. 全局渲染 `readonly_mode` 横幅。
 *
 * 注意：connect() 由其他页面的 setupWebSocket 触发；此处仅在 ws 已连接 / 连接成功时
 * 主动 subscribe 一次（重连后通过 connectionStateChange 也会再 subscribe）。
 */
import { onMounted, onUnmounted } from 'vue'
import { WarningFilled } from '@element-plus/icons-vue'
import { useAuthStore } from '@/stores/auth'
import { getWebSocketClient } from '@/utils/websocket'

const authStore = useAuthStore()
const wsClient = getWebSocketClient()

const ACCOUNT_TOPIC = 'account:*'

let unsubscribeAccount: (() => void) | null = null
let unsubscribeSnapshot: (() => void) | null = null
let unsubscribeConnState: (() => void) | null = null

/**
 * 最近一次已广播的 newActiveUid。
 *
 * `useAuthStore.switchAccount()` HTTP 成功后会**主动**派发一次
 * `multi-account:active-changed`（WS 路径兜底，避免首次切换时 WS 尚未订阅完成
 * 而丢事件）。本字段在每次广播后记录值；当 WS `Switched` 到达时若值一致则跳
 * 过重复广播，避免同一次切换触发两次跨视图刷新。
 */
let lastBroadcastedActiveUid: number | null | undefined = undefined

/** 广播 active 账号切换：跨视图刷新挂钩（幂等） */
function broadcastActiveChanged(newActiveUid: number | null) {
  if (lastBroadcastedActiveUid === newActiveUid) {
    return
  }
  lastBroadcastedActiveUid = newActiveUid
  window.dispatchEvent(
      new CustomEvent('multi-account:active-changed', {
        detail: { newActiveUid },
      }),
  )
}

/** 监听 store 主动派发的 active-changed 事件以同步 lastBroadcastedActiveUid */
function handleStoreBroadcast(ev: Event) {
  const detail = (ev as CustomEvent<{ newActiveUid: number | null }>).detail
  if (detail) {
    lastBroadcastedActiveUid = detail.newActiveUid
  }
}

onMounted(() => {
  // store 主动派发的 active-changed 事件由本组件记录最近值（用于 WS 幂等去重）
  window.addEventListener('multi-account:active-changed', handleStoreBroadcast)

  // 订阅 account:* 主题（idempotent；若 WS 未连接也会被缓存到 currentSubscriptions）
  wsClient.subscribe([ACCOUNT_TOPIC])

  unsubscribeAccount = wsClient.onAccountEvent((event) => {
    authStore.applyAccountEvent(event)
    if (event.event_type === 'switched') {
      broadcastActiveChanged(event.new_active_uid)
    }
  })

  unsubscribeSnapshot = wsClient.onSnapshot((snapshot) => {
    // 仅同步多账号字段，不接管其它（其它子视图自己消费 downloads/uploads/...）
    authStore.applySnapshot(snapshot)
  })

  // 重连成功后重新订阅 account:* （部分 WS 实现 reconnect 时会清空订阅集）
  unsubscribeConnState = wsClient.onConnectionStateChange((state) => {
    if (state === 'connected') {
      wsClient.subscribe([ACCOUNT_TOPIC])
    }
  })
})

onUnmounted(() => {
  unsubscribeAccount?.()
  unsubscribeSnapshot?.()
  unsubscribeConnState?.()
  window.removeEventListener('multi-account:active-changed', handleStoreBroadcast)
  // 不主动 unsubscribe(['account:*'])：App.vue 卸载意味着应用退出，无需保留连接
})
</script>

<style scoped>
#app {
  width: 100%;
  height: 100vh;
  overflow: hidden;
}

.readonly-banner {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  z-index: 9999;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  padding: 6px 16px;
  background-color: #fff7e6;
  border-bottom: 1px solid #ffd591;
  color: #d46b08;
  font-size: 13px;
  line-height: 1.4;
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.04);
}

.readonly-banner__icon {
  flex: 0 0 auto;
  font-size: 16px;
}

.readonly-banner__text strong {
  font-weight: 600;
}
</style>

