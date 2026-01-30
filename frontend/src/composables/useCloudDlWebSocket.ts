/**
 * 离线下载 WebSocket 订阅 Composable
 *
 * 本模块提供离线下载功能的 WebSocket 订阅服务，包括：
 * - 订阅/取消订阅离线下载事件
 * - 处理各类离线下载事件（状态变化、进度更新、任务完成等）
 * - 手动刷新任务列表
 * - 自动下载触发
 */

import { ref, onMounted, onUnmounted, type Ref } from 'vue'
import { getWebSocketClient, type ConnectionState } from '@/utils/websocket'
import { refreshTasks as apiRefreshTasks } from '@/api/cloudDl'
import type { CloudDlTaskInfo, AutoDownloadConfig } from '@/api/cloudDl'
import type { CloudDlEvent as WsCloudDlEvent } from '@/types/events'

// =====================================================
// CloudDlEvent 接口定义
// =====================================================

/**
 * 离线下载事件基础接口
 */
interface CloudDlEventBase {
  event_type: string
}

/**
 * 状态变化事件
 */
export interface CloudDlStatusChangedEvent extends CloudDlEventBase {
  event_type: 'status_changed'
  task_id: number
  old_status: number | null
  new_status: number
  task: CloudDlTaskInfo
}

/**
 * 任务完成事件
 */
export interface CloudDlTaskCompletedEvent extends CloudDlEventBase {
  event_type: 'task_completed'
  task_id: number
  task: CloudDlTaskInfo
  auto_download_config: AutoDownloadConfig | null
}

/**
 * 进度更新事件
 */
export interface CloudDlProgressUpdateEvent extends CloudDlEventBase {
  event_type: 'progress_update'
  task_id: number
  finished_size: number
  file_size: number
  progress_percent: number
}

/**
 * 任务列表刷新事件
 */
export interface CloudDlTaskListRefreshedEvent extends CloudDlEventBase {
  event_type: 'task_list_refreshed'
  tasks: CloudDlTaskInfo[]
}

/**
 * 离线下载事件联合类型
 */
export type CloudDlEvent =
  | CloudDlStatusChangedEvent
  | CloudDlTaskCompletedEvent
  | CloudDlProgressUpdateEvent
  | CloudDlTaskListRefreshedEvent

// =====================================================
// 事件回调类型
// =====================================================

/**
 * 状态变化回调
 */
export type OnStatusChangedCallback = (event: CloudDlStatusChangedEvent) => void

/**
 * 任务完成回调
 */
export type OnTaskCompletedCallback = (event: CloudDlTaskCompletedEvent) => void

/**
 * 进度更新回调
 */
export type OnProgressUpdateCallback = (event: CloudDlProgressUpdateEvent) => void

/**
 * 任务列表刷新回调
 */
export type OnTaskListRefreshedCallback = (event: CloudDlTaskListRefreshedEvent) => void

/**
 * 通用事件回调
 */
export type OnCloudDlEventCallback = (event: CloudDlEvent) => void

// =====================================================
// Composable 配置选项
// =====================================================

/**
 * useCloudDlWebSocket 配置选项
 */
export interface UseCloudDlWebSocketOptions {
  /** 是否在挂载时自动订阅（默认 true） */
  autoSubscribe?: boolean
  /** 状态变化回调 */
  onStatusChanged?: OnStatusChangedCallback
  /** 任务完成回调 */
  onTaskCompleted?: OnTaskCompletedCallback
  /** 进度更新回调 */
  onProgressUpdate?: OnProgressUpdateCallback
  /** 任务列表刷新回调 */
  onTaskListRefreshed?: OnTaskListRefreshedCallback
  /** 通用事件回调（接收所有事件） */
  onEvent?: OnCloudDlEventCallback
}

// =====================================================
// Composable 返回类型
// =====================================================

/**
 * useCloudDlWebSocket 返回类型
 */
export interface UseCloudDlWebSocketReturn {
  /** 是否已订阅 */
  isSubscribed: Ref<boolean>
  /** WebSocket 连接状态 */
  connectionState: Ref<ConnectionState>
  /** 是否正在刷新 */
  isRefreshing: Ref<boolean>
  /** 订阅离线下载事件 */
  subscribe: () => void
  /** 取消订阅离线下载事件 */
  unsubscribe: () => void
  /** 手动刷新任务列表 */
  refresh: () => Promise<CloudDlTaskInfo[]>
}

// =====================================================
// WebSocket 事件类别常量
// =====================================================

/** 离线下载订阅主题 */
const CLOUD_DL_SUBSCRIPTION = 'cloud_dl'

// =====================================================
// 事件处理函数
// =====================================================

/**
 * 处理离线下载事件
 *
 * @param rawEvent 原始事件数据
 * @param options 配置选项
 */
function handleCloudDlEvent(rawEvent: any, options: UseCloudDlWebSocketOptions): void {
  // 验证事件类型
  if (!rawEvent || typeof rawEvent.event_type !== 'string') {
    console.warn('[CloudDl WS] 收到无效事件:', rawEvent)
    return
  }

  const event = rawEvent as CloudDlEvent

  // 记录事件
  console.log(
    `📡 [CloudDl WS] 事件类型=${event.event_type} | 任务ID=${
      'task_id' in event ? event.task_id : 'N/A'
    }`,
    event
  )

  // 调用通用事件回调
  if (options.onEvent) {
    options.onEvent(event)
  }

  // 根据事件类型调用特定回调
  switch (event.event_type) {
    case 'status_changed':
      if (options.onStatusChanged) {
        options.onStatusChanged(event as CloudDlStatusChangedEvent)
      }
      break

    case 'task_completed':
      if (options.onTaskCompleted) {
        options.onTaskCompleted(event as CloudDlTaskCompletedEvent)
      }
      break

    case 'progress_update':
      if (options.onProgressUpdate) {
        options.onProgressUpdate(event as CloudDlProgressUpdateEvent)
      }
      break

    case 'task_list_refreshed':
      if (options.onTaskListRefreshed) {
        options.onTaskListRefreshed(event as CloudDlTaskListRefreshedEvent)
      }
      break

    default:
      // 处理未知事件类型
      console.warn('[CloudDl WS] 未知事件类型:', (event as any).event_type)
  }
}

// =====================================================
// Composable 实现
// =====================================================

/**
 * 离线下载 WebSocket 订阅 Composable
 *
 * 提供离线下载功能的 WebSocket 订阅服务，支持：
 * - 自动订阅/取消订阅
 * - 事件回调处理
 * - 手动刷新任务列表
 *
 * @param options 配置选项
 * @returns Composable 返回对象
 *
 * @example
 * ```vue
 * <script setup lang="ts">
 * import { useCloudDlWebSocket } from '@/composables/useCloudDlWebSocket'
 *
 * const { isSubscribed, refresh } = useCloudDlWebSocket({
 *   onStatusChanged: (event) => {
 *     console.log('状态变化:', event)
 *   },
 *   onTaskCompleted: (event) => {
 *     console.log('任务完成:', event)
 *     // 触发自动下载逻辑
 *   },
 *   onProgressUpdate: (event) => {
 *     console.log('进度更新:', event)
 *   },
 *   onTaskListRefreshed: (event) => {
 *     console.log('任务列表刷新:', event)
 *   },
 * })
 * </script>
 * ```
 */
export function useCloudDlWebSocket(
  options: UseCloudDlWebSocketOptions = {}
): UseCloudDlWebSocketReturn {
  const { autoSubscribe = true } = options

  // 响应式状态
  const isSubscribed = ref(false)
  const connectionState = ref<ConnectionState>('disconnected')
  const isRefreshing = ref(false)

  // WebSocket 客户端
  const wsClient = getWebSocketClient()

  // 事件处理器引用（用于清理）
  let cloudDlEventUnsubscribe: (() => void) | null = null
  let connectionStateUnsubscribe: (() => void) | null = null

  /**
   * 处理 WebSocket 离线下载事件
   */
  function handleWsCloudDlEvent(wsEvent: WsCloudDlEvent): void {
    // 转换为本地事件类型
    const event = wsEvent as unknown as CloudDlEvent
    handleCloudDlEvent(event, options)
  }

  /**
   * 订阅离线下载事件
   */
  function subscribe(): void {
    if (isSubscribed.value) {
      console.log('[CloudDl WS] 已订阅，跳过')
      return
    }

    console.log('[CloudDl WS] 订阅离线下载事件')

    // 确保 WebSocket 已连接
    if (!wsClient.isConnected()) {
      wsClient.connect()
    }

    // 订阅 cloud_dl 主题
    wsClient.subscribe([CLOUD_DL_SUBSCRIPTION])

    // 注册事件处理器
    cloudDlEventUnsubscribe = wsClient.onCloudDlEvent(handleWsCloudDlEvent)

    isSubscribed.value = true
  }

  /**
   * 取消订阅离线下载事件
   */
  function unsubscribe(): void {
    if (!isSubscribed.value) {
      console.log('[CloudDl WS] 未订阅，跳过')
      return
    }

    console.log('[CloudDl WS] 取消订阅离线下载事件')

    // 取消订阅 cloud_dl 主题
    wsClient.unsubscribe([CLOUD_DL_SUBSCRIPTION])

    // 清理事件处理器
    if (cloudDlEventUnsubscribe) {
      cloudDlEventUnsubscribe()
      cloudDlEventUnsubscribe = null
    }

    isSubscribed.value = false
  }

  /**
   * 手动刷新任务列表
   *
   * 调用后端 API 触发刷新，并通过 WebSocket 接收更新
   *
   * @returns 任务列表
   */
  async function refresh(): Promise<CloudDlTaskInfo[]> {
    if (isRefreshing.value) {
      console.log('[CloudDl WS] 正在刷新中，跳过')
      return []
    }

    isRefreshing.value = true
    console.log('[CloudDl WS] 手动刷新任务列表')

    try {
      const response = await apiRefreshTasks()
      console.log('[CloudDl WS] 刷新成功，共', response.tasks.length, '个任务')
      return response.tasks
    } catch (error) {
      console.error('[CloudDl WS] 刷新失败:', error)
      throw error
    } finally {
      isRefreshing.value = false
    }
  }

  /**
   * 设置连接状态监听
   */
  function setupConnectionStateListener(): void {
    connectionStateUnsubscribe = wsClient.onConnectionStateChange((state) => {
      connectionState.value = state
      console.log('[CloudDl WS] 连接状态变化:', state)

      // 重连后自动恢复订阅
      if (state === 'connected' && isSubscribed.value) {
        console.log('[CloudDl WS] 重连后恢复订阅')
        wsClient.subscribe([CLOUD_DL_SUBSCRIPTION])
      }
    })
  }

  /**
   * 清理资源
   */
  function cleanup(): void {
    if (isSubscribed.value) {
      unsubscribe()
    }

    if (cloudDlEventUnsubscribe) {
      cloudDlEventUnsubscribe()
      cloudDlEventUnsubscribe = null
    }

    if (connectionStateUnsubscribe) {
      connectionStateUnsubscribe()
      connectionStateUnsubscribe = null
    }
  }

  // 生命周期钩子
  onMounted(() => {
    setupConnectionStateListener()

    if (autoSubscribe) {
      subscribe()
    }
  })

  onUnmounted(() => {
    cleanup()
  })

  return {
    isSubscribed,
    connectionState,
    isRefreshing,
    subscribe,
    unsubscribe,
    refresh,
  }
}

// =====================================================
// 导出
// =====================================================

export default useCloudDlWebSocket
