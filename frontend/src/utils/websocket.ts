/**
 * WebSocket 客户端
 * 单例模式管理 WebSocket 连接，提供事件订阅机制
 */

import type {
  WsClientMessage,
  WsServerMessage,
  DownloadEvent,
  FolderEvent,
  UploadEvent,
  TransferEvent,
  BackupEvent,
  TimestampedEvent,
} from '@/types/events'

// 连接状态
export type ConnectionState = 'disconnected' | 'connecting' | 'connected'

// 事件回调类型
type DownloadEventCallback = (event: DownloadEvent) => void
type FolderEventCallback = (event: FolderEvent) => void
type UploadEventCallback = (event: UploadEvent) => void
type TransferEventCallback = (event: TransferEvent) => void
type BackupEventCallback = (event: BackupEvent) => void
type ConnectionStateCallback = (state: ConnectionState) => void

// 重连配置
const RECONNECT_DELAYS = [1000, 2000, 4000, 8000, 16000, 30000] // 指数退避
const HEARTBEAT_INTERVAL = 30000 // 30秒心跳
const HEARTBEAT_TIMEOUT = 60000 // 60秒超时

class WebSocketClient {
  private static instance: WebSocketClient | null = null

  private ws: WebSocket | null = null
  private connectionState: ConnectionState = 'disconnected'
  private reconnectAttempt = 0
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private lastPongTime = 0

  // 事件订阅者
  private downloadListeners: Set<DownloadEventCallback> = new Set()
  private folderListeners: Set<FolderEventCallback> = new Set()
  private uploadListeners: Set<UploadEventCallback> = new Set()
  private transferListeners: Set<TransferEventCallback> = new Set()
  private backupListeners: Set<BackupEventCallback> = new Set()
  private connectionStateListeners: Set<ConnectionStateCallback> = new Set()

  // 连接 ID
  private connectionId: string | null = null

  // 🔥 当前订阅集合
  private currentSubscriptions: Set<string> = new Set()

  private constructor() {
    // 私有构造函数，强制使用单例
  }

  /**
   * 获取单例实例
   */
  public static getInstance(): WebSocketClient {
    if (!WebSocketClient.instance) {
      WebSocketClient.instance = new WebSocketClient()
    }
    return WebSocketClient.instance
  }

  /**
   * 获取 WebSocket URL
   */
  private getWsUrl(): string {
    const isDev = import.meta.env?.DEV ?? false
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.host
    // 开发环境走 Vite 代理 /ws -> 8080；生产环境同域直连
    const path = isDev ? '/ws/api/v1/ws' : '/api/v1/ws'
    return `${protocol}//${host}${path}`
  }

  /**
   * 连接 WebSocket
   */
  public connect(): void {
    if (this.connectionState !== 'disconnected') {
      console.log('[WS] 已连接或正在连接，跳过')
      return
    }

    this.setConnectionState('connecting')
    const url = this.getWsUrl()
    console.log('[WS] 正在连接:', url)

    try {
      this.ws = new WebSocket(url)
      this.setupEventHandlers()
    } catch (error) {
      console.error('[WS] 创建连接失败:', error)
      this.scheduleReconnect()
    }
  }

  /**
   * 断开连接
   */
  public disconnect(): void {
    console.log('[WS] 主动断开连接')
    this.stopHeartbeat()
    this.cancelReconnect()

    if (this.ws) {
      this.ws.close(1000, 'Client disconnect')
      this.ws = null
    }

    this.setConnectionState('disconnected')
  }

  /**
   * 设置连接状态
   */
  private setConnectionState(state: ConnectionState): void {
    if (this.connectionState !== state) {
      this.connectionState = state
      console.log('[WS] 状态变更:', state)
      this.connectionStateListeners.forEach((cb) => cb(state))
    }
  }

  /**
   * 设置 WebSocket 事件处理器
   */
  private setupEventHandlers(): void {
    if (!this.ws) return

    this.ws.onopen = () => {
      console.log('[WS] 连接成功')
      this.reconnectAttempt = 0
      this.setConnectionState('connected')
      this.startHeartbeat()

      // 🔥 连接成功后自动恢复订阅
      if (this.currentSubscriptions.size > 0) {
        const subscriptions = Array.from(this.currentSubscriptions)
        console.log('[WS] 恢复订阅:', subscriptions)
        this.send({
          type: 'subscribe',
          subscriptions,
        })
      }
    }

    this.ws.onclose = (event) => {
      console.log('[WS] 连接关闭:', event.code, event.reason)
      this.stopHeartbeat()
      this.setConnectionState('disconnected')

      // 非正常关闭时自动重连
      if (event.code !== 1000) {
        this.scheduleReconnect()
      }
    }

    this.ws.onerror = (error) => {
      console.error('[WS] 连接错误:', error)
    }

    this.ws.onmessage = (event) => {
      this.handleMessage(event.data)
    }
  }

  /**
   * 处理收到的消息
   */
  private handleMessage(data: string): void {
    try {
      const message = JSON.parse(data) as WsServerMessage

      switch (message.type) {
        case 'connected':
          this.connectionId = message.connection_id
          console.log('[WS] 连接 ID:', this.connectionId)
          break

        case 'pong':
          this.lastPongTime = Date.now()
          break

        case 'event':
          this.dispatchEvent(message as any)
          break

        case 'event_batch':
          message.events.forEach((e) => this.dispatchEvent(e))
          break

        case 'snapshot':
          console.log('[WS] 收到状态快照')
          // TODO: 处理状态快照
          break

        case 'error':
          console.error('[WS] 服务端错误:', message.code, message.message)
          break

        case 'subscribe_success':
          console.log('[WS] 订阅成功:', (message as any).subscriptions)
          break

        case 'unsubscribe_success':
          console.log('[WS] 取消订阅成功:', (message as any).subscriptions)
          break

        default:
          console.warn('[WS] 未知消息类型:', message)
      }
    } catch (error) {
      console.error('[WS] 解析消息失败:', error, data)
    }
  }

  /**
   * 分发事件到订阅者
   */
  private dispatchEvent(event: TimestampedEvent): void {
    const { category } = event

    // 🔥 记录接收到的事件
    console.log(
        `📡 [WS接收] 类别=${category} | 事件=${(event.event as any).event_type} | 任务=${
            (event.event as any).task_id || (event.event as any).folder_id || 'unknown'
        } | 事件ID=${event.event_id} | 时间戳=${event.timestamp}`,
        event
    )

    switch (category) {
      case 'download':
        this.downloadListeners.forEach((cb) => cb(event.event as DownloadEvent))
        break
      case 'folder':
        this.folderListeners.forEach((cb) => cb(event.event as FolderEvent))
        break
      case 'upload':
        this.uploadListeners.forEach((cb) => cb(event.event as UploadEvent))
        break
      case 'transfer':
        this.transferListeners.forEach((cb) => cb(event.event as TransferEvent))
        break
      case 'backup':
        this.backupListeners.forEach((cb) => cb(event.event as BackupEvent))
        break
      default:
        console.warn('[WS] 未知事件类别:', category)
    }
  }

  /**
   * 发送消息
   */
  private send(message: WsClientMessage): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message))
    }
  }

  /**
   * 启动心跳
   */
  private startHeartbeat(): void {
    this.stopHeartbeat()
    this.lastPongTime = Date.now()

    this.heartbeatTimer = setInterval(() => {
      // 检查超时
      if (Date.now() - this.lastPongTime > HEARTBEAT_TIMEOUT) {
        console.warn('[WS] 心跳超时，重新连接')
        this.ws?.close(4000, 'Heartbeat timeout')
        return
      }

      // 发送心跳
      this.send({
        type: 'ping',
        timestamp: Date.now(),
      })
    }, HEARTBEAT_INTERVAL)
  }

  /**
   * 停止心跳
   */
  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
  }

  /**
   * 安排重连
   */
  private scheduleReconnect(): void {
    this.cancelReconnect()

    const delay = RECONNECT_DELAYS[Math.min(this.reconnectAttempt, RECONNECT_DELAYS.length - 1)]
    this.reconnectAttempt++

    console.log(`[WS] ${delay / 1000} 秒后重连 (第 ${this.reconnectAttempt} 次)`)

    this.reconnectTimer = setTimeout(() => {
      this.connect()
    }, delay)
  }

  /**
   * 取消重连
   */
  private cancelReconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  // ============ 订阅方法 ============

  /**
   * 订阅下载事件
   */
  public onDownloadEvent(callback: DownloadEventCallback): () => void {
    this.downloadListeners.add(callback)
    return () => this.downloadListeners.delete(callback)
  }

  /**
   * 订阅文件夹事件
   */
  public onFolderEvent(callback: FolderEventCallback): () => void {
    this.folderListeners.add(callback)
    return () => this.folderListeners.delete(callback)
  }

  /**
   * 订阅上传事件
   */
  public onUploadEvent(callback: UploadEventCallback): () => void {
    this.uploadListeners.add(callback)
    return () => this.uploadListeners.delete(callback)
  }

  /**
   * 订阅转存事件
   */
  public onTransferEvent(callback: TransferEventCallback): () => void {
    this.transferListeners.add(callback)
    return () => this.transferListeners.delete(callback)
  }

  /**
   * 订阅备份事件
   */
  public onBackupEvent(callback: BackupEventCallback): () => void {
    this.backupListeners.add(callback)
    return () => this.backupListeners.delete(callback)
  }

  /**
   * 订阅连接状态变化
   */
  public onConnectionStateChange(callback: ConnectionStateCallback): () => void {
    this.connectionStateListeners.add(callback)
    // 立即通知当前状态
    callback(this.connectionState)
    return () => this.connectionStateListeners.delete(callback)
  }

  /**
   * 请求状态快照
   */
  public requestSnapshot(): void {
    this.send({ type: 'request_snapshot' })
  }

  // ============ 🔥 服务端订阅管理 ============

  /**
   * 订阅事件
   *
   * 订阅名称格式：
   * - `download:file` - 文件下载事件
   * - `download:folder` - 文件夹下载事件
   * - `download:{group_id}` - 文件夹子任务事件（group_id 为文件夹任务 ID）
   * - `upload:*` - 所有上传事件
   * - `transfer:*` - 所有转存事件
   */
  public subscribe(subscriptions: string[]): void {
    subscriptions.forEach(sub => this.currentSubscriptions.add(sub))
    if (this.isConnected()) {
      console.log('[WS] 发送订阅:', subscriptions)
      this.send({
        type: 'subscribe',
        subscriptions,
      })
    }
  }

  /**
   * 取消订阅
   */
  public unsubscribe(subscriptions: string[]): void {
    subscriptions.forEach(sub => this.currentSubscriptions.delete(sub))
    if (this.isConnected()) {
      console.log('[WS] 发送取消订阅:', subscriptions)
      this.send({
        type: 'unsubscribe',
        subscriptions,
      })
    }
  }

  /**
   * 获取当前订阅列表
   */
  public getSubscriptions(): string[] {
    return Array.from(this.currentSubscriptions)
  }

  /**
   * 获取当前连接状态
   */
  public getConnectionState(): ConnectionState {
    return this.connectionState
  }

  /**
   * 是否已连接
   */
  public isConnected(): boolean {
    return this.connectionState === 'connected'
  }
}

// 导出单例获取方法
export function getWebSocketClient(): WebSocketClient {
  return WebSocketClient.getInstance()
}

// 导出便捷方法
export function connectWebSocket(): void {
  getWebSocketClient().connect()
}

export function disconnectWebSocket(): void {
  getWebSocketClient().disconnect()
}
