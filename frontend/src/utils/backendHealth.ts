/**
 * 后端健康监测 + 自适应轮询
 *
 * 背景：多个视图（下载/上传/自动备份/设置）在 WebSocket 未连接时会用
 * setInterval 以固定 1~2 秒间隔轮询后端。一旦后端进程停掉，每次轮询都会
 * 失败并弹出一次 "请求失败 (500)"/"网络错误"，造成无限刷屏，且固定间隔
 * 会持续高频打不存在的服务。
 *
 * 本模块提供一个全局单例 BackendHealthMonitor：
 *  - axios 拦截器在每次请求成功/失败时上报，集中维护"连续失败次数 / 是否断开"
 *  - 错误提示去重节流：进入断开态时只提示一次，恢复时提示一次
 *  - getBackoffDelay() 给轮询器提供指数退避间隔（健康时即 base，断开后阶梯式增长并封顶）
 *
 * 以及 createAdaptivePoller()：自调度的轮询器，每轮按 monitor 的退避间隔安排
 * 下一次执行，从而实现"阶梯式重连"——后端断开后退避到上限间隔持续尝试，
 * 恢复后自动回到基础间隔，全程不再高频刷屏。
 */

export type BackendStatus = 'healthy' | 'down'

export interface BackoffOptions {
  /** 健康时的基础间隔（毫秒） */
  baseDelayMs: number
  /** 退避封顶间隔（毫秒） */
  maxDelayMs: number
}

type Listener = (status: BackendStatus) => void

/** 判断一个 axios 错误是否属于"后端不可达/服务端错误"（断连类） */
export function isBackendDownError(error: unknown): boolean {
  const err = error as { response?: { status?: number }; code?: string } | undefined
  const status = err?.response?.status
  // 没有 response：网络错误 / 连接被拒 / 超时 → 视为后端不可达
  if (status === undefined) {
    return true
  }
  // 5xx：后端存活但出错，同样按断连类计入退避
  return status >= 500
}

export class BackendHealthMonitor {
  private consecutiveFailures = 0
  private status: BackendStatus = 'healthy'
  private listeners = new Set<Listener>()

  /** 进入断开态后，多少次连续失败才认定为"断开"（避免偶发抖动误报） */
  readonly downThreshold: number

  constructor(downThreshold = 3) {
    this.downThreshold = downThreshold
  }

  getStatus(): BackendStatus {
    return this.status
  }

  getConsecutiveFailures(): number {
    return this.consecutiveFailures
  }

  /**
   * 记录一次成功请求。
   * @returns 若刚从断开态恢复则为 true（调用方可用于"已恢复"提示）
   */
  recordSuccess(): boolean {
    this.consecutiveFailures = 0
    if (this.status === 'down') {
      this.status = 'healthy'
      this.emit()
      return true
    }
    return false
  }

  /**
   * 记录一次断连类失败。
   * @returns 若本次失败应当向用户提示则为 true（仅在刚跨入断开态时为 true）
   */
  recordFailure(): boolean {
    this.consecutiveFailures += 1
    if (this.status === 'healthy' && this.consecutiveFailures >= this.downThreshold) {
      this.status = 'down'
      this.emit()
      return true
    }
    return false
  }

  /**
   * 计算下一次轮询间隔：健康时返回 base；出现连续失败后指数退避并封顶。
   * 退避基于 consecutiveFailures，从第一次失败起就开始拉长间隔。
   */
  getBackoffDelay(opts: BackoffOptions): number {
    const { baseDelayMs, maxDelayMs } = opts
    if (this.consecutiveFailures === 0) {
      return baseDelayMs
    }
    const factor = 2 ** (this.consecutiveFailures - 1)
    return Math.min(baseDelayMs * factor, maxDelayMs)
  }

  subscribe(listener: Listener): () => void {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  /** 仅供测试：重置内部状态 */
  reset(): void {
    this.consecutiveFailures = 0
    this.status = 'healthy'
  }

  private emit(): void {
    for (const l of this.listeners) {
      l(this.status)
    }
  }
}

/** 全局单例 */
export const backendHealth = new BackendHealthMonitor()

export interface AdaptivePoller {
  start: () => void
  stop: () => void
  isRunning: () => boolean
}

export interface AdaptivePollerOptions extends Partial<BackoffOptions> {
  /** 注入的监测器（默认使用全局单例，测试时可替换） */
  monitor?: BackendHealthMonitor
}

/**
 * 创建一个自调度的自适应轮询器。
 *
 * 每轮执行 `fn` 后，按 monitor 的退避间隔安排下一轮：后端健康时为 baseDelayMs，
 * 断开后阶梯式增长到 maxDelayMs 封顶并持续重连，恢复后自动回到 baseDelayMs。
 * `fn` 自身是否抛错不影响调度——失败计数由 axios 拦截器统一上报给 monitor。
 */
export function createAdaptivePoller(
  fn: () => void | Promise<void>,
  options: AdaptivePollerOptions = {},
): AdaptivePoller {
  const baseDelayMs = options.baseDelayMs ?? 1000
  const maxDelayMs = options.maxDelayMs ?? 30000
  const monitor = options.monitor ?? backendHealth

  let timer: ReturnType<typeof setTimeout> | null = null
  let running = false

  const scheduleNext = () => {
    if (!running) return
    const delay = monitor.getBackoffDelay({ baseDelayMs, maxDelayMs })
    timer = setTimeout(runOnce, delay)
  }

  async function runOnce() {
    if (!running) return
    try {
      await fn()
    } catch {
      // fn 内部异常不应中断轮询；失败计数由拦截器上报
    }
    scheduleNext()
  }

  return {
    start() {
      if (running) return
      running = true
      // 立即执行一次，再按退避调度后续
      void runOnce()
    },
    stop() {
      running = false
      if (timer) {
        clearTimeout(timer)
        timer = null
      }
    },
    isRunning() {
      return running
    },
  }
}
