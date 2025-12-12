/**
 * UI 更新节流工具
 * 批量处理事件，减少 Vue 渲染次数
 */

export interface ThrottleOptions<T> {
  /** 更新间隔（毫秒），默认 100ms */
  interval?: number
  /** 更新回调，传入合并后的更新 */
  onUpdate: (updates: Map<string, T>) => void
  /** 获取事件的唯一标识 */
  getKey: (item: T) => string
}

/**
 * 创建更新节流器
 */
export function createUpdateThrottle<T>(options: ThrottleOptions<T>) {
  const { interval = 100, onUpdate, getKey } = options

  // 待处理的更新（按 key 去重，只保留最新）
  const pendingUpdates = new Map<string, T>()

  // 是否已安排更新
  let updateScheduled = false

  /**
   * 执行更新
   */
  function flush(): void {
    if (pendingUpdates.size === 0) {
      updateScheduled = false
      return
    }

    // 复制并清空待处理队列
    const updates = new Map(pendingUpdates)
    pendingUpdates.clear()
    updateScheduled = false

    // 调用回调
    onUpdate(updates)
  }

  /**
   * 安排更新
   */
  function scheduleUpdate(): void {
    if (updateScheduled) return

    updateScheduled = true

    // 使用 requestAnimationFrame 优先，否则用 setTimeout
    if (typeof requestAnimationFrame !== 'undefined') {
      // 先等待一帧，然后再等待 interval
      requestAnimationFrame(() => {
        setTimeout(flush, interval)
      })
    } else {
      setTimeout(flush, interval)
    }
  }

  /**
   * 添加更新
   */
  function push(item: T): void {
    const key = getKey(item)
    pendingUpdates.set(key, item)
    scheduleUpdate()
  }

  /**
   * 批量添加更新
   */
  function pushMany(items: T[]): void {
    for (const item of items) {
      const key = getKey(item)
      pendingUpdates.set(key, item)
    }
    if (items.length > 0) {
      scheduleUpdate()
    }
  }

  /**
   * 立即执行所有待处理的更新
   */
  function flushNow(): void {
    flush()
  }

  /**
   * 清空待处理的更新
   */
  function clear(): void {
    pendingUpdates.clear()
    updateScheduled = false
  }

  return {
    push,
    pushMany,
    flushNow,
    clear,
  }
}

/**
 * 合并任务列表更新
 * 将事件更新应用到任务列表
 */
export function applyTaskUpdates<T extends { id?: string }>(
  tasks: T[],
  updates: Map<string, Partial<T>>,
  options?: {
    /** 处理新增任务 */
    onCreated?: (update: Partial<T>) => T | null
    /** 处理删除任务 */
    onDeleted?: (taskId: string) => void
  }
): T[] {
  const tasksMap = new Map(tasks.map((t) => [t.id, t]))
  const deletedIds = new Set<string>()

  for (const [taskId, update] of updates) {
    const existing = tasksMap.get(taskId)

    if (existing) {
      // 更新现有任务
      Object.assign(existing, update)
    } else if (options?.onCreated) {
      // 新增任务
      const newTask = options.onCreated(update)
      if (newTask && newTask.id) {
        tasksMap.set(newTask.id, newTask)
      }
    }

    // 检查是否删除
    if ((update as any).event_type === 'deleted') {
      deletedIds.add(taskId)
      options?.onDeleted?.(taskId)
    }
  }

  // 移除已删除的任务
  for (const id of deletedIds) {
    tasksMap.delete(id)
  }

  return Array.from(tasksMap.values())
}
