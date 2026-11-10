import { defineStore } from 'pinia'
import { computed } from 'vue'
import { useConfigStore } from './config'

/**
 * 界面偏好
 *
 * 开关保存在后端配置 `[ui]` 段（系统设置页编辑），这里只做读取 + 默认值兜底；
 * 「网盘管理」最近浏览目录仅保存在内存（按账号区分），刷新页面后回到根目录。
 */
export const useUiPrefsStore = defineStore('uiPrefs', () => {
  const configStore = useConfigStore()

  /** 页面切换后保留网盘目录（默认开启） */
  const rememberNetdiskPath = computed(() => configStore.config?.ui?.remember_netdisk_path ?? true)
  /** 多账号横向快捷条（默认关闭） */
  const accountQuickBar = computed(() => configStore.config?.ui?.account_quick_bar ?? false)
  /** 「我的分享」高亮失效分享（文件已删除 / 已过期），默认开启 */
  const highlightDeadShares = computed(() => configStore.config?.ui?.highlight_dead_shares ?? true)
  /** 失效分享只保留「取消分享」，默认关闭 */
  const disableDeadShareActions = computed(
      () => configStore.config?.ui?.disable_dead_share_actions ?? false,
  )

  let loadPromise: Promise<void> | null = null

  /** 确保配置已加载（多处调用只请求一次；失败后允许重试） */
  function ensureLoaded(): Promise<void> {
    if (configStore.config) return Promise.resolve()
    if (!loadPromise) {
      loadPromise = configStore.fetchConfig().then(
          () => undefined,
          () => {
            loadPromise = null
          },
      )
    }
    return loadPromise
  }

  // 按账号记录的网盘最近目录；uid 未知时以 0 作键
  const lastNetdiskDirs = new Map<number, string>()

  function getLastNetdiskDir(uid: number | null): string | null {
    return lastNetdiskDirs.get(uid ?? 0) ?? null
  }

  function setLastNetdiskDir(uid: number | null, dir: string) {
    lastNetdiskDirs.set(uid ?? 0, dir)
  }

  return {
    rememberNetdiskPath,
    accountQuickBar,
    highlightDeadShares,
    disableDeadShareActions,
    ensureLoaded,
    getLastNetdiskDir,
    setLastNetdiskDir,
  }
})
