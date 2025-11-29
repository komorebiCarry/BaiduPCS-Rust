import { defineStore } from 'pinia'
import { ref, computed } from 'vue'
import type { FileEntry, SortField, SortOrder } from '@/api/filesystem'
import { listDirectory, getRoots, gotoPath } from '@/api/filesystem'

export const useFilePickerStore = defineStore('filepicker', () => {
  // 状态
  const currentPath = ref<string>('')
  const entries = ref<FileEntry[]>([])
  const selection = ref<FileEntry | null>(null)
  const viewMode = ref<'grid' | 'list'>('list')
  const sortField = ref<SortField>('name')
  const sortOrder = ref<SortOrder>('asc')
  const historyStack = ref<string[]>([])
  const forwardStack = ref<string[]>([])
  const loading = ref(false)
  const error = ref<string | null>(null)

  // 分页状态
  const page = ref(0)
  const pageSize = ref(100)
  const total = ref(0)
  const hasMore = ref(false)

  // 计算属性
  const canGoBack = computed(() => historyStack.value.length > 0)
  const canGoForward = computed(() => forwardStack.value.length > 0)
  const parentPath = ref<string | null>(null)
  const isRoot = computed(() => !parentPath.value)

  // 加载目录
  async function loadDirectory(path: string, pushHistory = true) {
    if (loading.value) return

    loading.value = true
    error.value = null

    try {
      // 如果是空路径，获取根目录列表
      if (!path) {
        const roots = await getRoots()
        entries.value = roots
        currentPath.value = ''
        parentPath.value = null
        total.value = roots.length
        page.value = 0
        hasMore.value = false
      } else {
        const response = await listDirectory({
          path,
          page: 0,
          page_size: pageSize.value,
          sort_field: sortField.value,
          sort_order: sortOrder.value,
        })

        entries.value = response.entries
        currentPath.value = response.currentPath
        parentPath.value = response.parentPath
        total.value = response.total
        page.value = response.page
        hasMore.value = response.hasMore
      }

      // 清除选择
      selection.value = null

      // 历史记录管理
      if (pushHistory && currentPath.value !== path) {
        if (path) {
          historyStack.value.push(path)
        }
        forwardStack.value = []
      }
    } catch (e: any) {
      error.value = e.message || '加载目录失败'
      console.error('加载目录失败:', e)
    } finally {
      loading.value = false
    }
  }

  // 加载更多
  async function loadMore() {
    if (loading.value || !hasMore.value) return

    loading.value = true

    try {
      const response = await listDirectory({
        path: currentPath.value,
        page: page.value + 1,
        page_size: pageSize.value,
        sort_field: sortField.value,
        sort_order: sortOrder.value,
      })

      entries.value = [...entries.value, ...response.entries]
      page.value = response.page
      hasMore.value = response.hasMore
    } catch (e: any) {
      console.error('加载更多失败:', e)
    } finally {
      loading.value = false
    }
  }

  // 导航到路径
  function navigateTo(path: string) {
    if (currentPath.value) {
      historyStack.value.push(currentPath.value)
    }
    forwardStack.value = []
    loadDirectory(path, false)
  }

  // 后退
  function goBack() {
    if (!canGoBack.value) return

    forwardStack.value.push(currentPath.value)
    const prevPath = historyStack.value.pop()!
    loadDirectory(prevPath, false)
  }

  // 前进
  function goForward() {
    if (!canGoForward.value) return

    historyStack.value.push(currentPath.value)
    const nextPath = forwardStack.value.pop()!
    loadDirectory(nextPath, false)
  }

  // 刷新
  function refresh() {
    loadDirectory(currentPath.value, false)
  }

  // 进入上级目录
  function goToParent() {
    if (parentPath.value !== null) {
      navigateTo(parentPath.value)
    } else {
      // 回到根目录列表
      navigateTo('')
    }
  }

  // 直达路径
  async function jumpToPath(path: string): Promise<boolean> {
    try {
      const result = await gotoPath({ path })
      if (result.valid) {
        if (result.entryType === 'directory') {
          navigateTo(result.resolvedPath)
        } else {
          // 如果是文件，导航到其父目录并选中
          const lastSep = result.resolvedPath.lastIndexOf('\\')
          const lastSepUnix = result.resolvedPath.lastIndexOf('/')
          const sep = Math.max(lastSep, lastSepUnix)
          if (sep > 0) {
            const parentDir = result.resolvedPath.substring(0, sep)
            await loadDirectory(parentDir, true)
            // 选中该文件
            const fileName = result.resolvedPath.substring(sep + 1)
            const entry = entries.value.find(e => e.name === fileName)
            if (entry) {
              selection.value = entry
            }
          }
        }
        return true
      } else {
        error.value = result.message || '路径无效'
        return false
      }
    } catch (e: any) {
      error.value = e.message || '跳转失败'
      return false
    }
  }

  // 选择条目
  function selectEntry(entry: FileEntry | null) {
    selection.value = entry
  }

  // 打开条目（双击）
  function openEntry(entry: FileEntry) {
    if (entry.entryType === 'directory') {
      navigateTo(entry.path)
    }
  }

  // 更改排序
  function changeSort(field: SortField, order?: SortOrder) {
    if (sortField.value === field && !order) {
      // 切换排序顺序
      sortOrder.value = sortOrder.value === 'asc' ? 'desc' : 'asc'
    } else {
      sortField.value = field
      sortOrder.value = order || 'asc'
    }
    refresh()
  }

  // 更改视图模式
  function changeViewMode(mode: 'grid' | 'list') {
    viewMode.value = mode
  }

  // 重置状态
  function reset() {
    currentPath.value = ''
    entries.value = []
    selection.value = null
    historyStack.value = []
    forwardStack.value = []
    loading.value = false
    error.value = null
    page.value = 0
    total.value = 0
    hasMore.value = false
    parentPath.value = null
  }

  return {
    // 状态
    currentPath,
    entries,
    selection,
    viewMode,
    sortField,
    sortOrder,
    historyStack,
    forwardStack,
    loading,
    error,
    page,
    pageSize,
    total,
    hasMore,
    parentPath,

    // 计算属性
    canGoBack,
    canGoForward,
    isRoot,

    // 方法
    loadDirectory,
    loadMore,
    navigateTo,
    goBack,
    goForward,
    refresh,
    goToParent,
    jumpToPath,
    selectEntry,
    openEntry,
    changeSort,
    changeViewMode,
    reset,
  }
})