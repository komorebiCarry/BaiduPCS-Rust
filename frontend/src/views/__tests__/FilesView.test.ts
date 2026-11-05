import { flushPromises, mount } from '@vue/test-utils'
import { defineComponent, h, nextTick, ref } from 'vue'
import ElementPlus from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import FilesView from '../FilesView.vue'

// 默认桌面端；需要移动端布局的用例在 beforeEach 中切换
const responsive = vi.hoisted(() => ({ isMobile: false }))

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(responsive.isMobile),
}))

// 默认单账号（不显示跨账号菜单）；跨账号用例临时切成多账号
const auth = vi.hoisted(() => ({
  hasMultipleAccounts: false,
  activeUid: 1 as number | null,
  accounts: [] as Array<{ uid: number; username: string; nickname: string | null }>,
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => auth,
}))

// 路由：默认无定位 query；定位用例临时写入
const routeState = vi.hoisted(() => ({ path: '/files', query: {} as Record<string, string> }))
const routerMock = vi.hoisted(() => ({ replace: vi.fn() }))

vi.mock('vue-router', () => ({
  useRoute: () => routeState,
  useRouter: () => routerMock,
}))

// 界面偏好：默认开启目录记忆，记忆表每个用例前清空
const uiPrefs = vi.hoisted(() => {
  const dirs = new Map<number, string>()
  return {
    rememberNetdiskPath: true,
    dirs,
    getLastNetdiskDir: (uid: number | null) => dirs.get(uid ?? 0) ?? null,
    setLastNetdiskDir: (uid: number | null, dir: string) => {
      dirs.set(uid ?? 0, dir)
    },
  }
})

vi.mock('@/stores/uiPrefs', () => ({
  useUiPrefsStore: () => uiPrefs,
}))

beforeEach(() => {
  uiPrefs.dirs.clear()
  uiPrefs.rememberNetdiskPath = true
  routeState.query = {}
  routerMock.replace.mockReset()
})

vi.mock('@/api/file', () => ({
  getFileList: vi.fn().mockResolvedValue({
    list: [],
    has_more: false,
    page: 1,
  }),
  searchFiles: vi.fn().mockResolvedValue({
    list: [],
    has_more: false,
  }),
  formatFileSize: vi.fn((size: number) => `${size}B`),
  formatTime: vi.fn(() => '2026-04-10 00:00:00'),
  createFolder: vi.fn(),
  renameFile: vi.fn(),
  joinPath: vi.fn((parent: string, name: string) => (parent === '/' ? `/${name}` : `${parent}/${name}`)),
  basename: vi.fn((p: string) => p.slice(p.lastIndexOf('/') + 1)),
  dirname: vi.fn((p: string) => (p.lastIndexOf('/') <= 0 ? '/' : p.slice(0, p.lastIndexOf('/')))),
  validateFilename: vi.fn(() => null),
  copyFiles: vi.fn(),
  moveFiles: vi.fn(),
  FILE_EXISTS_ERROR_CODE: -8,
  isNotFoundError: vi.fn((e: unknown) => /API error -9\b/.test(String((e as Error)?.message ?? e))),
  LOCATE_MAX_PAGES: 20,
}))

vi.mock('@/api/download', () => ({
  createDownload: vi.fn(),
  createFolderDownload: vi.fn(),
  createBatchDownload: vi.fn(),
}))

vi.mock('@/api/upload', () => ({
  createUpload: vi.fn(),
  createFolderUpload: vi.fn(),
}))

vi.mock('@/api/config', () => ({
  getConfig: vi.fn().mockResolvedValue({
    download: {
      recent_directory: '',
      default_directory: '',
      download_dir: 'downloads',
      ask_each_time: false,
    },
    upload: {
      recent_directory: '',
    },
    conflict_strategy: {
      default_upload_strategy: 'smart_dedup',
      default_download_strategy: 'overwrite',
    },
  }),
  updateRecentDirDebounced: vi.fn(),
  setDefaultDownloadDir: vi.fn(),
}))

vi.mock('@/api/autobackup', () => ({
  getEncryptionStatus: vi.fn().mockResolvedValue({
    has_key: false,
  }),
}))

// 搜索浮层（.search-flyout-toggle / .search-flyout-panel）只存在于移动端工具栏，
// 桌面端是常驻搜索框
describe('FilesView 移动端搜索浮层', () => {
  beforeEach(() => {
    responsive.isMobile = true
  })
  afterEach(() => {
    responsive.isMobile = false
  })

  it('展开搜索时保留图标按钮，并在空输入时点击外部自动收起', async () => {
    const wrapper = mount(FilesView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        stubs: {
          HomeFilled: true,
          Search: true,
          Download: true,
          Link: true,
          FolderAdd: true,
          Upload: true,
          Share: true,
          Refresh: true,
          Folder: true,
          Document: true,
          Loading: true,
          'el-table': true,
          'el-table-column': true,
          FilePickerModal: true,
          TransferDialog: true,
          ShareDialog: true,
          ShareDirectDownloadDialog: true,
        },
      },
    })

    await Promise.resolve()
    await Promise.resolve()
    await nextTick()

    const toggle = wrapper.find('.search-flyout-toggle')
    expect(toggle.exists()).toBe(true)

    await toggle.trigger('click')
    await nextTick()

    expect(wrapper.find('.search-flyout-toggle').exists()).toBe(true)
    expect(wrapper.find('.search-flyout-panel').exists()).toBe(true)

    document.body.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }))
    await nextTick()

    expect(wrapper.find('.search-flyout-panel').exists()).toBe(false)

    wrapper.unmount()
  })
})

describe('FilesView 路径面包屑', () => {
  function mountFilesView() {
    return mount(FilesView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        stubs: {
          HomeFilled: true,
          Search: true,
          Download: true,
          Link: true,
          FolderAdd: true,
          Upload: true,
          Share: true,
          Refresh: true,
          Folder: true,
          Document: true,
          Loading: true,
          'el-table': true,
          'el-table-column': true,
          FilePickerModal: true,
          TransferDialog: true,
          ShareDialog: true,
          ShareDirectDownloadDialog: true,
        },
      },
    })
  }

  it('只给当前层级打 is-last，中间层级保留分隔符', async () => {
    const wrapper = mountFilesView()
    await nextTick()

    // @ts-expect-error setup 内部状态，测试中直接驱动
    wrapper.vm.currentDir = '/产品/播德海/档案数字化加工平台'
    await nextTick()

    const items = wrapper.findAll('.breadcrumb-bar .el-breadcrumb__item')
    // 根目录 + 三级路径
    expect(items).toHaveLength(4)
    expect(items.map(i => i.classes('is-last'))).toEqual([false, false, false, true])
    // 分隔符始终在 DOM 中，可见性由 is-last 类决定，不依赖 :last-child
    expect(wrapper.findAll('.breadcrumb-bar .el-breadcrumb__separator')).toHaveLength(4)

    wrapper.unmount()
  })

  it('点击中间层级跳转，点击分隔符不跳转', async () => {
    const { getFileList } = await import('@/api/file')
    const wrapper = mountFilesView()
    await nextTick()

    // @ts-expect-error setup 内部状态，测试中直接驱动
    wrapper.vm.currentDir = '/产品/播德海/档案数字化加工平台'
    await nextTick()

    const items = wrapper.findAll('.breadcrumb-bar .el-breadcrumb__item')
    vi.mocked(getFileList).mockClear()

    // 分隔符不是点击热区
    await items[1].find('.el-breadcrumb__separator').trigger('click')
    expect(getFileList).not.toHaveBeenCalled()

    // 层级文字可点击，跳到对应层级
    await items[1].find('.crumb-link').trigger('click')
    await nextTick()
    expect(getFileList).toHaveBeenCalled()
    expect(vi.mocked(getFileList).mock.calls[0][0]).toBe('/产品')

    wrapper.unmount()
  })
})

describe('FilesView 右键菜单', () => {
  // el-table 桩：暴露右键改选中所需的实例方法并记录调用
  const tableCalls: unknown[][] = []
  const ElTableStub = defineComponent({
    setup(_, { expose }) {
      expose({
        clearSelection: () => tableCalls.push(['clear']),
        toggleRowSelection: (row: { fs_id: number }, selected: boolean) =>
          tableCalls.push(['toggle', row.fs_id, selected]),
      })
      return () => h('div')
    },
  })

  function mountFilesView() {
    tableCalls.length = 0
    return mount(FilesView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        stubs: {
          'el-table': ElTableStub,
          'el-table-column': true,
          FilePickerModal: true,
          TransferDialog: true,
          ShareDialog: true,
          ShareDirectDownloadDialog: true,
          NetdiskFolderPickerModal: true,
        },
      },
    })
  }

  function makeFile(overrides: Record<string, unknown> = {}) {
    return {
      fs_id: 1,
      path: '/docs/a.txt',
      server_filename: 'a.txt',
      size: 10,
      isdir: 0,
      category: 4,
      server_ctime: 0,
      server_mtime: 0,
      local_ctime: 0,
      local_mtime: 0,
      is_encrypted: false,
      is_encrypted_folder: false,
      ...overrides,
    }
  }

  function menuItems() {
    return Array.from(document.body.querySelectorAll<HTMLElement>('.file-context-menu__item'))
  }

  function menuLabels() {
    return menuItems().map((el) => el.textContent?.trim())
  }

  function clickMenuItem(label: string) {
    const item = menuItems().find((el) => el.textContent?.trim() === label)
    expect(item, `菜单项 ${label}`).toBeTruthy()
    item!.click()
  }

  function contextEvent() {
    return new MouseEvent('contextmenu', { bubbles: true, cancelable: true, clientX: 50, clientY: 60 })
  }

  it('空白处右键显示目录级菜单，并阻止浏览器原生菜单', async () => {
    const wrapper = mountFilesView()
    await nextTick()

    const event = contextEvent()
    wrapper.find('.file-list').element.dispatchEvent(event)
    await nextTick()

    expect(event.defaultPrevented).toBe(true)
    expect(menuLabels()).toEqual(['新建文件夹', '上传', '刷新'])

    clickMenuItem('新建文件夹')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.createFolderDialogVisible).toBe(true)
    expect(document.body.querySelector('.file-context-menu')).toBeNull()

    wrapper.unmount()
  })

  it('右键文件夹行：显示完整菜单，点击重命名打开对话框', async () => {
    const wrapper = mountFilesView()
    await nextTick()

    const folder = makeFile({ fs_id: 2, path: '/docs/photos', server_filename: 'photos', isdir: 1 })
    const event = contextEvent()
    // @ts-expect-error setup 内部方法（el-table 已 stub，直接驱动 row-contextmenu 处理器）
    wrapper.vm.handleRowContextMenu(folder, null, event)
    await nextTick()

    expect(event.defaultPrevented).toBe(true)
    // 右键未选中的行：改为只选中该行
    expect(tableCalls).toEqual([['clear'], ['toggle', 2, true]])
    expect(menuLabels()).toEqual(['打开', '下载', '分享', '复制到...', '移动到...', '重命名', '删除'])

    clickMenuItem('重命名')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.renameDialogVisible).toBe(true)
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.renameTarget.path).toBe('/docs/photos')

    wrapper.unmount()
  })

  it('加密文件的复制/移动/重命名置灰', async () => {
    const wrapper = mountFilesView()
    await nextTick()

    const encrypted = makeFile({ is_encrypted: true, original_name: 'secret.txt' })
    // @ts-expect-error setup 内部方法
    wrapper.vm.handleRowContextMenu(encrypted, null, contextEvent())
    await nextTick()

    const disabled = menuItems().filter((el) => el.classList.contains('is-disabled')).map((el) => el.textContent?.trim())
    expect(disabled).toEqual(['复制到...', '移动到...', '重命名'])

    wrapper.unmount()
  })

  it('右键已选中的行作用于整个选区（无重命名），点击分享带上全部选中项', async () => {
    const wrapper = mountFilesView()
    await nextTick()

    const a = makeFile({ fs_id: 1, path: '/docs/a.txt', server_filename: 'a.txt' })
    const b = makeFile({ fs_id: 3, path: '/docs/b.txt', server_filename: 'b.txt' })
    // @ts-expect-error setup 内部方法
    wrapper.vm.handleSelectionChange([a, b])
    // @ts-expect-error setup 内部方法
    wrapper.vm.handleRowContextMenu(b, null, contextEvent())
    await nextTick()

    // 右键已选中的行：保留原选区
    expect(tableCalls).toEqual([])
    expect(menuLabels()).toEqual(['下载', '分享', '复制到...', '移动到...', '删除'])

    clickMenuItem('分享')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.shareFiles.map((f: { fs_id: number }) => f.fs_id)).toEqual([1, 3])
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.showShareDialog).toBe(true)

    wrapper.unmount()
  })

  describe('多账号', () => {
    beforeEach(() => {
      auth.hasMultipleAccounts = true
      auth.accounts = [
        { uid: 1, username: 'a', nickname: null },
        { uid: 2, username: 'b', nickname: null },
      ]
    })
    afterEach(() => {
      auth.hasMultipleAccounts = false
      auth.accounts = []
    })

    it('显示"复制/移动到其他账号..."，点击后以右键对象打开跨账号对话框', async () => {
      const wrapper = mountFilesView()
      await nextTick()

      const file = makeFile()
      // @ts-expect-error setup 内部方法
      wrapper.vm.handleRowContextMenu(file, null, contextEvent())
      await nextTick()
      expect(menuLabels()).toEqual([
        '下载', '用其他账号下载...', '分享', '复制到...', '移动到...', '复制/移动到其他账号...', '重命名', '删除',
      ])

      clickMenuItem('复制/移动到其他账号...')
      await nextTick()
      // @ts-expect-error setup 内部状态
      expect(wrapper.vm.crossAccountVisible).toBe(true)
      // @ts-expect-error setup 内部状态
      expect(wrapper.vm.crossAccountFiles.map((f: { path: string }) => f.path)).toEqual(['/docs/a.txt'])
      // @ts-expect-error setup 内部状态
      expect(wrapper.vm.crossAccountMode).toBe('copy')

      wrapper.unmount()
    })

    it('"用其他账号下载..."以下载模式打开跨账号对话框', async () => {
      const wrapper = mountFilesView()
      await nextTick()

      // @ts-expect-error setup 内部方法
      wrapper.vm.handleRowContextMenu(makeFile(), null, contextEvent())
      await nextTick()
      clickMenuItem('用其他账号下载...')
      await nextTick()
      // @ts-expect-error setup 内部状态
      expect(wrapper.vm.crossAccountVisible).toBe(true)
      // @ts-expect-error setup 内部状态
      expect(wrapper.vm.crossAccountMode).toBe('download')
      // @ts-expect-error setup 内部状态
      expect(wrapper.vm.crossAccountFiles.map((f: { path: string }) => f.path)).toEqual(['/docs/a.txt'])

      wrapper.unmount()
    })

    it('加密文件的跨账号复制/移动/下载置灰', async () => {
      const wrapper = mountFilesView()
      await nextTick()

      const encrypted = makeFile({ is_encrypted: true, original_name: 'secret.txt' })
      // @ts-expect-error setup 内部方法
      wrapper.vm.handleRowContextMenu(encrypted, null, contextEvent())
      await nextTick()

      const disabled = menuItems().filter((el) => el.classList.contains('is-disabled')).map((el) => el.textContent?.trim())
      expect(disabled).toEqual(['用其他账号下载...', '复制到...', '移动到...', '复制/移动到其他账号...', '重命名'])

      wrapper.unmount()
    })

    it('跨目录（搜索结果）或父子重叠的选中项不打开跨账号对话框', async () => {
      const wrapper = mountFilesView()
      await nextTick()

      const a = makeFile({ fs_id: 1, path: '/x/a.txt', server_filename: 'a.txt' })
      const b = makeFile({ fs_id: 2, path: '/y/a.txt', server_filename: 'a.txt' })
      // @ts-expect-error setup 内部方法
      wrapper.vm.openCrossAccountDialog([a, b])
      await nextTick()
      // @ts-expect-error setup 内部状态
      expect(wrapper.vm.crossAccountVisible).toBe(false)

      const dir = makeFile({ fs_id: 3, path: '/d', server_filename: 'd', isdir: 1 })
      const child = makeFile({ fs_id: 4, path: '/d/a.txt', server_filename: 'a.txt' })
      // @ts-expect-error setup 内部方法
      wrapper.vm.openCrossAccountDialog([dir, child])
      await nextTick()
      // @ts-expect-error setup 内部状态
      expect(wrapper.vm.crossAccountVisible).toBe(false)

      wrapper.unmount()
    })

    it('跨账号移动删除了源文件时刷新列表，仅复制时不刷新', async () => {
      const wrapper = mountFilesView()
      await nextTick()
      const { getFileList } = await import('@/api/file')
      const before = vi.mocked(getFileList).mock.calls.length

      // @ts-expect-error setup 内部方法
      wrapper.vm.handleCrossAccountFinished({ source_deleted: false })
      await nextTick()
      expect(vi.mocked(getFileList).mock.calls.length).toBe(before)

      // @ts-expect-error setup 内部方法
      wrapper.vm.handleCrossAccountFinished({ source_deleted: true })
      await nextTick()
      expect(vi.mocked(getFileList).mock.calls.length).toBeGreaterThan(before)

      wrapper.unmount()
    })
  })

  it('右键菜单"移动到..."以右键对象为源，并禁选源父目录与文件夹自身子树', async () => {
    const wrapper = mountFilesView()
    await nextTick()

    const folder = makeFile({ fs_id: 2, path: '/docs/photos', server_filename: 'photos', isdir: 1 })
    // @ts-expect-error setup 内部方法
    wrapper.vm.handleRowContextMenu(folder, null, contextEvent())
    await nextTick()
    clickMenuItem('移动到...')
    await nextTick()

    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.showFolderPicker).toBe(true)
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.operationFiles.map((f: { path: string }) => f.path)).toEqual(['/docs/photos'])
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.folderPickerBlockedPaths).toEqual(['/docs/photos'])
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.folderPickerBlockedExactPaths).toEqual(['/docs'])

    wrapper.unmount()
  })
})

describe('FilesView 复制/移动同名冲突', () => {
  function mountFilesView() {
    return mount(FilesView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        stubs: {
          'el-table': true,
          'el-table-column': true,
          FilePickerModal: true,
          TransferDialog: true,
          ShareDialog: true,
          ShareDirectDownloadDialog: true,
          NetdiskFolderPickerModal: true,
        },
      },
    })
  }

  function file(path: string, isdir = 0) {
    const name = path.slice(path.lastIndexOf('/') + 1)
    return {
      fs_id: path.length, path, server_filename: name, size: 1, isdir, category: 1,
      server_ctime: 0, server_mtime: 0, local_ctime: 0, local_mtime: 0,
      is_encrypted: false, is_encrypted_folder: false,
    }
  }

  const conflictOutcome = {
    kind: 'failed',
    message: '任务失败: task_errno=-30',
    taskid: 1,
    errno: -30,
    task_errno: -30,
    still_running: false,
    failed_items: [
      { from: '/src/f.txt', to: '/dst/f.txt', error_code: -8, isdir: false },
      { from: '/src/dirA', to: '/dst/dirA', error_code: -8, isdir: true },
    ],
  }

  async function flush() {
    for (let i = 0; i < 5; i++) {
      await Promise.resolve()
      await nextTick()
    }
  }

  function footerButton(label: string): HTMLButtonElement {
    const btn = Array.from(document.body.querySelectorAll<HTMLButtonElement>('.el-dialog__footer button'))
        .find((b) => b.textContent?.trim() === label)
    expect(btn, `按钮 ${label}`).toBeTruthy()
    return btn!
  }

  async function startCopy(wrapper: ReturnType<typeof mountFilesView>) {
    // @ts-expect-error setup 内部状态
    wrapper.vm.operationFiles = [file('/src/f.txt'), file('/src/dirA', 1), file('/src/g.txt')]
    // @ts-expect-error setup 内部状态
    wrapper.vm.folderPickerOperation = 'copy'
    // @ts-expect-error setup 内部方法
    const done = wrapper.vm.handleFolderPicked('/dst') as Promise<void>
    await flush()
    // 包一层对象返回：async 函数直接 return Promise 会被展开，导致等待整个操作结束
    return { done }
  }

  it('冲突时弹窗，选择保留两者后只重提冲突项', async () => {
    const { copyFiles } = await import('@/api/file')
    vi.mocked(copyFiles).mockReset()
    vi.mocked(copyFiles)
        .mockResolvedValueOnce(conflictOutcome as never)
        .mockResolvedValueOnce({ kind: 'success', taskid: 2, total: 2, list: [] } as never)

    const wrapper = mountFilesView()
    await nextTick()
    const { done } = await startCopy(wrapper)

    expect(vi.mocked(copyFiles).mock.calls[0]).toEqual([[
      { path: '/src/f.txt', dest: '/dst', newname: 'f.txt' },
      { path: '/src/dirA', dest: '/dst', newname: 'dirA' },
      { path: '/src/g.txt', dest: '/dst', newname: 'g.txt' },
    ]])
    expect(Array.from(document.body.querySelectorAll('.conflict-name')).map((e) => e.textContent))
        .toEqual(['f.txt', 'dirA'])
    expect(document.body.querySelector('.conflict-done')?.textContent).toContain('其余 1 项')
    // 含文件夹冲突，不可覆盖
    expect(footerButton('覆盖').disabled).toBe(true)

    footerButton('保留两者').click()
    await done

    expect(vi.mocked(copyFiles).mock.calls[1]).toEqual([
      [
        { path: '/src/f.txt', dest: '/dst', newname: 'f.txt' },
        { path: '/src/dirA', dest: '/dst', newname: 'dirA' },
      ],
      'newcopy',
    ])

    wrapper.unmount()
  })

  it('选择跳过时不再提交', async () => {
    const { copyFiles } = await import('@/api/file')
    vi.mocked(copyFiles).mockReset()
    vi.mocked(copyFiles).mockResolvedValueOnce(conflictOutcome as never)

    const wrapper = mountFilesView()
    await nextTick()
    const { done } = await startCopy(wrapper)

    footerButton('跳过').click()
    await done

    expect(copyFiles).toHaveBeenCalledTimes(1)
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.batchCopying).toBe(false)

    wrapper.unmount()
  })
})

describe('FilesView 目录记忆与定位', () => {
  function mountFilesView() {
    return mount(FilesView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        stubs: {
          'el-table': true,
          'el-table-column': true,
          FilePickerModal: true,
          TransferDialog: true,
          ShareDialog: true,
          ShareDirectDownloadDialog: true,
          NetdiskFolderPickerModal: true,
        },
      },
    })
  }

  function file(path: string) {
    const name = path.slice(path.lastIndexOf('/') + 1)
    return {
      fs_id: path.length,
      path,
      server_filename: name,
      size: 1,
      isdir: 0,
      category: 4,
      server_ctime: 0,
      server_mtime: 0,
      local_ctime: 0,
      local_mtime: 0,
    }
  }

  async function requestedDirs() {
    const { getFileList } = await import('@/api/file')
    return vi.mocked(getFileList).mock.calls.map((c) => c[0])
  }

  beforeEach(async () => {
    const { getFileList } = await import('@/api/file')
    vi.mocked(getFileList).mockClear()
  })

  it('开启记忆时回到该账号上次浏览的目录，并记录新进入的目录', async () => {
    uiPrefs.dirs.set(1, '/a/b')
    const wrapper = mountFilesView()
    await flushPromises()
    expect(await requestedDirs()).toEqual(['/a/b'])

    // @ts-expect-error setup 内部方法
    wrapper.vm.navigateToDir('/c')
    await flushPromises()
    expect(uiPrefs.dirs.get(1)).toBe('/c')

    wrapper.unmount()
  })

  it('关闭记忆时始终从根目录开始', async () => {
    uiPrefs.rememberNetdiskPath = false
    uiPrefs.dirs.set(1, '/a/b')
    const wrapper = mountFilesView()
    await flushPromises()
    expect(await requestedDirs()).toEqual(['/'])
    wrapper.unmount()
  })

  it('记忆的目录已不存在时回退到最近的上级目录', async () => {
    const { getFileList } = await import('@/api/file')
    vi.mocked(getFileList)
        .mockRejectedValueOnce(new Error('获取文件列表失败: API error -9: '))
        .mockRejectedValueOnce(new Error('获取文件列表失败: API error -9: '))
    uiPrefs.dirs.set(1, '/a/b/gone')
    const wrapper = mountFilesView()
    await flushPromises()
    expect(await requestedDirs()).toEqual(['/a/b/gone', '/a/b', '/a'])
    expect(uiPrefs.dirs.get(1)).toBe('/a')
    wrapper.unmount()
  })

  it('非「目录不存在」的错误不逐级上探，直接回到根目录', async () => {
    const { getFileList } = await import('@/api/file')
    vi.mocked(getFileList)
        .mockRejectedValueOnce(new Error('获取文件列表失败: API error -9: '))
        .mockRejectedValueOnce(new Error('网络错误'))
    uiPrefs.dirs.set(1, '/a/b/gone')
    const wrapper = mountFilesView()
    await flushPromises()
    expect(await requestedDirs()).toEqual(['/a/b/gone', '/a/b', '/'])
    wrapper.unmount()
  })

  it('定位的上传目录已不存在：打开最近的上级目录', async () => {
    const { getFileList } = await import('@/api/file')
    vi.mocked(getFileList).mockRejectedValueOnce(new Error('获取文件列表失败: API error -9: '))
    routeState.query = { path: '/测试2-1', highlight: '/测试2-1/dummy_1.txt' }
    const wrapper = mountFilesView()
    await flushPromises()
    expect(await requestedDirs()).toEqual(['/测试2-1', '/'])
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.currentDir).toBe('/')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.locatedPath).toBeNull()
    wrapper.unmount()
  })

  it('定位请求：打开目标目录，翻页找到文件后高亮，并清掉 query', async () => {
    const { getFileList } = await import('@/api/file')
    vi.mocked(getFileList)
        .mockResolvedValueOnce({ list: [file('/up/a.txt')], has_more: true, page: 1 } as any)
        .mockResolvedValueOnce({ list: [file('/up/b.txt')], has_more: false, page: 2 } as any)
    uiPrefs.dirs.set(1, '/elsewhere')
    routeState.query = { path: '/up', highlight: '/up/b.txt' }

    const wrapper = mountFilesView()
    await flushPromises()

    expect(await requestedDirs()).toEqual(['/up', '/up'])
    expect(vi.mocked(getFileList).mock.calls.map((c) => c[1])).toEqual([1, 2])
    expect(routerMock.replace).toHaveBeenCalledWith({ path: '/files' })
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.locatedPath).toBe('/up/b.txt')

    // 切换目录后高亮清除
    // @ts-expect-error setup 内部方法
    wrapper.vm.navigateToDir('/')
    await flushPromises()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.locatedPath).toBeNull()

    wrapper.unmount()
  })
})

describe('FilesView 上传跳过秒传', () => {
  function mountFilesView() {
    return mount(FilesView, {
      global: {
        plugins: [ElementPlus],
        stubs: {
          'el-table': true,
          'el-table-column': true,
          FilePickerModal: true,
          TransferDialog: true,
          ShareDialog: true,
          ShareDirectDownloadDialog: true,
        },
      },
    })
  }

  it('上传对话框显示开关，默认值取自上传设置（缺省为 false）', async () => {
    const wrapper = mountFilesView()
    await flushPromises()
    const picker = wrapper
      .findAllComponents({ name: 'FilePickerModal' })
      .find((c) => c.props('title') === '选择上传文件')!
    expect(picker).toBeTruthy()
    expect(picker.props('showSkipRapid')).toBe(true)
    expect(picker.props('defaultSkipRapidUpload')).toBe(false)
    wrapper.unmount()

    // 设置里开启了"默认跳过秒传"
    const { getConfig } = await import('@/api/config')
    const base = await vi.mocked(getConfig)()
    vi.mocked(getConfig).mockResolvedValueOnce({
      ...base,
      upload: { ...base.upload, skip_rapid_upload: true },
    } as any)
    const wrapper2 = mountFilesView()
    await flushPromises()
    const picker2 = wrapper2
      .findAllComponents({ name: 'FilePickerModal' })
      .find((c) => c.props('title') === '选择上传文件')!
    expect(picker2.props('defaultSkipRapidUpload')).toBe(true)
    wrapper2.unmount()
  })

  it('把对话框里的选择透传给单文件与文件夹上传接口', async () => {
    const { createUpload, createFolderUpload } = await import('@/api/upload')
    vi.mocked(createUpload).mockClear()
    vi.mocked(createFolderUpload).mockClear()
    const wrapper = mountFilesView()
    await flushPromises()
    const vm = wrapper.vm as any

    const file = { id: 'f', name: 'a.zip', entryType: 'file', size: 1, createdAt: '', updatedAt: '', path: '/d/a.zip' }
    const dir = { id: 'd', name: 'pics', entryType: 'directory', size: null, createdAt: '', updatedAt: '', path: '/d/pics' }

    await vm.handleFilePickerSelect(file, false, 'smart_dedup', true)
    expect(vi.mocked(createUpload).mock.calls[0][0]).toMatchObject({ local_path: '/d/a.zip', skip_rapid_upload: true })

    await vm.handleFilePickerMultiSelect([file, dir], false, 'smart_dedup', false)
    expect(vi.mocked(createUpload).mock.calls[1][0]).toMatchObject({ skip_rapid_upload: false })
    expect(vi.mocked(createFolderUpload).mock.calls[0][0]).toMatchObject({ local_folder: '/d/pics', skip_rapid_upload: false })

    // 未传（旧调用方式）→ 不带该字段，由后端跟随全局设置
    await vm.handleFilePickerSelect(file, false)
    expect(vi.mocked(createUpload).mock.calls[2][0].skip_rapid_upload).toBeUndefined()
    wrapper.unmount()
  })
})

describe('FilesView 新建文件夹/重命名 回车提交', () => {
  // 回归：对话框里的 el-form 只有一个输入框，回车会触发浏览器"隐式提交"→ 整页刷新回到根目录，
  // 而且 @keyup.enter 在刷新开始后才触发，请求发不出去。修复后回车走表单 submit（已 preventDefault）。
  function mountFilesView() {
    return mount(FilesView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        stubs: {
          'el-table': true,
          'el-table-column': true,
          FilePickerModal: true,
          TransferDialog: true,
          ShareDialog: true,
          ShareDirectDownloadDialog: true,
          NetdiskFolderPickerModal: true,
        },
      },
    })
  }

  async function flush() {
    for (let i = 0; i < 5; i++) {
      await Promise.resolve()
      await nextTick()
    }
  }

  function dialogForm(placeholder: string) {
    const input = document.body.querySelector<HTMLInputElement>(`input[placeholder="${placeholder}"]`)
    expect(input, `输入框 ${placeholder}`).toBeTruthy()
    const form = input!.closest('form')
    expect(form, `${placeholder} 所在的 form`).toBeTruthy()
    return { input: input!, form: form! }
  }

  function submit(form: HTMLFormElement) {
    const event = new Event('submit', { bubbles: true, cancelable: true })
    form.dispatchEvent(event)
    return event
  }

  afterEach(() => {
    document.body.innerHTML = ''
  })

  it('新建文件夹：表单提交（回车）阻止原生刷新并创建到当前目录，进行中不重复提交', async () => {
    const { createFolder } = await import('@/api/file')
    vi.mocked(createFolder).mockReset()
    let resolveCreate!: () => void
    vi.mocked(createFolder).mockImplementation(
      () => new Promise((resolve) => { resolveCreate = () => resolve({} as never) }),
    )

    const wrapper = mountFilesView()
    await flush()
    // @ts-expect-error setup 内部状态
    wrapper.vm.currentDir = '/sub'
    // @ts-expect-error setup 内部状态
    wrapper.vm.createFolderDialogVisible = true
    await flush()
    // @ts-expect-error setup 内部状态
    wrapper.vm.createFolderForm.folderName = 'newdir'
    await flush()

    const { input, form } = dialogForm('请输入文件夹名称')
    // 输入框上不再挂 keyup.enter（否则回车会与表单 submit 重复触发）
    input.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true }))
    await flush()
    expect(createFolder).not.toHaveBeenCalled()

    const event = submit(form)
    expect(event.defaultPrevented).toBe(true)
    submit(form) // 请求未返回时再次回车
    await flush()
    expect(createFolder).toHaveBeenCalledTimes(1)
    expect(createFolder).toHaveBeenCalledWith('/sub/newdir')

    resolveCreate()
    await flush()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.createFolderDialogVisible).toBe(false)
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.currentDir).toBe('/sub')
    wrapper.unmount()
  })

  it('新建文件夹：名称为空时回车不提交、不刷新', async () => {
    const { createFolder } = await import('@/api/file')
    vi.mocked(createFolder).mockReset()
    const wrapper = mountFilesView()
    await flush()
    // @ts-expect-error setup 内部状态
    wrapper.vm.createFolderDialogVisible = true
    await flush()
    const event = submit(dialogForm('请输入文件夹名称').form)
    await flush()
    expect(event.defaultPrevented).toBe(true)
    expect(createFolder).not.toHaveBeenCalled()
    wrapper.unmount()
  })

  it('重命名：表单提交（回车）阻止原生刷新并调用重命名，进行中不重复提交', async () => {
    const { renameFile } = await import('@/api/file')
    vi.mocked(renameFile).mockReset()
    let resolveRename!: () => void
    vi.mocked(renameFile).mockImplementation(
      () => new Promise((resolve) => {
        resolveRename = () => resolve({ kind: 'success', taskid: null, total: 1, list: [] } as never)
      }),
    )

    const wrapper = mountFilesView()
    await flush()
    // @ts-expect-error setup 内部方法
    wrapper.vm.handleSingleRename({
      fs_id: 7, path: '/sub/old.txt', server_filename: 'old.txt', size: 1, isdir: 0, category: 6,
      server_ctime: 0, server_mtime: 0, local_ctime: 0, local_mtime: 0,
      is_encrypted: false, is_encrypted_folder: false,
    })
    await flush()
    // @ts-expect-error setup 内部状态
    wrapper.vm.renameNewName = 'new.txt'
    await flush()

    const { input, form } = dialogForm('输入新名称')
    input.dispatchEvent(new KeyboardEvent('keyup', { key: 'Enter', bubbles: true }))
    await flush()
    expect(renameFile).not.toHaveBeenCalled()

    const event = submit(form)
    expect(event.defaultPrevented).toBe(true)
    submit(form)
    await flush()
    expect(renameFile).toHaveBeenCalledTimes(1)
    expect(renameFile).toHaveBeenCalledWith({ path: '/sub/old.txt', newname: 'new.txt', id: 7 })

    resolveRename()
    await flush()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.renameDialogVisible).toBe(false)
    wrapper.unmount()
  })
})
