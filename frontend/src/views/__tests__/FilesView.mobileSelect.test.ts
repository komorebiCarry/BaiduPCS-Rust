import { flushPromises, mount } from '@vue/test-utils'
import { nextTick, ref } from 'vue'
import ElementPlus from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import FilesView from '../FilesView.vue'

const responsive = vi.hoisted(() => ({ isMobile: true }))

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(responsive.isMobile),
}))

const auth = vi.hoisted(() => ({
  hasMultipleAccounts: false,
  activeUid: 1 as number | null,
  accounts: [] as Array<{ uid: number; username: string; nickname: string | null }>,
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => auth,
}))

const routeState = vi.hoisted(() => ({ path: '/files', query: {} as Record<string, string> }))
const routerMock = vi.hoisted(() => ({ replace: vi.fn() }))

vi.mock('vue-router', () => ({
  useRoute: () => routeState,
  useRouter: () => routerMock,
}))

const uiPrefs = vi.hoisted(() => {
  const dirs = new Map<number, string>()
  return {
    rememberNetdiskPath: false,
    dirs,
    getLastNetdiskDir: () => null,
    setLastNetdiskDir: (uid: number | null, dir: string) => {
      dirs.set(uid ?? 0, dir)
    },
  }
})

vi.mock('@/stores/uiPrefs', () => ({
  useUiPrefsStore: () => uiPrefs,
}))

vi.mock('@/api/file', () => ({
  getFileList: vi.fn(),
  searchFiles: vi.fn().mockResolvedValue({ list: [], has_more: false }),
  formatFileSize: vi.fn((size: number) => `${size}B`),
  formatTime: vi.fn(() => '2026-04-10 00:00:00'),
  createFolder: vi.fn(),
  renameFile: vi.fn(),
  deleteFiles: vi.fn(),
  joinPath: vi.fn((parent: string, name: string) => (parent === '/' ? `/${name}` : `${parent}/${name}`)),
  basename: vi.fn((p: string) => p.slice(p.lastIndexOf('/') + 1)),
  dirname: vi.fn((p: string) => (p.lastIndexOf('/') <= 0 ? '/' : p.slice(0, p.lastIndexOf('/')))),
  validateFilename: vi.fn(() => null),
  copyFiles: vi.fn(),
  moveFiles: vi.fn(),
  FILE_EXISTS_ERROR_CODE: -8,
  isNotFoundError: vi.fn(() => false),
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

function makeFile(overrides: Record<string, unknown> = {}) {
  return {
    fs_id: 1,
    path: '/a.txt',
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

const folder = makeFile({ fs_id: 11, path: '/剧集', server_filename: '剧集', isdir: 1, size: 0 })
const movie = makeFile({ fs_id: 22, path: '/电影.mkv', server_filename: '电影.mkv', size: 2048 })
const doc = makeFile({ fs_id: 33, path: '/文档.pdf', server_filename: '文档.pdf', size: 1024 })

const stubs = {
  'el-table': true,
  'el-table-column': true,
  FilePickerModal: true,
  TransferDialog: true,
  ShareDialog: true,
  ShareDirectDownloadDialog: true,
  NetdiskFolderPickerModal: true,
  FileConflictDialog: true,
  CrossAccountTransferDialog: true,
}

async function mountMobile() {
  const wrapper = mount(FilesView, {
    attachTo: document.body,
    global: {
      plugins: [ElementPlus],
      stubs,
    },
  })
  await flushPromises()
  await nextTick()
  return wrapper
}

function cardByName(wrapper: Awaited<ReturnType<typeof mount>>, name: string) {
  const card = wrapper.findAll('.mobile-file-card').find((c) => c.text().includes(name))
  if (!card) throw new Error(`card ${name} not found`)
  return card
}

async function clickCardCheckbox(wrapper: Awaited<ReturnType<typeof mount>>, name: string) {
  const input = cardByName(wrapper, name).find('.file-card-checkbox input')
  await input.setValue(!(input.element as HTMLInputElement).checked)
  await nextTick()
}

describe('FilesView 移动端多选', () => {
  beforeEach(async () => {
    responsive.isMobile = true
    auth.hasMultipleAccounts = false
    const { getFileList } = await import('@/api/file')
    vi.mocked(getFileList).mockReset()
    vi.mocked(getFileList).mockResolvedValue({
      list: [folder, movie, doc],
      has_more: false,
      page: 1,
      dir: '/',
      total: 3,
    })
    const { createBatchDownload } = await import('@/api/download')
    vi.mocked(createBatchDownload).mockReset()
    vi.mocked(createBatchDownload).mockResolvedValue({
      task_ids: ['t1'],
      folder_task_ids: [],
      failed: [],
    })
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  it('勾选框切换选中，底栏随之显示/隐藏', async () => {
    const wrapper = await mountMobile()
    expect(wrapper.findAll('.mobile-file-card')).toHaveLength(3)
    expect(wrapper.findAll('.file-card-checkbox')).toHaveLength(3)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)

    await clickCardCheckbox(wrapper, '电影.mkv')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles.map((f: { fs_id: number }) => f.fs_id)).toEqual([22])
    expect(cardByName(wrapper, '电影.mkv').classes()).toContain('is-selected')
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(true)
    expect(wrapper.find('.mobile-batch-bar__count').text()).toBe('已选 1 项')

    await clickCardCheckbox(wrapper, '电影.mkv')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(0)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)

    wrapper.unmount()
  })

  it('点击文件夹卡片会进入目录，且不会把它加入选中', async () => {
    const { getFileList } = await import('@/api/file')
    const wrapper = await mountMobile()
    vi.mocked(getFileList).mockClear()
    vi.mocked(getFileList).mockResolvedValue({
      list: [],
      has_more: false,
      page: 1,
      dir: '/剧集',
      total: 0,
    })

    await cardByName(wrapper, '剧集').trigger('click')
    await nextTick()
    // 卡片点击只负责进目录，不应把文件夹加入选中（列表刷新前即可断言）
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles.map((f: { fs_id: number }) => f.fs_id)).not.toContain(11)

    await flushPromises()
    expect(getFileList).toHaveBeenCalled()
    expect(vi.mocked(getFileList).mock.calls[0][0]).toBe('/剧集')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(0)

    wrapper.unmount()
  })

  it('全选当前已加载项，取消会清空并收起底栏', async () => {
    const wrapper = await mountMobile()
    await clickCardCheckbox(wrapper, '电影.mkv')
    await wrapper.find('.mobile-batch-bar__select-all').trigger('click')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles.map((f: { fs_id: number }) => f.fs_id)).toEqual([11, 22, 33])
    expect(wrapper.find('.mobile-batch-bar__count').text()).toBe('已选 3 项')

    await wrapper.find('.mobile-batch-bar__clear').trigger('click')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(0)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)

    wrapper.unmount()
  })

  it('底栏操作会带着当前选中项调用批量处理', async () => {
    const { createBatchDownload } = await import('@/api/download')
    const wrapper = await mountMobile()
    await clickCardCheckbox(wrapper, '电影.mkv')
    await clickCardCheckbox(wrapper, '文档.pdf')

    await wrapper.find('.mobile-batch-bar__download').trigger('click')
    await flushPromises()
    expect(createBatchDownload).toHaveBeenCalledTimes(1)
    expect(vi.mocked(createBatchDownload).mock.calls[0][0].items.map((i: { fs_id: number }) => i.fs_id)).toEqual([22, 33])

    await clickCardCheckbox(wrapper, '电影.mkv')
    await clickCardCheckbox(wrapper, '文档.pdf')
    await wrapper.find('.mobile-batch-bar__share').trigger('click')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.showShareDialog).toBe(true)
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.shareFiles.map((f: { fs_id: number }) => f.fs_id)).toEqual([22, 33])

    await wrapper.find('.mobile-batch-bar__copy').trigger('click')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.showFolderPicker).toBe(true)
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.folderPickerOperation).toBe('copy')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.operationFiles.map((f: { fs_id: number }) => f.fs_id)).toEqual([22, 33])

    wrapper.unmount()
  })

  it('切换目录后会清空选中', async () => {
    const { getFileList } = await import('@/api/file')
    const wrapper = await mountMobile()
    await clickCardCheckbox(wrapper, '电影.mkv')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(1)

    vi.mocked(getFileList).mockResolvedValueOnce({
      list: [makeFile({ fs_id: 99, path: '/剧集/a.mkv', server_filename: 'a.mkv' })],
      has_more: false,
      page: 1,
      dir: '/剧集',
      total: 1,
    })
    await cardByName(wrapper, '剧集').trigger('click')
    await flushPromises()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(0)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)

    wrapper.unmount()
  })

  it('多账号时底栏提供「更多」入口', async () => {
    auth.hasMultipleAccounts = true
    const wrapper = await mountMobile()
    await clickCardCheckbox(wrapper, '电影.mkv')
    expect(wrapper.find('.mobile-batch-bar__more').exists()).toBe(true)
    wrapper.unmount()
  })

  it('桌面端不渲染卡片勾选和吸底栏', async () => {
    responsive.isMobile = false
    const wrapper = await mountMobile()
    expect(wrapper.find('.mobile-file-card').exists()).toBe(false)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)
    expect(wrapper.find('.toolbar-buttons').exists()).toBe(true)
    wrapper.unmount()
  })
})
