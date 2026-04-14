import { mount } from '@vue/test-utils'
import { nextTick, ref } from 'vue'
import ElementPlus from 'element-plus'
import { describe, expect, it, vi } from 'vitest'
import FilesView from '../FilesView.vue'

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(false),
}))

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

describe('FilesView 桌面端搜索浮层', () => {
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
