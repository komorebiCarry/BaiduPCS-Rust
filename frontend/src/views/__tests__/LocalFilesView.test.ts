import { flushPromises, mount } from '@vue/test-utils'
import { nextTick, ref } from 'vue'
import ElementPlus, { ElMessageBox } from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import LocalFilesView from '../LocalFilesView.vue'

const responsive = vi.hoisted(() => ({ isMobile: true }))

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(responsive.isMobile),
}))

vi.mock('@/api/localFiles', () => ({
  listLocalFiles: vi.fn(),
  deleteLocalFiles: vi.fn(),
}))

function makeEntry(overrides: Record<string, unknown> = {}) {
  return {
    id: '1',
    name: 'a.txt',
    entryType: 'file' as const,
    size: 10,
    createdAt: '2026-01-01T00:00:00Z',
    updatedAt: '2026-01-01T00:00:00Z',
    path: '/downloads/a.txt',
    ...overrides,
  }
}

const folder = makeEntry({
  id: 'd1',
  name: '剧集',
  entryType: 'directory',
  size: null,
  path: '/downloads/剧集',
})
const movie = makeEntry({ id: 'f1', name: '电影.mkv', path: '/downloads/电影.mkv', size: 2048 })
const doc = makeEntry({ id: 'f2', name: '文档.pdf', path: '/downloads/文档.pdf', size: 1024 })

async function mountView() {
  const wrapper = mount(LocalFilesView, {
    attachTo: document.body,
    global: {
      plugins: [ElementPlus],
      stubs: {
        'el-table': true,
        'el-table-column': true,
      },
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

describe('LocalFilesView 移动端多选', () => {
  beforeEach(async () => {
    responsive.isMobile = true
    const { listLocalFiles } = await import('@/api/localFiles')
    vi.mocked(listLocalFiles).mockReset()
    vi.mocked(listLocalFiles).mockResolvedValue({
      entries: [folder, movie, doc],
      currentPath: '/downloads',
      parentPath: null,
      total: 3,
      page: 0,
      pageSize: 100,
      hasMore: false,
      permissionInfo: null,
    })
  })

  afterEach(() => {
    document.body.innerHTML = ''
  })

  it('勾选框切换选中，底栏随之显示/隐藏', async () => {
    const wrapper = await mountView()
    expect(wrapper.findAll('.mobile-file-card')).toHaveLength(3)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)

    await clickCardCheckbox(wrapper, '电影.mkv')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles.map((f: { id: string }) => f.id)).toEqual(['f1'])
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
    const { listLocalFiles } = await import('@/api/localFiles')
    const wrapper = await mountView()
    vi.mocked(listLocalFiles).mockClear()
    vi.mocked(listLocalFiles).mockResolvedValue({
      entries: [],
      currentPath: '/downloads/剧集',
      parentPath: '/downloads',
      total: 0,
      page: 0,
      pageSize: 100,
      hasMore: false,
      permissionInfo: null,
    })

    await cardByName(wrapper, '剧集').trigger('click')
    await flushPromises()

    expect(listLocalFiles).toHaveBeenCalled()
    expect(vi.mocked(listLocalFiles).mock.calls[0][0]).toBe('/downloads/剧集')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(0)

    wrapper.unmount()
  })

  it('全选当前已加载项，取消会清空并收起底栏', async () => {
    const wrapper = await mountView()
    await wrapper.findAll('.file-card-checkbox input')[0].setValue(true)
    await nextTick()
    await wrapper.find('.mobile-batch-bar__select-all').trigger('click')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(3)
    expect(wrapper.find('.mobile-batch-bar__count').text()).toBe('已选 3 项')

    await wrapper.find('.mobile-batch-bar__clear').trigger('click')
    await nextTick()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(0)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)

    wrapper.unmount()
  })

  it('底栏删除会带着当前选中项发起批量删除', async () => {
    const confirm = vi.spyOn(ElMessageBox, 'confirm').mockRejectedValue('cancel')
    const wrapper = await mountView()
    await clickCardCheckbox(wrapper, '电影.mkv')
    await clickCardCheckbox(wrapper, '文档.pdf')
    await wrapper.find('.mobile-batch-bar__delete').trigger('click')
    await flushPromises()
    expect(confirm).toHaveBeenCalled()
    const message = String(confirm.mock.calls[0][0])
    expect(message).toContain('2 个')
    confirm.mockRestore()
    wrapper.unmount()
  })

  it('切换目录后会清空选中', async () => {
    const { listLocalFiles } = await import('@/api/localFiles')
    const wrapper = await mountView()
    await clickCardCheckbox(wrapper, '电影.mkv')
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(1)

    vi.mocked(listLocalFiles).mockResolvedValueOnce({
      entries: [makeEntry({ id: 'c1', name: 'a.mkv', path: '/downloads/剧集/a.mkv' })],
      currentPath: '/downloads/剧集',
      parentPath: '/downloads',
      total: 1,
      page: 0,
      pageSize: 100,
      hasMore: false,
      permissionInfo: null,
    })
    await cardByName(wrapper, '剧集').trigger('click')
    await flushPromises()
    // @ts-expect-error setup 内部状态
    expect(wrapper.vm.selectedFiles).toHaveLength(0)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)

    wrapper.unmount()
  })

  it('桌面端不渲染卡片勾选和吸底栏', async () => {
    responsive.isMobile = false
    const wrapper = await mountView()
    expect(wrapper.find('.mobile-file-card').exists()).toBe(false)
    expect(wrapper.find('.mobile-batch-bar').exists()).toBe(false)
    expect(wrapper.find('.toolbar-buttons').exists()).toBe(true)
    wrapper.unmount()
  })
})
