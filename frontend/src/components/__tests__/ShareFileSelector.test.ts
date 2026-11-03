import { flushPromises, mount } from '@vue/test-utils'
import { ref } from 'vue'
import ElementPlus from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import ShareFileSelector from '../ShareFileSelector.vue'

const transfer = vi.hoisted(() => ({
  previewShareFiles: vi.fn(),
  previewShareDir: vi.fn(),
}))

vi.mock('@/api/transfer', () => ({
  previewShareFiles: transfer.previewShareFiles,
  previewShareDir: transfer.previewShareDir,
}))

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(false),
}))

const SHARE_INFO = {
  short_key: '1abc',
  shareid: 'sid',
  uk: 'uk-1',
  share_uk: 'share-uk',
  bdstoken: 'tok',
  kind: 'personal' as const,
  token: 'tok',
}

const ROOT_FILES = [
  { fs_id: 1, is_dir: true, path: '/剧集', size: 0, name: '剧集' },
  { fs_id: 2, is_dir: false, path: '/说明.txt', size: 10, name: '说明.txt' },
]

function mountSelector(folderOnly: boolean) {
  return mount(ShareFileSelector, {
    attachTo: document.body,
    props: {
      files: ROOT_FILES,
      loading: false,
      shareInfo: SHARE_INFO,
      shareUrl: 'https://pan.baidu.com/s/1y7CluAbCdEfGh',
      folderOnly,
    },
    global: { plugins: [ElementPlus] },
  })
}

function itemByName(name: string): HTMLElement {
  const items = Array.from(document.body.querySelectorAll<HTMLElement>('.file-item'))
  const item = items.find((el) => el.querySelector('.file-name')?.textContent?.trim() === name)
  expect(item, `应存在条目「${name}」`).toBeTruthy()
  return item!
}

function selectedFiles(wrapper: ReturnType<typeof mountSelector>): unknown[] {
  const emitted = wrapper.emitted('update:selectedFiles')
  const last = emitted?.[emitted.length - 1]
  return (last?.[0] ?? []) as unknown[]
}

beforeEach(() => {
  transfer.previewShareDir.mockReset()
  transfer.previewShareFiles.mockReset()
  transfer.previewShareDir.mockResolvedValue({
    files: [{ fs_id: 3, is_dir: false, path: '/剧集/01.mp4', size: 5, name: '01.mp4' }],
  })
})

afterEach(() => {
  document.body.innerHTML = ''
})

describe('ShareFileSelector 仅选文件夹模式', () => {
  it('只勾选已浏览目录的子文件夹，不应自动排除父目录', async () => {
    transfer.previewShareDir.mockResolvedValueOnce({
      files: [{ fs_id: 3, is_dir: true, path: '/剧集/S01', size: 0, name: 'S01' }],
    })
    const wrapper = mountSelector(true)
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      itemByName('S01').querySelector<HTMLInputElement>('input[type="checkbox"]')!.click()
      await flushPromises()
      expect((selectedFiles(wrapper) as Array<{ path: string }>).map(f => f.path)).toEqual(['/剧集/S01'])
    } finally {
      wrapper.unmount()
    }
  })

  it('滚动加载更多文件夹不应自动选择排除项', async () => {
    const files = Array.from({ length: 100 }, (_, i) => ({
      fs_id: i + 10, is_dir: true, path: `/目录${i}`, size: 0, name: `目录${i}`,
    }))
    transfer.previewShareFiles.mockResolvedValueOnce({
      files: [{ fs_id: 1001, is_dir: true, path: '/下一页', size: 0, name: '下一页' }],
    })
    const wrapper = mountSelector(true)
    try {
      await wrapper.setProps({ files })
      await flushPromises()
      expect(selectedFiles(wrapper)).toHaveLength(0)
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()
      expect(transfer.previewShareFiles).toHaveBeenCalledTimes(1)
      expect(selectedFiles(wrapper)).toHaveLength(0)
    } finally {
      wrapper.unmount()
    }
  })

  it('文件不可勾选，默认不选中任何项', async () => {
    const wrapper = mountSelector(true)
    await flushPromises()

    expect(itemByName('说明.txt').querySelector('input[type="checkbox"]')).toBeNull()
    expect(itemByName('剧集').querySelector('input[type="checkbox"]')).toBeTruthy()
    expect(selectedFiles(wrapper)).toHaveLength(0)
    wrapper.unmount()
  })

  it('全选只选文件夹，不会把文件选进来', async () => {
    const wrapper = mountSelector(true)
    await flushPromises()

    const selectAll = document.body.querySelector<HTMLInputElement>('.selector-header input[type="checkbox"]')
    expect(selectAll, '应有全选框').toBeTruthy()
    selectAll!.click()
    await flushPromises()

    const picked = selectedFiles(wrapper) as Array<{ name: string; is_dir: boolean }>
    expect(picked).toHaveLength(1)
    expect(picked[0].is_dir).toBe(true)
    expect(picked[0].name).toBe('剧集')
    wrapper.unmount()
  })

  it('进入过子目录的文件夹，勾选后仍只产出文件夹本身', async () => {
    const wrapper = mountSelector(true)
    await flushPromises()

    itemByName('剧集').click()
    await flushPromises()
    const rootBtn = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '根目录')
    expect(rootBtn, '应有「根目录」面包屑').toBeTruthy()
    rootBtn!.click()
    await flushPromises()

    // 已展开过的文件夹，勾选后仍只选中自身
    const dirCheckbox = itemByName('剧集').querySelector<HTMLInputElement>('input[type="checkbox"]')
    expect(dirCheckbox).toBeTruthy()
    dirCheckbox!.click()
    await flushPromises()

    const picked = selectedFiles(wrapper) as Array<{ name: string; is_dir: boolean }>
    expect(picked).toHaveLength(1)
    expect(picked[0].is_dir).toBe(true)
    expect(picked[0].name).toBe('剧集')
    wrapper.unmount()
  })
})
