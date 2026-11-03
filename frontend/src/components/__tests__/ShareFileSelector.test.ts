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

// 真实分享的 uk / shareid 都是数字，sharelink 虚拟根才会被识别为命名空间
const PATH_SHARE_INFO = {
  ...SHARE_INFO,
  uk: '1100862997704',
  shareid: '6168417644',
}

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

interface PathSelectorOptions {
  files?: typeof ROOT_FILES
  initialSelectedPaths?: string[]
  emptySelectionMeansAll?: boolean
}

function mountPathSelector(options: PathSelectorOptions = {}) {
  return mount(ShareFileSelector, {
    attachTo: document.body,
    props: {
      files: options.files ?? ROOT_FILES,
      loading: false,
      shareInfo: PATH_SHARE_INFO,
      shareUrl: 'https://pan.baidu.com/s/1y7CluAbCdEfGh',
      pathSelection: true,
      initialSelectedPaths: options.initialSelectedPaths ?? [],
      emptySelectionMeansAll: options.emptySelectionMeansAll ?? false,
    },
    global: { plugins: [ElementPlus] },
  })
}

function emittedPaths(wrapper: ReturnType<typeof mountPathSelector>): string[] {
  const emitted = wrapper.emitted('update:selectedPaths')
  const last = emitted?.[emitted.length - 1]
  return (last?.[0] ?? []) as string[]
}

function emittedReady(wrapper: ReturnType<typeof mountPathSelector>): boolean | undefined {
  const emitted = wrapper.emitted('update:selectionReady')
  const last = emitted?.[emitted.length - 1]
  return last?.[0] as boolean | undefined
}

function checkboxState(name: string): { checked: boolean; indeterminate: boolean } {
  const input = itemByName(name).querySelector<HTMLElement>('.el-checkbox__input')
  expect(input, `条目「${name}」应有复选框`).toBeTruthy()
  const classes = input!.classList
  return {
    checked: classes.contains('is-checked'),
    indeterminate: classes.contains('is-indeterminate'),
  }
}

function headerCheckbox(): HTMLInputElement {
  const input = document.body.querySelector<HTMLInputElement>('.selector-header input[type="checkbox"]')
  expect(input, '应有全选框').toBeTruthy()
  return input!
}

/** 头部全选框的状态 */
function headerState(): { checked: boolean; indeterminate: boolean } {
  const input = document.body.querySelector<HTMLElement>('.selector-header .el-checkbox__input')
  expect(input, '应有全选框').toBeTruthy()
  const classes = input!.classList
  return {
    checked: classes.contains('is-checked'),
    indeterminate: classes.contains('is-indeterminate'),
  }
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

describe('ShareFileSelector 默认模式', () => {
  it('未开启路径选择时保持根目录自动全选与 fs_id 输出', async () => {
    const wrapper = mountSelector(false)
    try {
      await flushPromises()
      const picked = selectedFiles(wrapper) as Array<{ name: string }>
      expect(picked.map(f => f.name)).toEqual(['剧集', '说明.txt'])
      expect(wrapper.emitted('update:selectedPaths')).toBeUndefined()
    } finally {
      wrapper.unmount()
    }
  })
})

describe('ShareFileSelector 路径选择模式', () => {
  it('按初始路径恢复选择，未加载的后代只体现为半选', async () => {
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集/01.mp4'] })
    try {
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual(['/剧集/01.mp4'])
      expect(checkboxState('剧集')).toEqual({ checked: false, indeterminate: true })
      expect(checkboxState('说明.txt')).toEqual({ checked: false, indeterminate: false })
    } finally {
      wrapper.unmount()
    }
  })

  it('显式选中的目录为全选，勾选后按目录整体输出', async () => {
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集'] })
    try {
      await flushPromises()
      expect(checkboxState('剧集')).toEqual({ checked: true, indeterminate: false })
      itemByName('说明.txt').querySelector<HTMLInputElement>('input[type="checkbox"]')!.click()
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual(['/剧集', '/说明.txt'])
    } finally {
      wrapper.unmount()
    }
  })

  it('sharelink 命名空间别名与实际路径视为同一路径', async () => {
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/sharelink0-1/剧集'] })
    try {
      await flushPromises()
      expect(checkboxState('剧集')).toEqual({ checked: true, indeterminate: false })
      // 未改动时原样回显初始写法
      expect(emittedPaths(wrapper)).toEqual(['/sharelink0-1/剧集'])
    } finally {
      wrapper.unmount()
    }
  })

  it('空初始路径且允许整体分享时全部条目为全选', async () => {
    const wrapper = mountPathSelector({ emptySelectionMeansAll: true })
    try {
      await flushPromises()
      expect(checkboxState('剧集')).toEqual({ checked: true, indeterminate: false })
      expect(checkboxState('说明.txt')).toEqual({ checked: true, indeterminate: false })
      expect(emittedPaths(wrapper)).toEqual([])
      expect(emittedReady(wrapper)).toBe(true)
    } finally {
      wrapper.unmount()
    }
  })

  it('整体分享下取消一项会复用完整根目录生成剩余兄弟的显式路径', async () => {
    transfer.previewShareFiles.mockResolvedValue({ files: ROOT_FILES })
    const wrapper = mountPathSelector({ emptySelectionMeansAll: true })
    try {
      await flushPromises()
      itemByName('说明.txt').querySelector<HTMLInputElement>('input[type="checkbox"]')!.click()
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
      expect(emittedReady(wrapper)).toBe(true)
      // 根目录短页即完整列表，无需再请求
      expect(transfer.previewShareFiles).not.toHaveBeenCalled()
    } finally {
      wrapper.unmount()
    }
  })

  it('整体分享下展开失败时保留原选择并给出错误', async () => {
    const files = Array.from({ length: 100 }, (_, i) => ({
      fs_id: i + 10, is_dir: false, path: `/文件${i}.txt`, size: 1, name: `文件${i}.txt`,
    }))
    files[0] = { fs_id: 999, is_dir: false, path: '/说明.txt', size: 1, name: '说明.txt' }
    transfer.previewShareFiles.mockRejectedValue(new Error('网络中断'))
    const wrapper = mountPathSelector({ files, emptySelectionMeansAll: true })
    try {
      await flushPromises()
      itemByName('说明.txt').querySelector<HTMLInputElement>('input[type="checkbox"]')!.click()
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual([])
      const error = document.body.querySelector('.selection-error')
      expect(error?.textContent?.trim()).toBeTruthy()
      expect(checkboxState('说明.txt'), '整体分享回滚后条目仍为全选')
          .toEqual({ checked: true, indeterminate: false })
    } finally {
      wrapper.unmount()
    }
  })

  it('同步内容清空后不可确认，排除内容空选择仍然可用', async () => {
    const include = mountPathSelector({ initialSelectedPaths: ['/剧集'], emptySelectionMeansAll: true })
    try {
      await flushPromises()
      expect(emittedReady(include)).toBe(true)
      itemByName('剧集').querySelector<HTMLInputElement>('input[type="checkbox"]')!.click()
      await flushPromises()
      expect(emittedPaths(include)).toEqual([])
      expect(emittedReady(include)).toBe(false)
    } finally {
      include.unmount()
    }

    const exclude = mountPathSelector({})
    try {
      await flushPromises()
      expect(emittedPaths(exclude)).toEqual([])
      expect(emittedReady(exclude)).toBe(true)
    } finally {
      exclude.unmount()
    }
  })

  it('根目录全选覆盖整个目录列表（含后续分页）', async () => {
    const page1 = Array.from({ length: 100 }, (_, i) => ({
      fs_id: i + 1, is_dir: false, path: `/文件${i}.txt`, size: 1, name: `文件${i}.txt`,
    }))
    const page2 = [{ fs_id: 101, is_dir: false, path: '/最后一页.txt', size: 1, name: '最后一页.txt' }]
    transfer.previewShareFiles.mockImplementation(async (req: { page?: number }) =>
      (req.page ?? 1) === 1 ? { files: page1 } : { files: page2 },
    )
    const wrapper = mountPathSelector({ files: page1 })
    try {
      await flushPromises()
      headerCheckbox().click()
      await flushPromises()
      const paths = emittedPaths(wrapper)
      expect(paths).toHaveLength(101)
      expect(paths).toContain('/最后一页.txt')
      expect(transfer.previewShareFiles).toHaveBeenCalledTimes(2)
    } finally {
      wrapper.unmount()
    }
  })

  it('第一页全选但后续分页未知时表头为半选，点击后解析全部分页并全选', async () => {
    const page1 = Array.from({ length: 100 }, (_, i) => ({
      fs_id: i + 1, is_dir: false, path: `/文件${i}.txt`, size: 1, name: `文件${i}.txt`,
    }))
    const page2 = [{ fs_id: 101, is_dir: false, path: '/第101个.txt', size: 1, name: '第101个.txt' }]
    transfer.previewShareFiles.mockImplementation(async (req: { page?: number }) =>
      (req.page ?? 1) === 1 ? { files: page1 } : { files: page2 },
    )
    const wrapper = mountPathSelector({ files: page1, initialSelectedPaths: page1.map(f => f.path) })
    try {
      await flushPromises()
      // 后续分页未知：只能算半选，不能显示成整个目录已选
      expect(headerState()).toEqual({ checked: false, indeterminate: true })
      headerCheckbox().click()
      await flushPromises()
      const paths = emittedPaths(wrapper)
      // 点击必须解析全部分页后全选，绝不能清空既有选择
      expect(paths).toHaveLength(101)
      expect(paths).toContain('/第101个.txt')
      expect(paths).toContain('/文件0.txt')
    } finally {
      wrapper.unmount()
    }
  })

  it('根目录短页全部选中时表头为全选，首次点击即清空', async () => {
    const files = [
      { fs_id: 1, is_dir: false, path: '/one', size: 1, name: 'one' },
      { fs_id: 2, is_dir: false, path: '/two', size: 1, name: 'two' },
    ]
    const wrapper = mountPathSelector({ files, initialSelectedPaths: ['/one', '/two'] })
    try {
      await flushPromises()
      // 短页本身就代表完整列表：全部选中必须显示为 checked，而不是 mixed
      expect(headerState()).toEqual({ checked: true, indeterminate: false })
      headerCheckbox().click()
      await flushPromises()
      // 常规 checked → 清空，且无需再请求目录
      expect(emittedPaths(wrapper)).toEqual([])
      expect(transfer.previewShareFiles).not.toHaveBeenCalled()
      expect(headerState()).toEqual({ checked: false, indeterminate: false })
    } finally {
      wrapper.unmount()
    }
  })

  it('解析后续分页失败时保留原选择，不会清空', async () => {
    const page1 = Array.from({ length: 100 }, (_, i) => ({
      fs_id: i + 1, is_dir: false, path: `/文件${i}.txt`, size: 1, name: `文件${i}.txt`,
    }))
    transfer.previewShareFiles.mockImplementation(async (req: { page?: number }) => {
      if ((req.page ?? 1) === 1) return { files: page1 }
      throw new Error('网络中断')
    })
    const wrapper = mountPathSelector({ files: page1, initialSelectedPaths: page1.map(f => f.path) })
    try {
      await flushPromises()
      const headerBefore = headerCheckbox()
      headerBefore.focus()
      headerBefore.click()
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual(page1.map(f => f.path))
      const error = document.body.querySelector('.selection-error')
      expect(error?.textContent?.trim(), '分页失败应给出错误').toBeTruthy()
      // 事务失败后表头视觉必须回到旧模型（半选），不能停在用户点过的 checked
      expect(headerState(), '失败回滚后表头视觉与模型一致').toEqual({ checked: false, indeterminate: true })
      // 表头重挂载后焦点必须回到同一复选框
      const headerAfter = headerCheckbox()
      expect(headerAfter).not.toBe(headerBefore)
      expect(document.activeElement, '表头失败回滚后保持键盘焦点').toBe(headerAfter)
    } finally {
      wrapper.unmount()
    }
  })

  it('进入子目录不会改变已选路径', async () => {
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集'] })
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
      expect(checkboxState('01.mp4')).toEqual({ checked: true, indeterminate: false })
    } finally {
      wrapper.unmount()
    }
  })

  it('取消被选中目录覆盖的子项时展开该目录的全部分页', async () => {
    const page1 = Array.from({ length: 100 }, (_, i) => ({
      fs_id: 1000 + i,
      is_dir: false,
      path: `/剧集/${String(i).padStart(3, '0')}.mp4`,
      size: 1,
      name: `${String(i).padStart(3, '0')}.mp4`,
    }))
    const page2 = [{ fs_id: 2000, is_dir: false, path: '/剧集/999.mp4', size: 1, name: '999.mp4' }]
    transfer.previewShareDir.mockImplementation(async (req: { page?: number }) =>
      (req.page ?? 1) === 1 ? { files: page1 } : { files: page2 },
    )
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集'] })
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      itemByName('000.mp4').querySelector<HTMLInputElement>('input[type="checkbox"]')!.click()
      await flushPromises()
      const paths = emittedPaths(wrapper)
      expect(paths).toHaveLength(100)
      expect(paths).not.toContain('/剧集/000.mp4')
      expect(paths).toContain('/剧集/999.mp4')
    } finally {
      wrapper.unmount()
    }
  })

  it('展开中途失败时保留原选择并给出错误', async () => {
    const page1 = Array.from({ length: 100 }, (_, i) => ({
      fs_id: 1000 + i,
      is_dir: false,
      path: `/剧集/${String(i).padStart(3, '0')}.mp4`,
      size: 1,
      name: `${String(i).padStart(3, '0')}.mp4`,
    }))
    transfer.previewShareDir.mockImplementation(async (req: { page?: number }) => {
      if ((req.page ?? 1) === 1) return { files: page1 }
      throw new Error('网络中断')
    })
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集'] })
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      const focusedInput = itemByName('000.mp4').querySelector<HTMLInputElement>('input[type="checkbox"]')!
      focusedInput.focus()
      expect(document.activeElement).toBe(focusedInput)
      focusedInput.click()
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
      const error = document.body.querySelector('.selection-error')
      expect(error?.textContent?.trim()).toBeTruthy()
      // 回滚后行复选框必须回到模型值（仍由显式父目录覆盖），不能停在用户点过的未选
      expect(checkboxState('000.mp4'), '失败回滚后行勾选视觉与模型一致')
          .toEqual({ checked: true, indeterminate: false })
      // 复选框局部重挂载后，焦点必须回到同一逻辑条目
      const restoredInput = itemByName('000.mp4').querySelector<HTMLInputElement>('input[type="checkbox"]')!
      expect(restoredInput).not.toBe(focusedInput)
      expect(document.activeElement, '失败回滚后保持键盘焦点').toBe(restoredInput)
      expect(restoredInput.checked).toBe(true)
      expect(itemByName('000.mp4').querySelector('[aria-label="选择 000.mp4"]'), 'aria 标签保留').toBeTruthy()
    } finally {
      wrapper.unmount()
    }
  })

  it('翻页加载不会扩大已选范围', async () => {
    const files = Array.from({ length: 100 }, (_, i) => ({
      fs_id: i + 1, is_dir: false, path: `/文件${i}.txt`, size: 1, name: `文件${i}.txt`,
    }))
    transfer.previewShareFiles.mockResolvedValue({
      files: [{ fs_id: 1001, is_dir: false, path: '/下一页.txt', size: 1, name: '下一页.txt' }],
    })
    const wrapper = mountPathSelector({ files, initialSelectedPaths: ['/文件0.txt'] })
    try {
      await flushPromises()
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()
      expect(transfer.previewShareFiles).toHaveBeenCalledTimes(1)
      expect(emittedPaths(wrapper)).toEqual(['/文件0.txt'])
    } finally {
      wrapper.unmount()
    }
  })
})

describe('ShareFileSelector 选择事务与导航竞态', () => {
  // 100 条 = 首页满页，导航记录的内容不完整，取消子项时必须真正再拉一次目录
  const DIR_FILES = Array.from({ length: 100 }, (_, i) => ({
    fs_id: 10 + i,
    is_dir: false,
    path: `/剧集/${String(i).padStart(3, '0')}.mp4`,
    size: 1,
    name: `${String(i).padStart(3, '0')}.mp4`,
  }))

  interface Deferred<T> {
    promise: Promise<T>
    resolve: (value: T) => void
    reject: (reason?: unknown) => void
  }

  function deferred<T>(): Deferred<T> {
    let resolve!: (value: T) => void
    let reject!: (reason?: unknown) => void
    const promise = new Promise<T>((res, rej) => {
      resolve = res
      reject = rej
    })
    return { promise, resolve, reject }
  }

  /** 打开「剧集」并让随后一次目录展开挂起 */
  async function enterDirWithPendingExpand(
    pending: Deferred<{ files: typeof DIR_FILES }>,
  ) {
    let dirCalls = 0
    transfer.previewShareDir.mockImplementation(async () => {
      dirCalls++
      if (dirCalls === 1) return { files: DIR_FILES }
      return pending.promise
    })
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集'] })
    await flushPromises()
    itemByName('剧集').click()
    await flushPromises()
    itemByName('000.mp4').querySelector<HTMLInputElement>('input[type="checkbox"]')!.click()
    await flushPromises()
    return { wrapper, dirCalls: () => dirCalls }
  }

  function rootBreadcrumb(): HTMLElement {
    const btn = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '根目录')
    expect(btn, '应有「根目录」面包屑').toBeTruthy()
    return btn!
  }

  function busyText(): string {
    return document.body.querySelector<HTMLElement>('.selection-status')?.textContent?.trim() ?? ''
  }

  it('导航取消进行中的展开后控件恢复，迟到的成功不覆盖旧选择', async () => {
    const pending = deferred<{ files: typeof DIR_FILES }>()
    const { wrapper } = await enterDirWithPendingExpand(pending)
    try {
      // 展开未完成：busy、全选框禁用、确定所依赖的就绪为 false
      expect(busyText()).toContain('正在展开目录')
      expect(headerCheckbox().disabled, '事务进行中不可操作').toBe(true)
      expect(emittedReady(wrapper)).toBe(false)

      // 事务未结束时导航：原子取消旧事务，旧选择保持，busy 所有权释放
      rootBreadcrumb().click()
      await flushPromises()
      expect(busyText(), '导航后不得停留在展开中').not.toContain('正在展开目录')
      expect(headerCheckbox().disabled, '导航取消后控件必须恢复').toBe(false)
      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
      expect(emittedReady(wrapper)).toBe(true)

      // 迟到的成功既不改选择，也不改 busy / 就绪
      pending.resolve({ files: DIR_FILES })
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
      expect(busyText()).not.toContain('正在展开目录')
      expect(headerCheckbox().disabled).toBe(false)
      expect(emittedReady(wrapper)).toBe(true)
    } finally {
      wrapper.unmount()
    }
  })

  it('导航取消进行中的展开后迟到的失败不会留下错误或卡住控件', async () => {
    const pending = deferred<{ files: typeof DIR_FILES }>()
    const { wrapper } = await enterDirWithPendingExpand(pending)
    try {
      rootBreadcrumb().click()
      await flushPromises()

      pending.reject(new Error('网络中断'))
      await flushPromises()

      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
      expect(document.body.querySelector('.selection-error'), '旧事务失败不得污染新上下文').toBeNull()
      expect(busyText()).not.toContain('正在展开目录')
      expect(headerCheckbox().disabled).toBe(false)
      expect(emittedReady(wrapper)).toBe(true)
    } finally {
      wrapper.unmount()
    }
  })

  it('事务失败前焦点已移出选择器时不得抢回焦点', async () => {
    const pending = deferred<{ files: typeof DIR_FILES }>()
    let dirCalls = 0
    transfer.previewShareDir.mockImplementation(async () => {
      dirCalls++
      if (dirCalls === 1) return { files: DIR_FILES }
      return pending.promise
    })
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集'] })
    const outside = document.createElement('button')
    document.body.appendChild(outside)
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      const input = itemByName('000.mp4').querySelector<HTMLInputElement>('input[type="checkbox"]')!
      input.focus()
      input.click()
      await flushPromises()
      // 事务已从该复选框开始；用户随后把焦点移到选择器外
      outside.focus()
      expect(document.activeElement).toBe(outside)

      pending.reject(new Error('网络中断'))
      await flushPromises()

      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
      expect(document.activeElement, '用户焦点已离开时不得抢回').toBe(outside)
    } finally {
      outside.remove()
      wrapper.unmount()
    }
  })

  it('混选全选事务中导航：旧选择保留、控件恢复，迟到结果不落盘', async () => {
    const page1 = [
      { fs_id: 1000, is_dir: true, path: '/剧集', size: 0, name: '剧集' },
      ...Array.from({ length: 99 }, (_, i) => ({
        fs_id: i + 1, is_dir: false, path: `/文件${i}.txt`, size: 1, name: `文件${i}.txt`,
      })),
    ]
    const pending = deferred<{ files: Array<{ fs_id: number; is_dir: boolean; path: string; size: number; name: string }> }>()
    transfer.previewShareFiles.mockImplementation(() => pending.promise)
    transfer.previewShareDir.mockResolvedValue({ files: DIR_FILES })
    const wrapper = mountPathSelector({
      files: page1,
      initialSelectedPaths: page1.map(f => f.path),
    })
    const oldPaths = page1.map(f => f.path)
    try {
      await flushPromises()
      // 首页全选但后续分页未知 → 半选，点击开始解析全部分页
      expect(headerState()).toEqual({ checked: false, indeterminate: true })
      headerCheckbox().click()
      await flushPromises()
      expect(busyText()).toContain('正在展开目录')
      expect(headerCheckbox().disabled, '全选事务进行中').toBe(true)
      expect(emittedReady(wrapper)).toBe(false)

      // reviewer 场景：事务未结束时点目录行导航
      itemByName('剧集').click()
      await flushPromises()
      expect(busyText(), '导航后不得停在展开中').not.toContain('正在展开目录')
      expect(headerCheckbox().disabled, '导航取消后控件恢复').toBe(false)
      expect(emittedPaths(wrapper)).toEqual(oldPaths)
      expect(emittedReady(wrapper)).toBe(true)

      // 迟到的全选结果既不落盘也不改就绪
      pending.resolve({ files: [...page1, { fs_id: 900, is_dir: false, path: '/第101个.txt', size: 1, name: '第101个.txt' }] })
      await flushPromises()
      expect(emittedPaths(wrapper)).toEqual(oldPaths)
      expect(emittedReady(wrapper)).toBe(true)

      rootBreadcrumb().click()
      await flushPromises()
      expect(headerState(), '旧选择仍是首页全选、后续未知').toEqual({ checked: false, indeterminate: true })
    } finally {
      wrapper.unmount()
    }
  })

  it('旧事务晚于新事务完成时，不得清掉新事务的 busy 与就绪状态', async () => {
    const pendingOld = deferred<{ files: typeof DIR_FILES }>()
    const bigRoot = [
      { fs_id: 1, is_dir: true, path: '/剧集', size: 0, name: '剧集' },
      { fs_id: 2, is_dir: false, path: '/说明.txt', size: 10, name: '说明.txt' },
      ...Array.from({ length: 98 }, (_, i) => ({
        fs_id: 100 + i, is_dir: false, path: `/文件${i}.txt`, size: 1, name: `文件${i}.txt`,
      })),
    ]
    const pendingNew = deferred<{ files: typeof bigRoot }>()
    const { wrapper } = await enterDirWithPendingExpand(pendingOld)
    try {
      // 导航取消旧事务，随后换成一个需要分页的根目录并开始新的全选事务
      rootBreadcrumb().click()
      await flushPromises()
      await wrapper.setProps({ files: bigRoot })
      await flushPromises()
      transfer.previewShareFiles.mockImplementation((req: { page?: number }) =>
        (req.page ?? 1) === 1 ? pendingNew.promise : Promise.resolve({ files: [] }),
      )
      headerCheckbox().click()
      await flushPromises()

      expect(busyText()).toContain('正在展开目录')
      expect(headerCheckbox().disabled, '新事务进行中').toBe(true)
      expect(emittedReady(wrapper)).toBe(false)

      // 旧事务成功返回：不得清掉新事务的 busy，也不得写入旧结果
      pendingOld.resolve({ files: DIR_FILES })
      await flushPromises()
      expect(busyText(), '旧事务不得结束新事务的忙碌态').toContain('正在展开目录')
      expect(headerCheckbox().disabled).toBe(true)
      expect(emittedReady(wrapper)).toBe(false)
      expect(emittedPaths(wrapper)).toEqual(['/剧集'])

      // 新事务完成后才恢复，并写入新结果
      pendingNew.resolve({ files: bigRoot })
      await flushPromises()
      expect(busyText()).not.toContain('正在展开目录')
      expect(headerCheckbox().disabled).toBe(false)
      expect(emittedReady(wrapper)).toBe(true)
      expect(emittedPaths(wrapper)).toEqual(
        expect.arrayContaining(['/剧集', '/说明.txt']),
      )
    } finally {
      wrapper.unmount()
    }
  })
})

describe('ShareFileSelector 目录完整性与覆盖率矩阵', () => {
  function filesIn(prefix: string, count: number, base: number) {
    return Array.from({ length: count }, (_, i) => ({
      fs_id: base + i,
      is_dir: false,
      path: `${prefix}/${String(i).padStart(3, '0')}.bin`,
      size: 1,
      name: `${String(i).padStart(3, '0')}.bin`,
    }))
  }

  function pageSlice<T>(all: T[], page: number): T[] {
    const start = (page - 1) * 100
    return all.slice(start, start + 100)
  }

  async function scrollToEnd(wrapper: ReturnType<typeof mountPathSelector>, times: number) {
    for (let i = 0; i < times; i++) {
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()
    }
  }

  it.each([1, 99, 100, 101, 200])(
    '根目录 N=%i：全选经分页结束后表头为全选并可一次清空',
    async (count) => {
      const all = filesIn('/根', count, 3000)
      transfer.previewShareFiles.mockImplementation(async (req: { page?: number }) => ({
        files: pageSlice(all, req.page ?? 1),
      }))
      const wrapper = mountPathSelector({
        files: all.slice(0, 100),
        initialSelectedPaths: all.map(f => f.path),
        emptySelectionMeansAll: true,
      })
      try {
        await flushPromises()
        if (count >= 100) {
          expect(headerState(), '整页首页且后续未知时必须半选').toEqual({ checked: false, indeterminate: true })
          await scrollToEnd(wrapper, Math.floor(count / 100))
        }
        expect(headerState(), '分页结束后的完整列表应派生全选').toEqual({ checked: true, indeterminate: false })
        // 未点击前不得因翻页扩大或改写已选路径
        expect(emittedPaths(wrapper)).toEqual(all.map(f => f.path))
        expect(emittedReady(wrapper)).toBe(true)

        headerCheckbox().click()
        await flushPromises()
        expect(emittedPaths(wrapper), 'checked 一次点击清空').toEqual([])
        expect(emittedReady(wrapper), '清空同步内容后不可确认').toBe(false)
        expect(headerState()).toEqual({ checked: false, indeterminate: false })
      } finally {
        wrapper.unmount()
      }
    },
  )

  it.each([100, 101, 200])(
    '根目录 N=%i：仅首页选中为半选，点击解析全部分页后全选',
    async (count) => {
      const all = filesIn('/根', count, 4000)
      transfer.previewShareFiles.mockImplementation(async (req: { page?: number }) => ({
        files: pageSlice(all, req.page ?? 1),
      }))
      const firstPage = all.slice(0, 100).map(f => f.path)
      const wrapper = mountPathSelector({
        files: all.slice(0, 100),
        initialSelectedPaths: firstPage,
        emptySelectionMeansAll: true,
      })
      try {
        await flushPromises()
        expect(headerState()).toEqual({ checked: false, indeterminate: true })
        expect(emittedPaths(wrapper), '未知分页不得扩大已选范围').toEqual(firstPage)

        headerCheckbox().click()
        await flushPromises()
        expect(emittedPaths(wrapper)).toEqual(all.map(f => f.path))
        expect(headerState()).toEqual({ checked: true, indeterminate: false })
        expect(emittedReady(wrapper)).toBe(true)
      } finally {
        wrapper.unmount()
      }
    },
  )

  it('根目录 N=0 不渲染全选框', async () => {
    const wrapper = mountPathSelector({ files: [], emptySelectionMeansAll: true })
    try {
      await flushPromises()
      expect(document.body.querySelector('.selector-header')).toBeNull()
      expect(emittedPaths(wrapper)).toEqual([])
      expect(emittedReady(wrapper)).toBe(true)
    } finally {
      wrapper.unmount()
    }
  })

  it.each([1, 99, 100, 101, 200])(
    '子目录 N=%i：全选经分页结束后表头为全选并可一次清空',
    async (count) => {
      const all = filesIn('/剧集', count, 6000)
      transfer.previewShareDir.mockImplementation(async (req: { page?: number }) => ({
        files: pageSlice(all, req.page ?? 1),
      }))
      const wrapper = mountPathSelector({
        initialSelectedPaths: all.map(f => f.path),
        emptySelectionMeansAll: true,
      })
      try {
        await flushPromises()
        itemByName('剧集').click()
        await flushPromises()
        if (count >= 100) {
          expect(headerState(), '整页首页且后续未知时必须半选').toEqual({ checked: false, indeterminate: true })
          await scrollToEnd(wrapper, Math.floor(count / 100))
          expect(transfer.previewShareDir.mock.calls.length, 'scrolled pages').toBe(1 + Math.floor(count / 100))
        }
        expect(headerState()).toEqual({ checked: true, indeterminate: false })
        expect(emittedPaths(wrapper)).toEqual(all.map(f => f.path))
        expect(emittedReady(wrapper)).toBe(true)

        headerCheckbox().click()
        await flushPromises()
        expect(emittedPaths(wrapper), 'checked 一次点击清空当前子树').toEqual([])
        expect(emittedReady(wrapper), '清空同步内容后不可确认').toBe(false)
        expect(headerState()).toEqual({ checked: false, indeterminate: false })
      } finally {
        wrapper.unmount()
      }
    },
  )

  it.each([100, 101, 200])(
    '子目录 N=%i：仅首页选中为半选，点击解析全部分页后全选',
    async (count) => {
      const all = filesIn('/剧集', count, 7000)
      transfer.previewShareDir.mockImplementation(async (req: { page?: number }) => ({
        files: pageSlice(all, req.page ?? 1),
      }))
      const firstPage = all.slice(0, 100).map(f => f.path)
      const wrapper = mountPathSelector({ initialSelectedPaths: firstPage })
      try {
        await flushPromises()
        itemByName('剧集').click()
        await flushPromises()
        expect(headerState()).toEqual({ checked: false, indeterminate: true })
        expect(emittedPaths(wrapper), '未知分页不得扩大已选范围').toEqual(firstPage)

        headerCheckbox().click()
        await flushPromises()
        expect(emittedPaths(wrapper)).toEqual(all.map(f => f.path))
        expect(headerState()).toEqual({ checked: true, indeterminate: false })
      } finally {
        wrapper.unmount()
      }
    },
  )

  it('子目录 N=0 无全选框且不改变已选路径', async () => {
    transfer.previewShareDir.mockResolvedValue({ files: [] })
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集'] })
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      expect(document.body.querySelector('.selector-header')).toBeNull()
      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
    } finally {
      wrapper.unmount()
    }
  })

  it('显式祖先覆盖当前子目录时表头为全选，点击取消整个子树并保留无关兄弟', async () => {
    const namespace = '/sharelink1100862997704-6168417644'
    const s01 = { fs_id: 8101, is_dir: true, path: `${namespace}/剧集/S01`, size: 0, name: 'S01' }
    const s02 = { fs_id: 8102, is_dir: true, path: '/剧集/S02', size: 0, name: 'S02' }
    const chapter = { fs_id: 8103, is_dir: false, path: '/剧集/说明.txt', size: 1, name: '说明.txt' }
    const s01Children = filesIn(`${namespace}/剧集/S01`, 100, 9000)
    const parentChildren = [s01, s02, chapter, ...filesIn('/剧集', 97, 8200)]
    transfer.previewShareDir.mockImplementation(async (req: { dir: string; page?: number }) => {
      if (req.dir.endsWith('/S01')) return { files: pageSlice(s01Children, req.page ?? 1) }
      // 父目录整页首页：展开时必须继续取空尾页才能确认完整兄弟列表
      return { files: pageSlice(parentChildren, req.page ?? 1) }
    })
    const wrapper = mountPathSelector({ initialSelectedPaths: ['/剧集', '/说明.txt'] })
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      itemByName('S01').click()
      await flushPromises()

      // 显式祖先 /剧集 保证整个 S01 子树（含未加载页）已选
      expect(headerState(), '祖先覆盖时表头必须为全选').toEqual({ checked: true, indeterminate: false })
      expect(checkboxState('000.bin')).toEqual({ checked: true, indeterminate: false })

      headerCheckbox().click()
      await flushPromises()

      const paths = emittedPaths(wrapper)
      expect(paths).toEqual(expect.arrayContaining(['/说明.txt', '/剧集/S02', '/剧集/说明.txt']))
      expect(paths.some(p => p === '/剧集/S01' || p.includes('/剧集/S01/')), '当前子树必须整体移除').toBe(false)
      expect(headerState(), '取消后当前目录不再被覆盖').toEqual({ checked: false, indeterminate: false })
    } finally {
      wrapper.unmount()
    }
  })
})

describe('ShareFileSelector 分页请求所有权', () => {
  interface Deferred<T> {
    promise: Promise<T>
    resolve: (value: T) => void
    reject: (reason?: unknown) => void
  }

  function deferred<T>(): Deferred<T> {
    let resolve!: (value: T) => void
    let reject!: (reason?: unknown) => void
    const promise = new Promise<T>((res, rej) => {
      resolve = res
      reject = rej
    })
    return { promise, resolve, reject }
  }

  function filesIn(prefix: string, count: number, base: number) {
    return Array.from({ length: count }, (_, i) => ({
      fs_id: base + i,
      is_dir: false,
      path: `${prefix}/${String(i).padStart(3, '0')}.mp4`,
      size: 1,
      name: `${String(i).padStart(3, '0')}.mp4`,
    }))
  }

  function pageSlice<T>(all: T[], page: number): T[] {
    const start = (page - 1) * 100
    return all.slice(start, start + 100)
  }

  function renderedNames(): string[] {
    return Array.from(document.body.querySelectorAll<HTMLElement>('.file-item .file-name'))
        .map(el => el.textContent?.trim() ?? '')
  }

  function rootBreadcrumb(): HTMLElement {
    const btn = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '根目录')
    expect(btn, '应有「根目录」面包屑').toBeTruthy()
    return btn!
  }

  function loadingMoreVisible(): boolean {
    return (document.body.textContent ?? '').includes('加载更多...')
  }

  it('子目录 page2 迟到成功后跨导航返回根目录：不污染根列表/缓存', async () => {
    const page1 = filesIn('/剧集', 100, 10000)
    const pending = deferred<{ files: typeof page1 }>()
    let dirCalls = 0
    transfer.previewShareDir.mockImplementation(async (req: { page?: number }) => {
      dirCalls++
      if ((req.page ?? 1) === 1) return { files: page1 }
      return pending.promise
    })
    const wrapper = mountPathSelector({ initialSelectedPaths: page1.map(f => f.path) })
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      expect(headerState()).toEqual({ checked: false, indeterminate: true })
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()
      expect(dirCalls, '应请求到子目录 page2').toBe(2)

      rootBreadcrumb().click()
      await flushPromises()
      pending.resolve({
        files: [
          { fs_id: 12000, is_dir: false, path: '/剧集/第100集.mp4', size: 1, name: '第100集.mp4' },
          { fs_id: 12001, is_dir: false, path: '/剧集/第101集.mp4', size: 1, name: '第101集.mp4' },
        ],
      })
      await flushPromises()

      expect(renderedNames()).toEqual(['剧集', '说明.txt'])
      expect(emittedPaths(wrapper)).toEqual(page1.map(f => f.path))

      // 陈旧页不得把子目录缓存标记为完整：重进后仍应是半选
      itemByName('剧集').click()
      await flushPromises()
      expect(headerState()).toEqual({ checked: false, indeterminate: true })
    } finally {
      wrapper.unmount()
    }
  })

  it('子目录 page2 迟到失败跨目录导航：不产生错误也不污染目录B', async () => {
    const dirPage1 = filesIn('/剧集', 100, 13000)
    const pending = deferred<{ files: typeof dirPage1 }>()
    transfer.previewShareDir.mockImplementation(async (req: { dir: string; page?: number }) => {
      if (req.dir.endsWith('/广告')) {
        return {
          files: [{ fs_id: 14000, is_dir: false, path: '/广告/片头.mp4', size: 1, name: '片头.mp4' }],
        }
      }
      if ((req.page ?? 1) === 1) return { files: dirPage1 }
      return pending.promise
    })
    const rootFiles = [
      ...ROOT_FILES,
      { fs_id: 14001, is_dir: true, path: '/广告', size: 0, name: '广告' },
    ]
    const wrapper = mountPathSelector({ files: rootFiles, initialSelectedPaths: ['/剧集'] })
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()

      rootBreadcrumb().click()
      await flushPromises()
      itemByName('广告').click()
      await flushPromises()
      pending.reject(new Error('网络中断'))
      await flushPromises()

      expect(renderedNames()).toEqual(['片头.mp4'])
      expect(document.body.querySelector('.selection-error')).toBeNull()
      expect(emittedPaths(wrapper)).toEqual(['/剧集'])
    } finally {
      wrapper.unmount()
    }
  })

  it('根目录 page2 在数据源重载后迟到：丢弃旧页且重置根分页', async () => {
    const page1 = filesIn('/旧', 100, 15000)
    const pending = deferred<{ files: typeof page1 }>()
    transfer.previewShareFiles.mockImplementation((req: { page?: number }) =>
      (req.page ?? 1) === 1 ? Promise.resolve({ files: page1 }) : pending.promise,
    )
    const wrapper = mountPathSelector({ files: page1 })
    const replacement = [
      { fs_id: 16000, is_dir: false, path: '/新.txt', size: 1, name: '新.txt' },
    ]
    try {
      await flushPromises()
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()

      await wrapper.setProps({ files: replacement })
      await flushPromises()
      pending.resolve({
        files: [{ fs_id: 16001, is_dir: false, path: '/旧第2页.txt', size: 1, name: '旧第2页.txt' }],
      })
      await flushPromises()

      expect(renderedNames()).toEqual(['新.txt'])
      expect(emittedPaths(wrapper)).toEqual([])
      // 新数据源短页即完整，且旧页不得让 hasMore 复活
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()
      expect(transfer.previewShareFiles.mock.calls.filter(c => (c[0].page ?? 1) > 1)).toHaveLength(1)
    } finally {
      wrapper.unmount()
    }
  })

  it('卸载后 page2 迟到不产生任何新输出', async () => {
    const page1 = filesIn('/根', 100, 17000)
    const pending = deferred<{ files: typeof page1 }>()
    transfer.previewShareFiles.mockImplementation((req: { page?: number }) =>
      (req.page ?? 1) === 1 ? Promise.resolve({ files: page1 }) : pending.promise,
    )
    const wrapper = mountPathSelector({ files: page1 })
    await flushPromises()
    await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
    await flushPromises()
    const emitSpy = vi.spyOn(wrapper.vm.$, 'emit')

    wrapper.unmount()
    pending.resolve({
      files: [{ fs_id: 17101, is_dir: false, path: '/迟到.txt', size: 1, name: '迟到.txt' }],
    })
    await flushPromises()

    expect(emitSpy, '卸载后不得再 emit').not.toHaveBeenCalled()
  })

  it('旧 page2 的 finally 不得清掉新目录的新分页请求', async () => {
    const rootPage1 = [
      ...ROOT_FILES,
      ...filesIn('/根', 98, 18000),
    ]
    const oldPending = deferred<{ files: typeof rootPage1 }>()
    const newPending = deferred<{ files: Array<{ fs_id: number; is_dir: boolean; path: string; size: number; name: string }> }>()
    const newPage1 = filesIn('/剧集', 100, 19000)
    transfer.previewShareFiles.mockImplementation((req: { page?: number }) =>
      (req.page ?? 1) === 1 ? Promise.resolve({ files: rootPage1 }) : oldPending.promise,
    )
    transfer.previewShareDir.mockImplementation(async (req: { page?: number }) => {
      if ((req.page ?? 1) === 1) return { files: newPage1 }
      return newPending.promise
    })
    const wrapper = mountPathSelector({ files: rootPage1 })
    try {
      await flushPromises()
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()

      itemByName('剧集').click()
      await flushPromises()
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()
      expect(loadingMoreVisible(), '新目录分页进行中').toBe(true)

      oldPending.resolve({
        files: [{ fs_id: 20001, is_dir: false, path: '/根/旧第2页.mp4', size: 1, name: '旧第2页.mp4' }],
      })
      await flushPromises()
      expect(loadingMoreVisible(), '旧结果不得释放新请求的加载态').toBe(true)
      expect(renderedNames().includes('旧第2页.mp4')).toBe(false)

      newPending.resolve({
        files: [{ fs_id: 20002, is_dir: false, path: '/剧集/新第2页.mp4', size: 1, name: '新第2页.mp4' }],
      })
      await flushPromises()
      expect(loadingMoreVisible()).toBe(false)
      expect(renderedNames()).toContain('新第2页.mp4')
    } finally {
      wrapper.unmount()
    }
  })

  it('返回根目录后从保存的根页码继续请求', async () => {
    const rootPage1 = [
      ...ROOT_FILES,
      ...filesIn('/根', 98, 21000),
    ]
    const rootPage2 = filesIn('/根2', 100, 22000)
    const rootPage3 = [{ fs_id: 23000, is_dir: false, path: '/根3/末页.mp4', size: 1, name: '末页.mp4' }]
    transfer.previewShareFiles.mockImplementation(async (req: { page?: number }) => {
      if ((req.page ?? 1) === 2) return { files: rootPage2 }
      if ((req.page ?? 1) === 3) return { files: rootPage3 }
      return { files: rootPage1 }
    })
    transfer.previewShareDir.mockResolvedValue({
      files: [{ fs_id: 23001, is_dir: false, path: '/剧集/01.mp4', size: 1, name: '01.mp4' }],
    })
    const wrapper = mountPathSelector({ files: rootPage1 })
    try {
      await flushPromises()
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()
      expect(transfer.previewShareFiles.mock.calls.at(-1)?.[0].page).toBe(2)

      itemByName('剧集').click()
      await flushPromises()
      rootBreadcrumb().click()
      await flushPromises()
      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()

      expect(transfer.previewShareFiles.mock.calls.at(-1)?.[0].page).toBe(3)
      expect(renderedNames()).toContain('末页.mp4')
      expect(renderedNames()).toContain('000.mp4')
    } finally {
      wrapper.unmount()
    }
  })

  it('整层事务确认完整后，后续滚动页不得降级缓存或表头', async () => {
    const all = filesIn('/根', 200, 24000)
    transfer.previewShareFiles.mockImplementation(async (req: { page?: number }) => ({
      files: pageSlice(all, req.page ?? 1),
    }))
    const firstPage = all.slice(0, 100).map(f => f.path)
    const wrapper = mountPathSelector({
      files: all.slice(0, 100),
      initialSelectedPaths: firstPage,
      emptySelectionMeansAll: true,
    })
    try {
      await flushPromises()
      expect(headerState()).toEqual({ checked: false, indeterminate: true })
      headerCheckbox().click()
      await flushPromises()
      expect(headerState()).toEqual({ checked: true, indeterminate: false })
      expect(emittedPaths(wrapper)).toEqual(all.map(f => f.path))

      await wrapper.get('.el-scrollbar__wrap').trigger('scroll')
      await flushPromises()

      expect(headerState(), '后续滚动页不得把完整缓存降级').toEqual({ checked: true, indeterminate: false })
      expect(emittedPaths(wrapper)).toEqual(all.map(f => f.path))
      expect(new Set(renderedNames()).size, '重复分页不得产生重复条目').toBe(renderedNames().length)
    } finally {
      wrapper.unmount()
    }
  })

  it('事务取消后不再请求 fetchAllChildren 的下一页', async () => {
    const all = filesIn('/剧集', 300, 25000)
    const page2 = deferred<{ files: typeof all }>()
    transfer.previewShareDir.mockImplementation(async (req: { page?: number }) => {
      if ((req.page ?? 1) === 1) return { files: pageSlice(all, 1) }
      if ((req.page ?? 1) === 2) return page2.promise
      return { files: pageSlice(all, 3) }
    })
    const firstPage = all.slice(0, 100).map(f => f.path)
    const wrapper = mountPathSelector({ initialSelectedPaths: firstPage })
    try {
      await flushPromises()
      itemByName('剧集').click()
      await flushPromises()
      expect(headerState()).toEqual({ checked: false, indeterminate: true })
      headerCheckbox().click()
      await flushPromises()
      expect(transfer.previewShareDir.mock.calls.some(c => (c[0].page ?? 1) === 2)).toBe(true)

      rootBreadcrumb().click()
      await flushPromises()
      page2.resolve({ files: pageSlice(all, 2) })
      await flushPromises()

      expect(transfer.previewShareDir.mock.calls.some(c => (c[0].page ?? 1) >= 3), '取消后不得继续请求下一页').toBe(false)
      expect(emittedPaths(wrapper)).toEqual(firstPage)
    } finally {
      wrapper.unmount()
    }
  })
})
