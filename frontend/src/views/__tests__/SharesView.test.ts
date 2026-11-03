import { flushPromises, mount } from '@vue/test-utils'
import { defineComponent, h, inject, provide, ref, type PropType } from 'vue'
import ElementPlus from 'element-plus'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import type { ShareRecord } from '@/api/share'
import SharesView from '../SharesView.vue'

const shareApi = vi.hoisted(() => ({
  getShareList: vi.fn(),
  modifyShareExpire: vi.fn(),
}))
const mobile = vi.hoisted(() => ({ value: false }))

vi.mock('@/api/share', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/share')>()
  return {
    SHARE_EXPIRE_OPTIONS: actual.SHARE_EXPIRE_OPTIONS,
    getShareList: shareApi.getShareList,
    getShareDetail: vi.fn(),
    cancelShare: vi.fn(),
    modifyShareExpire: shareApi.modifyShareExpire,
  }
})

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(mobile.value),
}))

function record(extra: Partial<ShareRecord> = {}): ShareRecord {
  return {
    shareId: 1,
    fsIds: [1],
    shortlink: 'https://pan.baidu.com/s/1abc',
    status: 0,
    public: 0,
    typicalCategory: 6,
    typicalPath: '/a/demo.zip',
    expiredType: 1,
    expiredTime: 0,
    viewCount: 12,
    ...extra,
  }
}

// happy-dom 下 el-table 的 MutationObserver 不可用：用最小桩按列渲染表头和每行单元格
const TABLE_DATA = Symbol('table-data')
const ElTableStub = defineComponent({
  props: { data: { type: Array as PropType<Record<string, unknown>[]>, default: () => [] } },
  setup(props, { slots }) {
    provide(TABLE_DATA, () => props.data)
    return () => h('div', { class: 'table-stub' }, slots.default?.())
  },
})
const ElTableColumnStub = defineComponent({
  props: { label: String, prop: String },
  setup(props, { slots }) {
    const rows = inject<() => Record<string, unknown>[]>(TABLE_DATA, () => [])
    return () =>
      h('div', { class: 'col' }, [
        h('span', { class: 'th' }, props.label ?? ''),
        ...rows().map((row) =>
          h(
            'span',
            { class: 'td' },
            slots.default ? slots.default({ row }) : String(props.prop ? row[props.prop] : ''),
          ),
        ),
      ])
  },
})

async function mountWith(list: ShareRecord[], isMobile: boolean) {
  mobile.value = isMobile
  shareApi.getShareList.mockResolvedValue({ list, total: list.length, page: 1 })
  const wrapper = mount(SharesView, {
    attachTo: document.body,
    global: {
      plugins: [ElementPlus],
      components: { ...ElementPlusIconsVue },
      stubs: { 'el-table': ElTableStub, 'el-table-column': ElTableColumnStub },
    },
  })
  await flushPromises()
  return wrapper
}

describe('SharesView 下载 / 保存次数', () => {
  afterEach(() => {
    document.body.innerHTML = ''
    vi.clearAllMocks()
  })

  it('接口未返回 dCnt / tCnt 时不显示下载、保存列（不臆造为 0）', async () => {
    const wrapper = await mountWith([record()], false)
    const headers = wrapper.findAll('.th').map((th) => th.text())
    expect(headers).toContain('浏览')
    expect(headers).not.toContain('下载')
    expect(headers).not.toContain('保存')
    wrapper.unmount()
  })

  it('接口返回时 PC 表格显示对应列，缺值的行显示 -', async () => {
    const wrapper = await mountWith(
      [record({ downloadCount: 29, transferCount: 184 }), record({ shareId: 2 })],
      false,
    )
    const column = (label: string) =>
      wrapper
        .findAll('.col')
        .find((c) => c.find('.th').text() === label)!
        .findAll('.td')
        .map((td) => td.text())
    expect(column('下载')).toEqual(['29', '-'])
    expect(column('保存')).toEqual(['184', '-'])
    wrapper.unmount()
  })

  it('移动端卡片只在有值时显示下载 / 保存行', async () => {
    const wrapper = await mountWith(
      [record({ downloadCount: 0, transferCount: 5 }), record({ shareId: 2 })],
      true,
    )
    const cards = wrapper.findAll('.share-card')
    expect(cards).toHaveLength(2)
    expect(cards[0].text()).toContain('下载:0 次')
    expect(cards[0].text()).toContain('保存:5 次')
    expect(cards[1].text()).not.toContain('下载:')
    expect(cards[1].text()).not.toContain('保存:')
    wrapper.unmount()
  })
})

describe('SharesView 修改有效期', () => {
  afterEach(() => {
    document.body.innerHTML = ''
    vi.clearAllMocks()
    vi.restoreAllMocks()
  })

  const future = () => Math.floor(Date.now() / 1000) + 86400

  it('下拉选项只有网页端提供的 1/3/7/30 天和永久', async () => {
    const wrapper = await mountWith([record({ expiredType: 0, expiredTime: future() })], false)
    const dropdown = wrapper.findAllComponents({ name: 'ElDropdown' }).find((d) =>
      d.classes().includes('row-expire-dropdown'),
    )!
    expect(dropdown.exists()).toBe(true)
    const items = dropdown
      .findAllComponents({ name: 'ElDropdownItem' })
      .map((i) => [i.text(), i.props('command')])
    expect(items).toEqual([
      ['1天有效', 1],
      ['3天有效', 3],
      ['7天有效', 7],
      ['30天有效', 30],
      ['永久有效', -1],
    ])
    wrapper.unmount()
  })

  it('单条修改后用返回值就地更新过期时间，不整表刷新', async () => {
    const wrapper = await mountWith([record({ expiredType: 0, expiredTime: future() })], false)
    shareApi.getShareList.mockClear()
    shareApi.modifyShareExpire.mockResolvedValue({
      results: [{ shareId: 1, success: true, expiredType: 1, expiredTime: 0 }],
      successCount: 1,
      failedCount: 0,
    })
    const vm = wrapper.vm as any
    await vm.handleModifyExpire(vm.shareList[0], -1)
    await flushPromises()
    expect(shareApi.modifyShareExpire).toHaveBeenCalledWith([1], -1)
    expect(shareApi.getShareList).not.toHaveBeenCalled()
    expect(vm.shareList[0].expiredType).toBe(1)
    expect(vm.formatExpireTime(vm.shareList[0])).toBe('永久有效')

    const newTs = future() + 3 * 86400
    shareApi.modifyShareExpire.mockResolvedValue({
      results: [{ shareId: 1, success: true, expiredType: 0, expiredTime: newTs }],
      successCount: 1,
      failedCount: 0,
    })
    await vm.handleModifyExpire(vm.shareList[0], 3)
    await flushPromises()
    expect(shareApi.modifyShareExpire).toHaveBeenLastCalledWith([1], 3)
    expect(vm.shareList[0]).toMatchObject({ expiredType: 0, expiredTime: newTs })
    expect(vm.expireUpdatingIds).toEqual([])
    wrapper.unmount()
  })

  it('失败时不改行，并显示百度错误原因', async () => {
    const { ElMessage } = await import('element-plus')
    const errorSpy = vi.spyOn(ElMessage, 'error')
    const ts = future()
    const wrapper = await mountWith([record({ expiredType: 0, expiredTime: ts })], false)
    shareApi.modifyShareExpire.mockResolvedValue({
      results: [
        {
          shareId: 1,
          success: false,
          error: '该分享已开启自动转存，无法修改或恢复有效期（errno=60024）',
        },
      ],
      successCount: 0,
      failedCount: 1,
    })
    const vm = wrapper.vm as any
    await vm.handleModifyExpire(vm.shareList[0], 7)
    await flushPromises()
    expect(vm.shareList[0]).toMatchObject({ expiredType: 0, expiredTime: ts })
    expect(errorSpy).toHaveBeenCalledWith(expect.stringContaining('errno=60024'))
    wrapper.unmount()
  })

  it('请求本身失败时重新拉列表，显示真实有效期', async () => {
    const wrapper = await mountWith([record({ expiredType: 0, expiredTime: future() })], false)
    shareApi.getShareList.mockClear()
    shareApi.modifyShareExpire.mockRejectedValue(new Error('timeout of 35000ms exceeded'))
    const vm = wrapper.vm as any
    await vm.handleModifyExpire(vm.shareList[0], 30)
    await flushPromises()
    expect(shareApi.getShareList).toHaveBeenCalledTimes(1)
    expect(vm.expireUpdatingIds).toEqual([])
    wrapper.unmount()
  })

  it('已过期的分享不能修改（与网页端一致）', async () => {
    const wrapper = await mountWith(
      [
        record({ shareId: 1, expiredType: -1, expiredTime: 0 }),
        record({ shareId: 2, expiredType: 0, expiredTime: 1000 }),
      ],
      true,
    )
    const vm = wrapper.vm as any
    expect(vm.isExpireLocked(vm.shareList[0])).toBe(true)
    expect(vm.isExpireLocked(vm.shareList[1])).toBe(true)
    await vm.handleModifyExpire(vm.shareList[0], 7)
    expect(shareApi.modifyShareExpire).not.toHaveBeenCalled()
    const dropdowns = wrapper
      .findAllComponents({ name: 'ElDropdown' })
      .filter((d) => d.classes().includes('row-expire-dropdown'))
    expect(dropdowns).toHaveLength(2)
    expect(dropdowns.every((d) => d.props('disabled'))).toBe(true)
    wrapper.unmount()
  })

  it('批量修改跳过已过期的分享，部分失败给出汇总', async () => {
    const { ElMessage, ElMessageBox } = await import('element-plus')
    const confirmSpy = vi.spyOn(ElMessageBox, 'confirm').mockResolvedValue('confirm' as any)
    const warnSpy = vi.spyOn(ElMessage, 'warning')
    const ts = future()
    const wrapper = await mountWith(
      [
        record({ shareId: 1, expiredType: 0, expiredTime: ts }),
        record({ shareId: 2, expiredType: -1, expiredTime: 0 }),
        record({ shareId: 3, expiredType: 1, expiredTime: 0 }),
      ],
      false,
    )
    const vm = wrapper.vm as any
    vm.handleSelectionChange(vm.shareList)
    await flushPromises()
    expect(
      wrapper
        .findAllComponents({ name: 'ElDropdown' })
        .some((d) => d.classes().includes('batch-expire-dropdown')),
    ).toBe(true)
    const newTs = ts + 30 * 86400
    shareApi.modifyShareExpire.mockResolvedValue({
      results: [
        { shareId: 1, success: true, expiredType: 0, expiredTime: newTs },
        { shareId: 3, success: false, error: '分享链接不存在（可能已被取消）（errno=24）' },
      ],
      successCount: 1,
      failedCount: 1,
    })
    await vm.handleBatchExpire(30)
    await flushPromises()
    expect(confirmSpy).toHaveBeenCalledWith(
      expect.stringContaining('已跳过 1 个已过期'),
      '批量修改有效期',
      expect.anything(),
    )
    expect(shareApi.modifyShareExpire).toHaveBeenCalledWith([1, 3], 30)
    expect(vm.shareList[0].expiredTime).toBe(newTs)
    expect(vm.shareList[2]).toMatchObject({ expiredType: 1, expiredTime: 0 })
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('成功 1 个，失败 1 个'))
    expect(vm.batchExpireUpdating).toBe(false)
    wrapper.unmount()
  })

  it('切换账号后丢弃旧账号的修改回包', async () => {
    const ts = future()
    const wrapper = await mountWith([record({ expiredType: 0, expiredTime: ts })], false)
    let resolve!: (v: unknown) => void
    shareApi.modifyShareExpire.mockReturnValue(new Promise((r) => (resolve = r)))
    const vm = wrapper.vm as any
    const pending = vm.handleModifyExpire(vm.shareList[0], -1)
    // 新账号的列表里恰好也有同 ID 的分享
    shareApi.getShareList.mockResolvedValue({
      list: [record({ expiredType: 0, expiredTime: ts })],
      total: 1,
      page: 1,
    })
    window.dispatchEvent(new Event('multi-account:active-changed'))
    await flushPromises()
    resolve({
      results: [{ shareId: 1, success: true, expiredType: 1, expiredTime: 0 }],
      successCount: 1,
      failedCount: 0,
    })
    await pending
    await flushPromises()
    expect(vm.shareList[0]).toMatchObject({ expiredType: 0, expiredTime: ts })
    wrapper.unmount()
  })
})
