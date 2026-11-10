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
const uiPrefs = vi.hoisted(() => ({ highlightDeadShares: true, disableDeadShareActions: false }))

vi.mock('@/stores/uiPrefs', () => ({
  useUiPrefsStore: () => uiPrefs,
}))

vi.mock('@/api/share', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/share')>()
  return {
    SHARE_EXPIRE_OPTIONS: actual.SHARE_EXPIRE_OPTIONS,
    deadShareKind: actual.deadShareKind,
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

async function mountWith(
  list: ShareRecord[],
  isMobile: boolean,
  extra: Record<string, unknown> = { total: list.length },
) {
  mobile.value = isMobile
  shareApi.getShareList.mockResolvedValue({ list, page: 1, total: 0, ...extra })
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

  it('移动端卡片：浏览 / 下载 / 保存在同一行（缺值不显示），过期时间单独一行', async () => {
    const wrapper = await mountWith(
      [record({ downloadCount: 0, transferCount: 5 }), record({ shareId: 2 })],
      true,
    )
    const cards = wrapper.findAll('.share-card')
    expect(cards).toHaveLength(2)
    const stats = (i: number) => cards[i].findAll('.stats-row .stat').map((x) => x.text())
    expect(stats(0)).toEqual(['浏览 12', '下载 0', '保存 5'])
    expect(stats(1)).toEqual(['浏览 12'])
    expect(cards[0].findAll('.stats-row')).toHaveLength(1)
    expect(cards[0].find('.expire-row').text()).toContain('永久有效')
    expect(cards[0].find('.stats-row').text()).not.toContain('永久有效')
    wrapper.unmount()
  })
})

describe('SharesView 分页与计数（百度不返回 total）', () => {
  afterEach(() => {
    document.body.innerHTML = ''
    vi.clearAllMocks()
  })

  it('total 为 0 但列表非空时，顶部显示 count，不显示「0 条」', async () => {
    const list = Array.from({ length: 3 }, (_, i) => record({ shareId: i + 1 }))
    const wrapper = await mountWith(list, false, { total: 0, count: 3, has_more: false, nextpage: 0 })
    const header = wrapper.find('.page-header').text()
    expect(header).toContain('3 条分享记录')
    expect(header).not.toContain('0 条')
    // 只有一页：不显示任何分页
    expect(wrapper.find('.pagination-wrapper').exists()).toBe(false)
    wrapper.unmount()
  })

  it('旧后端没有 count 时用列表长度', async () => {
    const wrapper = await mountWith([record(), record({ shareId: 2 })], true, { total: 0 })
    expect(wrapper.find('.page-header').text()).toContain('2 条分享记录')
    wrapper.unmount()
  })

  it('has_more 驱动上一页 / 下一页', async () => {
    const wrapper = await mountWith([record()], false, { total: 0, count: 50, has_more: true, nextpage: 2 })
    const pager = wrapper.find('.simple-pager')
    expect(pager.exists()).toBe(true)
    expect(wrapper.find('.page-header').text()).toContain('第 1 页 · 本页 50 条')
    expect(pager.find('.pager-prev').attributes('disabled')).toBeDefined()
    expect(pager.find('.pager-next').attributes('disabled')).toBeUndefined()

    shareApi.getShareList.mockResolvedValue({ list: [record({ shareId: 9 })], total: 0, count: 1, has_more: false, nextpage: 0, page: 2 })
    await pager.find('.pager-next').trigger('click')
    await flushPromises()
    expect(shareApi.getShareList).toHaveBeenLastCalledWith(2)
    const after = wrapper.find('.simple-pager')
    expect(after.exists()).toBe(true)
    expect(after.text()).toContain('第 2 页')
    expect(after.find('.pager-next').attributes('disabled')).toBeDefined()
    expect(after.find('.pager-prev').attributes('disabled')).toBeUndefined()
    wrapper.unmount()
  })

  it('接口给了 total 时仍用完整分页条', async () => {
    const wrapper = await mountWith([record()], false, { total: 45 })
    expect(wrapper.find('.simple-pager').exists()).toBe(false)
    expect(wrapper.findComponent({ name: 'ElPagination' }).exists()).toBe(true)
    expect(wrapper.find('.page-header').text()).toContain('45 条分享记录')
    wrapper.unmount()
  })
})

type DeadShareVm = {
  shareList: ShareRecord[]
  getStatusType: (r: ShareRecord) => string
  formatExpireTime: (r: ShareRecord) => string
  isExpireLocked: (r: ShareRecord) => boolean
  shareRowClass: (p: { row: ShareRecord }) => string
}

describe('SharesView 失效分享（文件已删除 / 已过期）', () => {
  afterEach(() => {
    document.body.innerHTML = ''
    vi.clearAllMocks()
    uiPrefs.highlightDeadShares = true
    uiPrefs.disableDeadShareActions = false
  })

  // 与脱敏 fixture（backend/src/netdisk/testdata/share_record_page1.json）同构
  const deleted = () =>
    record({ shareId: 4, typicalPath: '分享的文件已被删除', expiredType: 1, status: 0, downloadCount: 0, transferCount: 0 })
  const expiredByPath = () =>
    record({ shareId: 3, typicalPath: '分享已过期', expiredType: 0, expiredTime: 0, status: 10 })
  const expiredByType = () => record({ shareId: 5, expiredType: -1, status: 10 })
  const normal = () => record({ shareId: 1, downloadCount: 3, transferCount: 1 })

  it('状态标签显示 已删除 / 已过期，而不是 正常 / 未知', async () => {
    const wrapper = await mountWith([normal(), deleted(), expiredByPath(), expiredByType()], true)
    const tags = wrapper.findAll('.share-card .share-status-tag').map((t) => t.text())
    expect(tags).toEqual(['正常', '已删除', '已过期', '已过期'])
    const vm = wrapper.vm as unknown as DeadShareVm
    expect(vm.getStatusType(vm.shareList[1])).toBe('danger')
    expect(vm.getStatusType(vm.shareList[2])).toBe('warning')
    // 已过期标记的分享视为已过期（不能改有效期）
    expect(vm.formatExpireTime(vm.shareList[2])).toBe('已过期')
    expect(vm.isExpireLocked(vm.shareList[2])).toBe(true)
    // 文件已删除但仍在有效期内：默认仍可改有效期
    expect(vm.isExpireLocked(vm.shareList[1])).toBe(false)
    wrapper.unmount()
  })

  it('高亮：桌面行 class 与移动端卡片 class，界面偏好关闭时不高亮', async () => {
    const wrapper = await mountWith([normal(), deleted(), expiredByPath()], true)
    const cards = wrapper.findAll('.share-card')
    expect(cards[0].classes()).not.toContain('dead-share')
    expect(cards[1].classes()).toContain('dead-share--deleted')
    expect(cards[2].classes()).toContain('dead-share--expired')
    const vm = wrapper.vm as unknown as DeadShareVm
    expect(vm.shareRowClass({ row: vm.shareList[1] })).toBe('dead-share dead-share--deleted')
    expect(vm.shareRowClass({ row: vm.shareList[0] })).toBe('')
    wrapper.unmount()

    uiPrefs.highlightDeadShares = false
    const plain = await mountWith([deleted()], true)
    expect(plain.find('.share-card').classes()).not.toContain('dead-share--deleted')
    // 标签不受高亮开关影响
    expect(plain.find('.share-status-tag').text()).toBe('已删除')
    plain.unmount()
  })

  it('「失效分享只能取消」开启时，失效分享只保留取消按钮', async () => {
    uiPrefs.disableDeadShareActions = true
    const wrapper = await mountWith([normal(), deleted()], true)
    const buttons = (i: number) =>
      wrapper
        .findAll('.share-card')[i]
        .findAll('.card-actions button')
        .map((b) => [b.text(), b.attributes('disabled') !== undefined])
    expect(buttons(0)).toEqual([
      ['提取码', false],
      ['复制', false],
      ['有效期', false],
      ['取消', false],
    ])
    expect(buttons(1)).toEqual([
      ['提取码', true],
      ['复制', true],
      ['有效期', true],
      ['取消', false],
    ])
    const vm = wrapper.vm as unknown as DeadShareVm
    expect(vm.isExpireLocked(vm.shareList[1])).toBe(true)
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
