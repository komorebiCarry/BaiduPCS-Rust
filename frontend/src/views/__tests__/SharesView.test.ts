import { flushPromises, mount } from '@vue/test-utils'
import { defineComponent, h, inject, provide, ref, type PropType } from 'vue'
import ElementPlus from 'element-plus'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import type { ShareRecord } from '@/api/share'
import SharesView from '../SharesView.vue'

const shareApi = vi.hoisted(() => ({
  getShareList: vi.fn(),
}))
const mobile = vi.hoisted(() => ({ value: false }))

vi.mock('@/api/share', () => ({
  getShareList: shareApi.getShareList,
  getShareDetail: vi.fn(),
  cancelShare: vi.fn(),
}))

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
