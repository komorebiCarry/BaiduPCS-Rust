import { mount } from '@vue/test-utils'
import { nextTick, ref } from 'vue'
import ElementPlus from 'element-plus'
import { createPinia, setActivePinia } from 'pinia'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import FilePickerModal from '../FilePickerModal.vue'
import { useFilePickerStore } from '@/stores/filepicker'
import type { FileEntry } from '@/api/filesystem'

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(false),
}))

vi.mock('@/api/filesystem', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/filesystem')>()
  return {
    ...actual,
    listDirectory: vi.fn().mockResolvedValue({ entries: [], total: 0, hasMore: false, path: '/' }),
    getRoots: vi.fn().mockResolvedValue({ roots: [] }),
    gotoPath: vi.fn().mockResolvedValue({ valid: false }),
  }
})

const entry: FileEntry = {
  id: '1',
  name: 'a.zip',
  entryType: 'file',
  size: 10,
  createdAt: '',
  updatedAt: '',
  path: '/data/a.zip',
}

function mountModal(props: Record<string, unknown>) {
  return mount(FilePickerModal, {
    props: { modelValue: true, mode: 'upload', ...props },
    global: {
      plugins: [ElementPlus],
      stubs: {
        // 直接渲染内容和底栏，避免 el-dialog 的 teleport / 动画
        'el-dialog': { template: '<div><slot /><slot name="footer" /></div>' },
        NavigatorBar: true,
        FileList: true,
        EmptyState: true,
        ErrorState: true,
        Lock: true,
      },
    },
  })
}

describe('FilePickerModal 跳过秒传开关', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
  })

  it('未开启 showSkipRapid 时不显示开关，事件第 4 个参数为 undefined', async () => {
    const wrapper = mountModal({ showConflictStrategy: true })
    await nextTick()
    expect(wrapper.find('.skip-rapid-switch').exists()).toBe(false)

    useFilePickerStore().multiSelection = [entry]
    ;(wrapper.vm as any).handleConfirm()
    const args = wrapper.emitted('select-multiple')![0]
    expect(args[3]).toBeUndefined()
  })

  it('打开时按设置默认值初始化，确认时随事件带出（多选与单选）', async () => {
    const wrapper = mountModal({ showSkipRapid: true, defaultSkipRapidUpload: true })
    await nextTick()
    expect(wrapper.find('.skip-rapid-switch').exists()).toBe(true)

    const vm = wrapper.vm as any
    vm.handleOpen()
    expect(vm.skipRapidUpload).toBe(true)

    const store = useFilePickerStore()
    store.multiSelection = [entry]
    vm.handleConfirm()
    expect(wrapper.emitted('select-multiple')![0][3]).toBe(true)

    // 用户在本次对话框里关掉 → 带出 false
    await wrapper.setProps({ modelValue: true, multiple: false })
    vm.skipRapidUpload = false
    store.selection = entry
    vm.handleConfirm()
    expect(wrapper.emitted('select')![0][3]).toBe(false)

    // 再次打开恢复为设置默认值
    vm.handleOpen()
    expect(vm.skipRapidUpload).toBe(true)
  })
})
