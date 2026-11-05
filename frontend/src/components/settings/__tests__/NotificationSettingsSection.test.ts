import { flushPromises, mount } from '@vue/test-utils'
import ElementPlus from 'element-plus'
import { afterEach, describe, expect, it, vi } from 'vitest'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import NotificationSettingsSection from '../NotificationSettingsSection.vue'
import { normalizeNotificationConfig, type NotificationConfig } from '@/api/config'

vi.mock('@/api/config', async () => {
  const actual = await vi.importActual<typeof import('@/api/config')>('@/api/config')
  return { ...actual, testNotification: vi.fn() }
})

function mountSection(cfg: Partial<NotificationConfig>) {
  const model = normalizeNotificationConfig(cfg)
  const wrapper = mount(NotificationSettingsSection, {
    attachTo: document.body,
    props: {
      modelValue: model,
      'onUpdate:modelValue': (v: NotificationConfig) => wrapper.setProps({ modelValue: v }),
    },
    global: { plugins: [ElementPlus], components: { ...ElementPlusIconsVue } },
  })
  return { wrapper, model }
}

afterEach(() => {
  document.body.innerHTML = ''
})

describe('NotificationSettingsSection 分享同步列出文件数（#183）', () => {
  it('显示列出文件数（默认 5），说明 0 = 不列并提示 {{files}} 占位符', async () => {
    const { wrapper } = mountSection({ enabled: true })
    await flushPromises()
    const input = document.querySelector<HTMLInputElement>('.file-list-limit input')!
    expect(input.value).toBe('5')
    const item = input.closest('.el-form-item')!
    expect(item.textContent).toContain('填 0 不列文件名')
    expect(item.textContent).toContain('{{files}}')
    wrapper.unmount()
  })

  it('总开关关闭时禁用；保存的 0 原样显示', async () => {
    const { wrapper } = mountSection({ enabled: false, share_sync_file_list_limit: 0 })
    await flushPromises()
    const input = document.querySelector<HTMLInputElement>('.file-list-limit input')!
    expect(input.disabled).toBe(true)
    expect(input.value).toBe('0')
    wrapper.unmount()
  })

  it('修改后写回 model；Webhook 占位符列表含 {{files}}', async () => {
    const { wrapper, model } = mountSection({
      enabled: true,
      webhook: { ...normalizeNotificationConfig().webhook, enabled: true },
    })
    await flushPromises()
    const num = wrapper.findComponent({ name: 'ElInputNumber' })
    num.vm.$emit('update:modelValue', 8)
    await flushPromises()
    expect(model.share_sync_file_list_limit).toBe(8)
    const phs = Array.from(document.querySelectorAll('code.ph')).map((c) => c.textContent)
    expect(phs).toContain('{{files}}')
    wrapper.unmount()
  })
})
