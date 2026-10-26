import { flushPromises, mount } from '@vue/test-utils'
import { ref } from 'vue'
import ElementPlus from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import * as ElementPlusIconsVue from '@element-plus/icons-vue'
import SettingsView from '../SettingsView.vue'

const configApi = vi.hoisted(() => ({
  getConfig: vi.fn(),
  updateConfig: vi.fn(),
}))

vi.mock('@/api/config', async () => {
  const actual = await vi.importActual<typeof import('@/api/config')>('@/api/config')
  return {
    ...actual,
    getConfig: configApi.getConfig,
    updateConfig: configApi.updateConfig,
    getRecommendedConfig: vi.fn(async () => ({
      vip_name: '普通用户',
      warnings: [],
      recommended: { threads: 4, chunk_size: 1048576, max_tasks: 4 },
    })),
    getProxyStatus: vi.fn(async () => ({})),
    testProxyConnection: vi.fn(async () => ({})),
    getTransferConfig: vi.fn(async () => ({})),
    updateTransferConfig: vi.fn(async () => ({})),
    resetToRecommended: vi.fn(async () => ({})),
  }
})

const rules = vi.hoisted(() => ({
  capabilities: vi.fn(),
  validate: vi.fn(),
}))

vi.mock('@/api/shareSync', () => ({
  getExcludeRuleCapabilities: rules.capabilities,
  validateExcludeRules: rules.validate,
}))

vi.mock('@/api/transfer', () => ({
  cleanupOrphanedTempDirs: vi.fn(),
  getTransferConfig: vi.fn(async () => ({})),
}))

vi.mock('@/api/upload', () => ({}))

vi.mock('@/api/download', () => ({}))

// 设置页通过 config store 读取配置；这里直接转给被测的 getConfig mock
vi.mock('@/stores/config', () => ({
  useConfigStore: () => ({
    config: null,
    fetchConfig: async () => configApi.getConfig(),
  }),
}))

vi.mock('vue-router', () => ({
  useRoute: () => ({ query: {} }),
}))

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(false),
}))

// 版本号由构建期注入；测试环境直接给常量，避免依赖 vite define
vi.mock('@/utils/version', () => ({
  APP_VERSION: '2.3.3',
  APP_VERSION_LABEL: 'v2.3.3',
}))

vi.mock('@/components/settings/AuthSettingsSection.vue', () => ({
  default: { name: 'AuthSettingsSection', template: '<div />' },
}))
vi.mock('@/components/settings/AccountManagementSection.vue', () => ({
  default: { name: 'AccountManagementSection', template: '<div />' },
}))
vi.mock('@/views/settings/BudgetPanel.vue', () => ({
  default: { name: 'BudgetPanel', template: '<div />' },
}))
vi.mock('@/components/FilePicker', () => ({
  FilePickerModal: { name: 'FilePickerModal', template: '<div />' },
}))

const BASE_CONFIG = {
  server: { host: '127.0.0.1', port: 8080 },
  download: {},
  upload: {},
  filesystem: { allowed_paths: [], default_path: null },
  network: { proxy: {} },
  share_sync: {
    exclude_preset_rules: ['*.nfo'],
    exclude_regex_preset_rules: ['^/股票/按月归档/2025.*$'],
  },
  ui: {},
}

async function mountSettings() {
  const wrapper = mount(SettingsView, {
    attachTo: document.body,
    global: {
      plugins: [ElementPlus],
      // 应用入口全局注册了所有图标；测试里同样注册，避免模板解析失败
      components: { ...ElementPlusIconsVue },
    },
  })
  await flushPromises()
  return wrapper
}

function presetBlock(): HTMLElement {
  const el = document.querySelector<HTMLElement>('#section-sharesync .preset-rules-editor')
  expect(el, '应有分享同步词表区块').toBeTruthy()
  return el!
}

function presetInput(): HTMLTextAreaElement {
  return presetBlock().querySelector<HTMLTextAreaElement>('textarea')!
}

function addButton(): HTMLButtonElement {
  return Array.from(presetBlock().querySelectorAll<HTMLButtonElement>('button'))
      .find((b) => b.textContent?.trim() === '添加')!
}

function kindInputs(): HTMLInputElement[] {
  return Array.from(presetBlock().querySelectorAll<HTMLInputElement>('.el-radio-button input'))
}

beforeEach(() => {
  configApi.getConfig.mockReset().mockResolvedValue(JSON.parse(JSON.stringify(BASE_CONFIG)))
  configApi.updateConfig.mockReset().mockResolvedValue('ok')
  rules.capabilities.mockReset().mockResolvedValue({
    supports_regex: true,
    regex_limits: { max_rules: 64, max_rule_bytes: 1024, max_total_bytes: 32768, max_test_path_bytes: 4096 },
  })
  rules.validate.mockReset().mockResolvedValue({ valid: true, errors: [] })
})

afterEach(() => {
  document.body.innerHTML = ''
})

describe('SettingsView 排除规则词表', () => {
  it('默认展示通配符词表，并标出正则词表条数互不影响', async () => {
    const wrapper = await mountSettings()

    expect(presetBlock().textContent).toContain('*.nfo')
    const tags = Array.from(presetBlock().querySelectorAll<HTMLElement>('.el-tag'))
    expect(tags).toHaveLength(1)
    expect(tags[0].textContent?.trim()).toBe('*.nfo')

    // 切到正则：只显示正则词表，并带「正则」标记
    kindInputs()[1].click()
    await flushPromises()
    const regexTags = Array.from(presetBlock().querySelectorAll<HTMLElement>('.el-tag'))
    expect(regexTags).toHaveLength(1)
    expect(regexTags[0].textContent).toContain('正则')
    expect(regexTags[0].textContent).toContain('^/股票/按月归档/2025.*$')
    wrapper.unmount()
  })

  it('新增通配符词表不触碰正则词表', async () => {
    const wrapper = await mountSettings()

    presetInput().value = '*.tmp'
    presetInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    expect(rules.validate, '纯通配符新增不需要后端校验').not.toHaveBeenCalled()
    const globTags = () => Array.from(presetBlock().querySelectorAll<HTMLElement>('.el-tag'))
        .map((t) => t.textContent?.trim() ?? '')
    expect(globTags()).toContain('*.nfo')
    expect(globTags()).toContain('*.tmp')

    // 切到正则：正则词表保持原样，没有被 glob 新增污染
    kindInputs()[1].click()
    await flushPromises()
    expect(presetBlock().querySelectorAll<HTMLElement>('.el-tag')).toHaveLength(1)
    expect(presetBlock().textContent).toContain('^/股票/按月归档/2025.*$')
    wrapper.unmount()
  })

  it('新增正则词表先经后端整批校验，通过后才保存', async () => {
    const wrapper = await mountSettings()
    kindInputs()[1].click()
    await flushPromises()

    presetInput().value = '^/基金/.*$'
    presetInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    expect(rules.validate).toHaveBeenCalledTimes(1)
    const req = rules.validate.mock.calls[0][0] as {
      exclude_patterns: string[]
      exclude_regex_patterns: string[]
    }
    // 校验使用完整的候选集合（含 glob 与已有正则）
    expect(req.exclude_patterns).toEqual(['*.nfo'])
    expect(req.exclude_regex_patterns).toEqual(['^/股票/按月归档/2025.*$', '^/基金/.*$'])

    const regexTexts = Array.from(presetBlock().querySelectorAll<HTMLElement>('.el-tag'))
        .map((t) => t.textContent ?? '')
    expect(regexTexts).toHaveLength(2)
    expect(regexTexts[1]).toContain('^/基金/.*$')

    // 正则不得污染通配符词表
    kindInputs()[0].click()
    await flushPromises()
    const globTexts = Array.from(presetBlock().querySelectorAll<HTMLElement>('.el-tag'))
        .map((t) => t.textContent?.trim() ?? '')
    expect(globTexts).toEqual(['*.nfo'])
    wrapper.unmount()
  })

  it('正则词表校验失败时不修改配置并显示行号', async () => {
    rules.validate.mockResolvedValue({
      valid: false,
      errors: [{ kind: 'regex', index: 1, message: '无法编译正则：unclosed group' }],
    })
    const wrapper = await mountSettings()
    kindInputs()[1].click()
    await flushPromises()

    presetInput().value = '('
    presetInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    expect(configApi.updateConfig, '校验失败不得保存').not.toHaveBeenCalled()
    const error = presetBlock().querySelector<HTMLElement>('.preset-rules-error')
    expect(error?.textContent).toContain('正则本次新增第 1 条')
    expect(presetInput().value, '失败时保留输入').toBe('(')
    wrapper.unmount()
  })

  it('后端不支持正则时禁用该类型选项', async () => {
    rules.capabilities.mockRejectedValue(Object.assign(new Error('request failed'), {
      response: { status: 404 },
    }))
    const wrapper = await mountSettings()

    expect(kindInputs()[1].disabled, '404 时应禁用正则词表').toBe(true)
    expect(presetBlock().textContent).toContain('当前后端仅支持通配符')
    wrapper.unmount()
  })

  it('删除只影响当前类型的词表', async () => {
    const wrapper = await mountSettings()
    kindInputs()[1].click()
    await flushPromises()

    const close = presetBlock().querySelector<HTMLElement>('.el-tag .el-tag__close')!
    close.click()
    await flushPromises()

    expect(presetBlock().textContent).toContain('暂无常用规则')

    // 通配符词表不受影响
    kindInputs()[0].click()
    await flushPromises()
    const globTexts = Array.from(presetBlock().querySelectorAll<HTMLElement>('.el-tag'))
        .map((t) => t.textContent?.trim() ?? '')
    expect(globTexts).toEqual(['*.nfo'])
    wrapper.unmount()
  })

  it('旧配置缺少正则词表字段时按空处理且不报错', async () => {
    const legacy = JSON.parse(JSON.stringify(BASE_CONFIG))
    delete legacy.share_sync.exclude_regex_preset_rules
    configApi.getConfig.mockResolvedValue(legacy)
    const wrapper = await mountSettings()

    kindInputs()[1].click()
    await flushPromises()
    expect(presetBlock().textContent).toContain('暂无常用规则')
    expect(presetBlock().querySelector('.preset-rules-error'), '缺字段不应产生错误提示').toBeNull()
    wrapper.unmount()
  })
})

describe('SettingsView 词表异步校验快照（F2）', () => {
  const regexTags = () =>
    Array.from(presetBlock().querySelectorAll<HTMLElement>('.el-tag'))
        .map((t) => t.textContent?.trim() ?? '')

  it('校验期间改输入并删除词表：返回后不覆盖新输入、不复活已删项', async () => {
    let release: (v: unknown) => void = () => {}
    rules.validate.mockReturnValue(new Promise((resolve) => {
      release = resolve
    }))
    const wrapper = await mountSettings()
    kindInputs()[1].click()
    await flushPromises()
    expect(regexTags()).toHaveLength(1)

    // 发起 ^/slow$ 的新增，校验响应被挂起
    presetInput().value = '^/slow$'
    presetInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    // 校验未返回时：改输入 + 删掉既有正则词表
    presetInput().value = '^/newer$'
    presetInput().dispatchEvent(new Event('input'))
    await flushPromises()
    presetBlock().querySelector<HTMLElement>('.el-tag .el-tag__close')!.click()
    await flushPromises()

    release({ valid: true, errors: [] })
    await flushPromises()

    expect(presetInput().value, '不得清空更新的输入').toBe('^/newer$')
    expect(regexTags(), '不得复活已删除的词表项，也不得落过期新增').toEqual([])
    expect(presetBlock().querySelector('.preset-rules-error')).toBeNull()
    wrapper.unmount()
  })

  it('校验期间切换类型：旧类型的成功结果被丢弃', async () => {
    let release: (v: unknown) => void = () => {}
    rules.validate.mockReturnValue(new Promise((resolve) => {
      release = resolve
    }))
    const wrapper = await mountSettings()
    kindInputs()[1].click()
    await flushPromises()

    presetInput().value = '^/slow$'
    presetInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    kindInputs()[0].click()
    await flushPromises()
    release({ valid: true, errors: [] })
    await flushPromises()

    kindInputs()[1].click()
    await flushPromises()
    expect(regexTags(), '切换类型后旧候选不得写入').toEqual(['正则^/股票/按月归档/2025.*$'])
    wrapper.unmount()
  })

  it('校验期间无改动则正常写入并清空来源输入', async () => {
    let release: (v: unknown) => void = () => {}
    rules.validate.mockReturnValue(new Promise((resolve) => {
      release = resolve
    }))
    const wrapper = await mountSettings()
    kindInputs()[1].click()
    await flushPromises()

    presetInput().value = '^/ok$'
    presetInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    release({ valid: true, errors: [] })
    await flushPromises()

    expect(presetInput().value).toBe('')
    expect(regexTags()).toEqual(['正则^/股票/按月归档/2025.*$', '正则^/ok$'])
    wrapper.unmount()
  })
})
