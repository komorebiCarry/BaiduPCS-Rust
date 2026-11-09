import { flushPromises, mount } from '@vue/test-utils'
import { ref } from 'vue'
import ElementPlus, { ElMessage } from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import ShareIncludeExcludeEditor from '../ShareIncludeExcludeEditor.vue'

const transfer = vi.hoisted(() => ({
  previewShareFiles: vi.fn(),
  previewShareDir: vi.fn(),
}))

vi.mock('@/api/transfer', () => ({
  previewShareFiles: transfer.previewShareFiles,
  previewShareDir: transfer.previewShareDir,
}))

// 常用规则词表来自后端配置；mock 是有状态的：getConfig 返回当前持久化配置，
// updateConfig 把保存结果写回同一份状态，用于验证「保存 → 重载 → 点选」链路。
// 每个用例可改 presetRules / regexPresetRules 控制词表内容。
const config = vi.hoisted(() => ({
  presetRules: [] as string[],
  regexPresetRules: [] as string[],
  // 与词表无关的配置段：保存时必须原样保留
  otherConfig: {
    ui: { remember_netdisk_path: true, account_quick_bar: false },
    download: { max_global_threads: 8 },
  } as Record<string, unknown>,
  updateConfig: vi.fn(),
}))

vi.mock('@/api/config', () => ({
  getConfig: vi.fn(async () => ({
    ...config.otherConfig,
    share_sync: {
      exclude_preset_rules: [...config.presetRules],
      exclude_regex_preset_rules: [...config.regexPresetRules],
    },
  })),
  updateConfig: config.updateConfig,
}))

// 后端能力与规则校验：默认支持正则且校验通过
const rules = vi.hoisted(() => ({
  capabilities: vi.fn(),
  validate: vi.fn(),
}))

vi.mock('@/api/shareSync', () => ({
  getExcludeRuleCapabilities: rules.capabilities,
  validateExcludeRules: rules.validate,
}))

const routerPush = vi.hoisted(() => vi.fn())

vi.mock('vue-router', () => ({
  useRouter: () => ({ push: routerPush }),
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
  kind: 'personal',
  token: 'tok',
}

function mountEditor(props: Record<string, unknown> = {}) {
  return mount(ShareIncludeExcludeEditor, {
    attachTo: document.body,
    props: {
      shareUrl: 'https://pan.baidu.com/s/1y7CluAbCdEfGh',
      password: null,
      ownerLoggedIn: true,
      includePaths: [],
      excludePatterns: [],
      excludePaths: [],
      ...props,
    },
    global: { plugins: [ElementPlus] },
  })
}


/** 当前可见的 el-select 下拉项。页面上现有「包含规则」和「排除规则」两个类型下拉，不能全页 query。 */
function visibleSelectDropdownItems(): HTMLElement[] {
  const poppers = Array.from(document.body.querySelectorAll<HTMLElement>('.el-popper')).filter((el) => {
    if (!el.querySelector('.el-select-dropdown')) return false
    if (el.getAttribute('aria-hidden') === 'true') return false
    if ((el as HTMLElement).style.display === 'none') return false
    return true
  })
  const popper = poppers.at(-1)
  if (!popper) return []
  return Array.from(popper.querySelectorAll<HTMLElement>('.el-select-dropdown__item:not(.is-disabled-hidden)'))
}

/** 某个字段区块（按标签文案定位） */
function fieldBlock(label: string): HTMLElement {
  const blocks = Array.from(document.body.querySelectorAll<HTMLElement>('.field-block'))
  const block = blocks.find((b) => b.querySelector('.field-label')?.textContent?.trim() === label)
  expect(block, `应存在「${label}」区块`).toBeTruthy()
  return block!
}

function fieldLabelTexts(): string[] {
  return Array.from(document.body.querySelectorAll<HTMLElement>('.field-block .field-label'))
      .map((el) => el.textContent?.trim() ?? '')
}

/** 区块内的「从分享浏览」按钮 */
function browseButton(label: string): HTMLElement {
  const btn = Array.from(fieldBlock(label).querySelectorAll<HTMLElement>('.path-actions button'))
      .find((b) => b.textContent?.trim() === '从分享浏览')
  expect(btn, `「${label}」区块应有浏览按钮`).toBeTruthy()
  return btn!
}

/** 区块内的「使用说明」开关（按可访问名称定位） */
function helpButton(accessibleName: string): HTMLElement {
  const btn = document.body.querySelector<HTMLElement>(`button[aria-label="${accessibleName}"]`)
  expect(btn, `应有可访问名为「${accessibleName}」的说明开关`).toBeTruthy()
  return btn!
}

/** 区块内当前展开的说明文案（未展开时为 null） */
function helpText(label: string): string | null {
  const el = fieldBlock(label).querySelector<HTMLElement>('.field-help')
  return el ? (el.textContent?.trim() ?? '') : null
}

/** 选择器里的条目（按显示名） */
function fileItem(name: string): HTMLElement {
  const items = Array.from(document.body.querySelectorAll<HTMLElement>('.file-item'))
  const item = items.find((el) => el.querySelector('.file-name')?.textContent?.trim() === name)
  expect(item, `选择器应包含条目「${name}」`).toBeTruthy()
  return item!
}

function checkboxOf(item: HTMLElement): HTMLInputElement | null {
  return item.querySelector<HTMLInputElement>('input[type="checkbox"]')
}

function confirmButton(): HTMLElement {
  const btns = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
  const btn = btns.find((b) => b.textContent?.trim() === '确定')
  expect(btn, '选择器应有「确定」按钮').toBeTruthy()
  return btn!
}

function cancelButton(): HTMLElement {
  const btns = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
  const btn = btns.find((b) => b.textContent?.trim() === '取消')
  expect(btn, '选择器应有「取消」按钮').toBeTruthy()
  return btn!
}

/** 选择器里某个条目的复选框状态（标准 checked / indeterminate） */
function checkboxStateOf(name: string): { checked: boolean; indeterminate: boolean } {
  const input = fileItem(name).querySelector<HTMLElement>('.el-checkbox__input')
  expect(input, `条目「${name}」应有复选框`).toBeTruthy()
  return {
    checked: input!.classList.contains('is-checked'),
    indeterminate: input!.classList.contains('is-indeterminate'),
  }
}

/** 选择器头部的全选框 */
function headerCheckbox(): HTMLInputElement {
  const input = document.body.querySelector<HTMLInputElement>('.selector-header input[type="checkbox"]')
  expect(input, '选择器应有全选框').toBeTruthy()
  return input!
}

/** 选择器底部的状态提示 */
function pickerHint(): string {
  const hint = document.body.querySelector<HTMLElement>('.picker-hint')
  return hint?.textContent?.trim() ?? ''
}

beforeEach(() => {
  transfer.previewShareFiles.mockReset()
  transfer.previewShareDir.mockReset()
  transfer.previewShareFiles.mockResolvedValue({
    files: [
      { fs_id: 1, is_dir: true, path: '/剧集', size: 0, name: '剧集' },
      { fs_id: 3, is_dir: true, path: '/广告', size: 0, name: '广告' },
      { fs_id: 2, is_dir: false, path: '/说明.txt', size: 10, name: '说明.txt' },
    ],
    share_info: SHARE_INFO,
  })
  transfer.previewShareDir.mockResolvedValue({
    files: [
      { fs_id: 11, is_dir: false, path: '/剧集/01.mp4', size: 5, name: '01.mp4' },
      { fs_id: 12, is_dir: false, path: '/剧集/02.mp4', size: 5, name: '02.mp4' },
    ],
  })
  config.presetRules = []
  config.regexPresetRules = []
  config.updateConfig.mockClear()
  // 默认实现模拟后端持久化：把保存的词表写回状态，供后续重载读取
  config.updateConfig.mockImplementation(async (cfg: {
    share_sync?: { exclude_preset_rules?: string[]; exclude_regex_preset_rules?: string[] }
  }) => {
    config.presetRules = [...(cfg?.share_sync?.exclude_preset_rules ?? [])]
    config.regexPresetRules = [...(cfg?.share_sync?.exclude_regex_preset_rules ?? [])]
  })
  routerPush.mockClear()
  rules.capabilities.mockReset().mockResolvedValue({
    supports_regex: true,
    regex_limits: { max_rules: 64, max_rule_bytes: 1024, max_total_bytes: 32768, max_test_path_bytes: 4096 },
  })
  rules.validate.mockReset().mockResolvedValue({ valid: true, errors: [] })
})

afterEach(() => {
  document.body.innerHTML = ''
})

describe('ShareIncludeExcludeEditor 排除内容', () => {
  it('排除内容浏览可选文件与目录，保存的是精确路径而非 glob', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    browseButton('排除内容').click()
    await flushPromises()

    // 文件与目录都可勾选
    expect(checkboxOf(fileItem('说明.txt')), '排除内容模式下文件应可勾选').toBeTruthy()
    expect(checkboxOf(fileItem('剧集')), '排除内容模式下目录应可勾选').toBeTruthy()

    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()

    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:excludePaths')
    expect(emitted, '应发出排除内容变更').toBeTruthy()
    expect(emitted![0][0]).toEqual(['/说明.txt'])
    // 精确排除不带任何通配符，避免「前缀相似目录被误排除」
    expect((emitted![0][0] as string[])[0]).not.toContain('*')
    // 排除内容不影响同步内容
    expect(wrapper.emitted('update:includePaths')).toBeUndefined()
  })

  it('排除内容确认替换旧模型，不把已删除的排除项合并回来', async () => {
    const wrapper = mountEditor({ excludePaths: ['/广告'] })
    await flushPromises()

    browseButton('排除内容').click()
    await flushPromises()

    // 打开时带出已存的排除项
    expect(checkboxStateOf('广告').checked).toBe(true)

    checkboxOf(fileItem('广告'))!.click()
    await flushPromises()
    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()

    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:excludePaths')!
    expect(emitted[emitted.length - 1][0]).toEqual(['/说明.txt'])
  })

  it('同步内容浏览整体呈现为整个分享，取消勾选后才产出显式路径', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()

    // 初始为「整个分享」：全部勾选，确认后仍是整个分享
    expect(checkboxStateOf('剧集').checked).toBe(true)
    expect(checkboxStateOf('说明.txt').checked).toBe(true)
    expect(pickerHint()).toBe('整个分享')

    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()

    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:includePaths')
    expect(emitted).toBeTruthy()
    expect(emitted![0][0]).toEqual(['/剧集', '/广告'])
    expect(wrapper.emitted('update:excludePaths')).toBeUndefined()
  })
})

describe('ShareIncludeExcludeEditor 路径选择（P1 集成）', () => {
  it('重新打开已存同步内容：带出目录与文件，父目录为半选', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集/S01', '/剧集/S02'] })
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()

    expect(checkboxStateOf('剧集')).toEqual({ checked: false, indeterminate: true })
    expect(checkboxStateOf('说明.txt')).toEqual({ checked: false, indeterminate: false })
    wrapper.unmount()
  })

  it('新增一个文件保留全部既有选择', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集/S01', '/剧集/S02'] })
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()
    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()
    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:includePaths')!
    expect(emitted[emitted.length - 1][0]).toEqual(['/剧集/S01', '/剧集/S02', '/说明.txt'])
    wrapper.unmount()
  })

  it('取消勾选会真正移除该路径', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集', '/说明.txt'] })
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()
    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()
    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:includePaths')!
    expect(emitted[emitted.length - 1][0]).toEqual(['/剧集'])
    wrapper.unmount()
  })

  it('取消不写任何模型，重新打开确认仍是原值', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集'] })
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()
    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()
    cancelButton().click()
    await flushPromises()
    expect(wrapper.emitted('update:includePaths'), '取消不得写入模型').toBeUndefined()

    // 重新打开：回到已存内容，不保留上一次的临时改动
    browseButton('同步内容').click()
    await flushPromises()
    expect(checkboxStateOf('说明.txt').checked).toBe(false)
    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:includePaths')!
    expect(emitted[emitted.length - 1][0]).toEqual(['/剧集'])
    wrapper.unmount()
  })

  it('清空同步内容后不可确认，提示改选同步内容', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()
    headerCheckbox().click()
    await flushPromises()

    expect(pickerHint()).toBe('请选择要同步的内容')
    expect((confirmButton() as HTMLButtonElement).disabled, '不可确认时确定按钮应禁用').toBe(true)
    expect(wrapper.emitted('update:includePaths'), '未确认不得写入模型').toBeUndefined()
    wrapper.unmount()
  })

  it('排除内容里被选中目录的子项：展开后保留其余兄弟路径', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集'] })
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()
    fileItem('剧集').click()
    await flushPromises()

    // 子项属于被选中的目录：取消勾选后目录被替换成剩余兄弟
    checkboxOf(fileItem('01.mp4'))!.click()
    await flushPromises()
    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:includePaths')!
    expect(emitted[emitted.length - 1][0]).toEqual(['/剧集/02.mp4'])
    wrapper.unmount()
  })

  it('展开失败时保留原选择，不会退回整个分享', async () => {
    const many = Array.from({ length: 100 }, (_, i) => ({
      fs_id: 100 + i,
      is_dir: false,
      path: `/剧集/${String(i).padStart(3, '0')}.mp4`,
      size: 1,
      name: `${String(i).padStart(3, '0')}.mp4`,
    }))
    let dirCalls = 0
    transfer.previewShareDir.mockImplementation(async () => {
      dirCalls += 1
      // 导航只拿到第一页（目录尚未列全），展开时再取后续页失败
      if (dirCalls === 1) return { files: many }
      throw new Error('网络中断')
    })
    const wrapper = mountEditor({ includePaths: ['/剧集'] })
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()
    fileItem('剧集').click()
    await flushPromises()

    checkboxOf(fileItem('000.mp4'))!.click()
    await flushPromises()

    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:includePaths')!
    expect(emitted[emitted.length - 1][0]).toEqual(['/剧集'])
    wrapper.unmount()
  })

  it('排除内容同样支持文件与目录的精确选择', async () => {
    const wrapper = mountEditor({ excludePaths: ['/剧集/S01'] })
    await flushPromises()

    browseButton('排除内容').click()
    await flushPromises()

    expect(checkboxStateOf('剧集')).toEqual({ checked: false, indeterminate: true })
    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()
    confirmButton().click()
    await flushPromises()

    const emitted = wrapper.emitted('update:excludePaths')!
    expect(emitted[emitted.length - 1][0]).toEqual(['/剧集/S01', '/说明.txt'])
    expect(wrapper.emitted('update:includePaths'), '排除内容不得影响同步内容').toBeUndefined()
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 头部与折叠说明', () => {
  it('字段标题为「同步内容」，外层「同步范围」不在此组件内', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    expect(fieldLabelTexts()).toEqual(['同步内容', '包含规则', '排除内容', '排除规则'])
    wrapper.unmount()
  })

  it('未选同步内容时头部显示「整个分享」，且不渲染空的标签区块', async () => {
    const wrapper = mountEditor({ includePaths: [] })
    await flushPromises()

    const block = fieldBlock('同步内容')
    expect(block.querySelector('.field-state')?.textContent?.trim()).toBe('整个分享')
    expect(block.querySelector('.path-tags'), '空范围不应有标签区块').toBeNull()
    wrapper.unmount()
  })

  it('已选同步内容时不再显示「整个分享」，标签可见', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集'] })
    await flushPromises()

    const block = fieldBlock('同步内容')
    expect(block.querySelector('.field-state')).toBeNull()
    const tag = block.querySelector('.path-tags .el-tag')
    expect(tag?.textContent?.trim()).toBe('/剧集')
    wrapper.unmount()
  })

  it('展示时去掉合法合成命名空间，移除仍按原始路径发出', async () => {
    const wrapper = mountEditor({
      includePaths: ['/sharelink1-2/A/报告 .txt', '/apps/bypy/B'],
      excludePaths: ['/sharelink1-2/C/广告. '],
    })
    await flushPromises()

    const includeTags = Array.from(fieldBlock('同步内容').querySelectorAll('.path-tags .el-tag'))
    expect(includeTags.map((tag) => tag.textContent?.trim())).toEqual(['/A/报告 .txt', '/apps/bypy/B'])
    const excludeTag = fieldBlock('排除内容').querySelector('.path-tags .el-tag')
    expect(excludeTag?.textContent).toBe('/C/广告. ')

    fieldBlock('排除内容')
        .querySelector<HTMLElement>('.path-tags .el-tag .el-tag__close')!
        .click()
    await flushPromises()
    expect(wrapper.emitted('update:excludePaths')![0][0]).toEqual([])
    wrapper.unmount()
  })

  it('三处使用说明默认全部折叠（aria-expanded=false 且无说明文案）', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    for (const [name, label] of [
      ['同步内容使用说明', '同步内容'],
      ['包含规则使用说明', '包含规则'],
      ['排除内容使用说明', '排除内容'],
      ['排除规则使用说明', '排除规则'],
    ]) {
      const btn = helpButton(name)
      expect(btn.getAttribute('aria-expanded'), `${name} 默认应折叠`).toBe('false')
      expect(helpText(label), `${label} 默认不应展开说明`).toBeNull()
    }
    // 默认折叠时不应出现「使用说明」的正文段落
    expect(document.body.querySelector('.exclude-help')).toBeNull()
    wrapper.unmount()
  })

  it('三处说明各自独立开关，互不影响', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    helpButton('同步内容使用说明').click()
    await flushPromises()

    expect(helpButton('同步内容使用说明').getAttribute('aria-expanded')).toBe('true')
    expect(helpText('同步内容')).toBe(
        '不选则同步整个分享。选择文件夹会包含其全部内容及后续新增内容；选择文件则只同步该文件。'
        + '在已整体选中的文件夹（或整个分享）里取消个别项目，会把它改为逐项选择，之后新增的内容不再自动同步；'
        + '只想跳过个别内容时，建议保留整体选择，改用「排除内容」。',
    )
    // 另两处仍是折叠状态
    expect(helpButton('排除内容使用说明').getAttribute('aria-expanded')).toBe('false')
    expect(helpText('排除内容')).toBeNull()
    expect(helpButton('排除规则使用说明').getAttribute('aria-expanded')).toBe('false')

    // 再开排除内容：两处同时展开，互不关闭
    helpButton('排除内容使用说明').click()
    await flushPromises()
    expect(helpText('排除内容')).toBe('选择文件只排除该文件；选择目录则连同其中全部内容一起排除，均不参与同步。')
    expect(helpText('同步内容')).toBeTruthy()

    // 关掉同步内容：排除内容说明保持展开
    helpButton('同步内容使用说明').click()
    await flushPromises()
    expect(helpButton('同步内容使用说明').getAttribute('aria-expanded')).toBe('false')
    expect(helpText('同步内容')).toBeNull()
    expect(helpText('排除内容')).toBe('选择文件只排除该文件；选择目录则连同其中全部内容一起排除，均不参与同步。')
    wrapper.unmount()
  })

  it('使用说明箭头随展开状态切换，按钮文案不变且图标对辅助技术隐藏', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    const names = ['同步内容使用说明', '包含规则使用说明', '排除内容使用说明', '排除规则使用说明']
    const caretOf = (name: string) => helpButton(name).querySelector<HTMLElement>('.help-caret')!
    const closedCaret = new Map<string, string>()

    for (const name of names) {
      const caret = caretOf(name)
      expect(caret, `${name} 应有状态箭头`).toBeTruthy()
      expect(caret.getAttribute('aria-hidden'), `${name} 箭头应为装饰性`).toBe('true')
      expect(caret.querySelector('svg'), `${name} 应渲染箭头图标`).toBeTruthy()
      expect(helpButton(name).textContent?.trim(), `${name} 文案应保持不变`).toBe('使用说明')
      closedCaret.set(name, caret.innerHTML)
    }

    for (const name of names) {
      helpButton(name).click()
      await flushPromises()
      expect(helpButton(name).getAttribute('aria-expanded')).toBe('true')
      expect(caretOf(name).innerHTML, `${name} 展开后箭头应切换`).not.toBe(closedCaret.get(name))
    }
    wrapper.unmount()
  })

  it('展开排除规则说明复用既有说明组件，并可再次折叠', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    helpButton('排除规则使用说明').click()
    await flushPromises()
    expect(document.body.querySelector('.exclude-help'), '应显示既有排除规则说明').toBeTruthy()

    helpButton('排除规则使用说明').click()
    await flushPromises()
    expect(document.body.querySelector('.exclude-help')).toBeNull()
    wrapper.unmount()
  })

  it('关闭已选标签会按索引发出新的路径列表', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集', '/广告'], excludePaths: ['/缓存'] })
    await flushPromises()

    const includeClose = fieldBlock('同步内容')
        .querySelector<HTMLElement>('.path-tags .el-tag .el-tag__close')
    expect(includeClose, '同步内容标签应可关闭').toBeTruthy()
    includeClose!.click()
    await flushPromises()

    expect(wrapper.emitted('update:includePaths')![0][0]).toEqual(['/广告'])

    const excludeClose = fieldBlock('排除内容')
        .querySelector<HTMLElement>('.path-tags .el-tag .el-tag__close')
    expect(excludeClose, '排除内容标签应可关闭').toBeTruthy()
    excludeClose!.click()
    await flushPromises()

    expect(wrapper.emitted('update:excludePaths')![0][0]).toEqual([])
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 排除规则与常用词表', () => {
  it('每行一条添加规则，输入法组词时不提交', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    const textarea = fieldBlock('排除规则').querySelector<HTMLTextAreaElement>('textarea')!
    expect(textarea, '应有规则输入框').toBeTruthy()

    // 组词中的回车（选字确认）不提交
    textarea.value = '*.tmp'
    textarea.dispatchEvent(new Event('input'))
    await flushPromises()
    textarea.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', isComposing: true, bubbles: true }))
    await flushPromises()
    expect(wrapper.emitted('update:excludePatterns')).toBeUndefined()

    // 正常回车提交，按行拆分
    textarea.value = '*.tmp\n*.bak'
    textarea.dispatchEvent(new Event('input'))
    await flushPromises()
    textarea.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true }))
    await flushPromises()

    const emitted = wrapper.emitted('update:excludePatterns')!
    expect(emitted[emitted.length - 1][0]).toEqual(['*.tmp', '*.bak'])
    wrapper.unmount()
  })

  it('规则输入框占位符为「输入规则，每行一条」，与「添加」按钮同一行', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    const row = fieldBlock('排除规则').querySelector<HTMLElement>('.exclude-input-row')!
    const textarea = row.querySelector<HTMLTextAreaElement>('textarea')
    expect(textarea?.getAttribute('placeholder'), '占位符应为简短提示').toBe('输入规则，每行一条')
    expect(row.querySelector('.exclude-input')?.contains(textarea!), '输入框应在输入行内').toBe(true)

    const addBtn = Array.from(row.querySelectorAll('button')).find((b) => b.textContent?.trim() === '添加')
    expect(addBtn, '输入行内应有「添加」按钮').toBeTruthy()
    expect(fieldBlock('排除规则').querySelector('.field-label')?.textContent?.trim()).toBe('排除规则')
    wrapper.unmount()
  })

  it('点击常用词表 chip 添加规则，保存按钮在无规则时禁用', async () => {
    config.presetRules = ['*.nfo']
    const wrapper = mountEditor()
    await flushPromises()

    // el-tag 外层是过渡组件，实际可点区域是里面的 el-tag 元素
    const chip = fieldBlock('排除规则').querySelector<HTMLElement>('.preset-tag .el-tag')
    expect(chip?.textContent?.trim()).toBe('*.nfo')

    const saveBtn = Array.from(fieldBlock('排除规则').querySelectorAll<HTMLButtonElement>('button'))
        .find((b) => b.textContent?.trim() === '存为常用')
    expect(saveBtn, '应有「存为常用」按钮').toBeTruthy()
    expect(saveBtn!.disabled, '无规则时「存为常用」应禁用').toBe(true)

    chip!.click()
    await flushPromises()
    expect(wrapper.emitted('update:excludePatterns')![0][0]).toEqual(['*.nfo'])
    wrapper.unmount()
  })

  it('有规则时「存为常用」可点击并写回配置，「管理常用规则」跳转设置页', async () => {
    config.presetRules = []
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()

    const buttons = Array.from(fieldBlock('排除规则').querySelectorAll<HTMLButtonElement>('button'))
    const saveBtn = buttons.find((b) => b.textContent?.trim() === '存为常用')
    expect(saveBtn && !saveBtn.disabled, '有规则时「存为常用」应可用').toBe(true)

    await saveBtn!.click()
    await flushPromises()
    expect(config.updateConfig).toHaveBeenCalledTimes(1)
    const saved = config.updateConfig.mock.calls[0][0] as { share_sync: { exclude_preset_rules: string[] } }
    expect(saved.share_sync.exclude_preset_rules).toEqual(['*.tmp'])

    const manageBtn = Array.from(fieldBlock('排除规则').querySelectorAll<HTMLButtonElement>('button'))
        .find((b) => b.textContent?.trim() === '管理常用规则')
    expect(manageBtn, '应有「管理常用规则」入口').toBeTruthy()
    manageBtn!.click()
    await flushPromises()
    expect(routerPush).toHaveBeenCalledWith({ path: '/settings', query: { tab: 'sharesync' } })
    wrapper.unmount()
  })

  it('词表为空时不再渲染空态长文案，仅保留两个操作按钮', async () => {
    config.presetRules = []
    const wrapper = mountEditor()
    await flushPromises()

    const presetRow = fieldBlock('排除规则').querySelector<HTMLElement>('.preset-row')!
    expect(presetRow.querySelectorAll('.preset-tag')).toHaveLength(0)
    const labels = Array.from(presetRow.querySelectorAll('button')).map((b) => b.textContent?.trim())
    expect(labels).toEqual(['存为常用', '管理常用规则'])
    // 旧的长空态文案已移除
    expect(presetRow.textContent).not.toContain('常用规则词表为空')
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 账号未登录', () => {
  it('保留未登录提示并禁用浏览入口', async () => {
    const wrapper = mountEditor({ ownerLoggedIn: false })
    await flushPromises()

    expect(fieldBlock('同步内容').textContent).toContain('订阅所属账号未登录，目录树预览不可用')
    const browse = browseButton('同步内容') as HTMLButtonElement
    expect(browse.disabled, '账号未登录时浏览按钮应禁用').toBe(true)
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 正则规则', () => {
  const rulesBlock = () => fieldBlock('排除规则')
  const ruleInput = () =>
      rulesBlock().querySelector<HTMLTextAreaElement>('.exclude-input textarea')!
  const addButton = () =>
      Array.from(rulesBlock().querySelectorAll<HTMLButtonElement>('.exclude-input-row button'))
          .find((b) => b.textContent?.trim() === '添加')!
  const kindSelect = () => rulesBlock().querySelector<HTMLElement>('.rule-kind-select')!
  const kindOptions = () => visibleSelectDropdownItems()
  const regexTags = () =>
      Array.from(rulesBlock().querySelectorAll<HTMLElement>('.path-tags .el-tag'))
          .filter((el) => el.textContent?.includes('正则'))

  /** 打开真实下拉框并选择新增规则类型 */
  async function selectKind(kind: 'glob' | 'regex') {
    kindSelect().querySelector<HTMLElement>('.el-select__wrapper')!.click()
    await flushPromises()
    const label = kind === 'regex' ? '正则表达式' : '通配符'
    const option = kindOptions().find((o) => o.textContent?.trim() === label)
    expect(option, `下拉框应有「${label}」选项`).toBeTruthy()
    option!.click()
    await flushPromises()
  }

  it('规则类型是输入行内的真实下拉框，默认通配符且无分段单选', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    expect(rulesBlock().querySelector('.rule-kind-row'), '不应再有独立的类型行').toBeNull()
    expect(rulesBlock().querySelector('.el-radio-group, .el-radio-button'), '不应再有分段单选').toBeNull()
    const select = kindSelect()
    expect(select.querySelector('[role="combobox"]'), '规则类型必须是 combobox').toBeTruthy()
    expect(select.textContent).toContain('通配符')
    expect(
        select.compareDocumentPosition(ruleInput()),
        '下拉框必须在规则输入框之前',
    ).toBe(Node.DOCUMENT_POSITION_FOLLOWING)
    expect(ruleInput().getAttribute('placeholder')).toBe('输入规则，每行一条')

    // 下拉选项：两项齐全，能力可用时正则可选
    select.querySelector<HTMLElement>('.el-select__wrapper')!.click()
    await flushPromises()
    expect(kindOptions().map((o) => o.textContent?.trim())).toEqual(['通配符', '正则表达式'])
    expect(kindOptions()[1].classList.contains('is-disabled'), '能力可用时正则可选').toBe(false)
    wrapper.unmount()
  })

  it('后端不支持时正则选项禁用，既有正则标签仍可见可删', async () => {
    rules.capabilities.mockRejectedValue(Object.assign(new Error('request failed'), {
      response: { status: 404 },
    }))
    const wrapper = mountEditor({ excludeRegexPatterns: ['^/股票/.*$'] })
    await flushPromises()

    kindSelect().querySelector<HTMLElement>('.el-select__wrapper')!.click()
    await flushPromises()
    expect(kindOptions()[1].classList.contains('is-disabled'), '404 时应禁用正则选项').toBe(true)
    expect(kindSelect().textContent, '未确认能力时保持通配符').toContain('通配符')
    document.body.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }))
    await flushPromises()
    expect(rulesBlock().textContent).toContain('当前后端仅支持通配符')
    expect(regexTags()).toHaveLength(1)

    // 仍可删除已有正则（不依赖能力探测结果）
    const close = regexTags()[0].querySelector<HTMLElement>('.el-tag__close')!
    close.click()
    await flushPromises()
    expect(wrapper.emitted('update:excludeRegexPatterns')![0][0]).toEqual([])
    wrapper.unmount()
  })

  it('类型选择只影响之后新增的规则，不迁移或改写已有标签', async () => {
    const wrapper = mountEditor({
      excludePatterns: ['*.tmp'],
      excludeRegexPatterns: ['^/a$'],
    })
    await flushPromises()

    await selectKind('regex')
    // 两种既有标签都保留在各自模型里，切换类型不会搬动它们
    expect(rulesBlock().querySelectorAll('.path-tags .el-tag')).toHaveLength(2)

    await selectKind('glob')
    ruleInput().value = '*.bak'
    ruleInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    expect(wrapper.emitted('update:excludePatterns')![0][0]).toEqual(['*.tmp', '*.bak'])
    expect(wrapper.emitted('update:excludeRegexPatterns'), '切换类型不得改写既有正则').toBeUndefined()
    wrapper.unmount()
  })

  it('切换类型后输入框提示与可访问名随之变化', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    await selectKind('regex')

    expect(ruleInput().getAttribute('placeholder')).toBe('输入正则，每行一条')
    expect(ruleInput().getAttribute('aria-label')).toBe('输入正则排除规则')
    wrapper.unmount()
  })

  it('新增正则先整批校验，通过后仅发出 regex 模型', async () => {
    const wrapper = mountEditor()
    await flushPromises()
    await selectKind('regex')

    ruleInput().value = '^/股票/按月归档/2025.*$'
    ruleInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    expect(rules.validate).toHaveBeenCalledTimes(1)
    const req = rules.validate.mock.calls[0][0] as {
      exclude_patterns: string[]
      exclude_regex_patterns: string[]
    }
    expect(req.exclude_regex_patterns).toEqual(['^/股票/按月归档/2025.*$'])
    expect(req.exclude_patterns).toEqual([])

    expect(wrapper.emitted('update:excludeRegexPatterns')![0][0])
        .toEqual(['^/股票/按月归档/2025.*$'])
    expect(wrapper.emitted('update:excludePatterns'), '正则不得写入通配符模型').toBeUndefined()
    expect(ruleInput().value, '成功后清空输入').toBe('')
    wrapper.unmount()
  })

  it('校验失败时不发出任何规则，并显示带行号的错误', async () => {
    rules.validate.mockResolvedValue({
      valid: false,
      errors: [{ kind: 'regex', index: 1, message: '无法编译正则：unclosed group' }],
    })
    const wrapper = mountEditor()
    await flushPromises()
    await selectKind('regex')

    ruleInput().value = '^/ok$'
    ruleInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    expect(wrapper.emitted('update:excludeRegexPatterns')).toBeUndefined()
    const error = rulesBlock().querySelector<HTMLElement>('.rule-error')
    expect(error?.textContent).toContain('正则本次新增第 2 条')
    expect(error?.textContent).toContain('无法编译正则')
    expect(ruleInput().value, '失败时保留输入').toBe('^/ok$')
    wrapper.unmount()
  })

  it('校验期间输入已变化则丢弃过期结果', async () => {
    let release: (v: unknown) => void = () => {}
    rules.validate.mockReturnValue(new Promise((resolve) => {
      release = resolve
    }))
    const wrapper = mountEditor()
    await flushPromises()
    await selectKind('regex')

    ruleInput().value = '^/旧$'
    ruleInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    // 校验返回前用户改了输入
    ruleInput().value = '^/新$'
    ruleInput().dispatchEvent(new Event('input'))
    await flushPromises()

    release({ valid: true, errors: [] })
    await flushPromises()

    expect(wrapper.emitted('update:excludeRegexPatterns'), '过期批次不得写入').toBeUndefined()
    expect(ruleInput().value, '不得清掉更新的输入').toBe('^/新$')
    wrapper.unmount()
  })

  it('校验端点不可用时拒绝新增正则并给出可读原因', async () => {
    rules.validate.mockRejectedValue(Object.assign(new Error('request failed'), {
      response: { status: 404, data: { message: 'Not Found' } },
    }))
    const wrapper = mountEditor()
    await flushPromises()
    await selectKind('regex')

    ruleInput().value = '^/a$'
    ruleInput().dispatchEvent(new Event('input'))
    await flushPromises()
    addButton().click()
    await flushPromises()

    expect(wrapper.emitted('update:excludeRegexPatterns')).toBeUndefined()
    expect(rulesBlock().querySelector('.rule-error')?.textContent)
        .toContain('无法校验排除规则')
    wrapper.unmount()
  })

  it('validateRules() 无正则时直接通过，有正则时交由后端判定', async () => {
    const clean = mountEditor()
    await flushPromises()
    const cleanResult = await (clean.vm as unknown as {
      validateRules: () => Promise<{ ok: boolean }>
    }).validateRules()
    expect(cleanResult.ok).toBe(true)
    expect(rules.validate, '无正则不请求后端').not.toHaveBeenCalled()
    clean.unmount()

    rules.validate.mockResolvedValue({
      valid: false,
      errors: [{ kind: 'regex', index: 0, message: '不能为空' }],
    })
    const withRegex = mountEditor({ excludeRegexPatterns: ['  '] })
    await flushPromises()
    const result = await (withRegex.vm as unknown as {
      validateRules: () => Promise<{ ok: boolean; message?: string }>
    }).validateRules()
    expect(result.ok).toBe(false)
    expect(result.message).toContain('第 1 条')
    expect(rules.validate).toHaveBeenCalledTimes(1)
    withRegex.unmount()
  })

  it('测试已添加规则时用两份列表试匹配并展示结果', async () => {
    rules.validate.mockResolvedValue({ valid: true, errors: [], excluded: true })
    const wrapper = mountEditor({
      excludePatterns: ['*.tmp'],
      excludeRegexPatterns: ['^/股票/按月归档/2025.*$'],
    })
    await flushPromises()

    helpButton('测试已添加规则').click()
    await flushPromises()
    expect(
        rulesBlock().textContent,
        '冗长的测试范围说明句已移除',
    ).not.toContain('测试已添加规则（不含同步范围与精确排除内容）')
    expect(rulesBlock().querySelector('.rule-test-title'), '不应保留标题节点').toBeNull()
    expect(rulesBlock().textContent).toContain('相对分享根目录')

    const pathInput = rulesBlock().querySelector<HTMLInputElement>('.rule-test-input input')!
    pathInput.value = '/股票/按月归档/2025-01/x.mp4'
    pathInput.dispatchEvent(new Event('input'))
    await flushPromises()

    const runBtn = rulesBlock()
        .querySelector<HTMLButtonElement>('.rule-test-row button[aria-label="测试已添加规则"]')!
    runBtn.click()
    await flushPromises()

    const req = rules.validate.mock.calls[0][0] as {
      exclude_patterns: string[]
      exclude_regex_patterns: string[]
      test_path?: string
    }
    expect(req.exclude_patterns).toEqual(['*.tmp'])
    expect(req.exclude_regex_patterns).toEqual(['^/股票/按月归档/2025.*$'])
    expect(req.test_path).toBe('/股票/按月归档/2025-01/x.mp4')
    expect(rulesBlock().querySelector('.rule-test-result')?.textContent).toContain('会排除该路径')
    wrapper.unmount()
  })

  it('试匹配失败时清除旧结果并显示错误', async () => {
    rules.validate.mockResolvedValueOnce({ valid: true, errors: [], excluded: false })
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()

    helpButton('测试已添加规则').click()
    await flushPromises()
    const runBtn = rulesBlock()
        .querySelector<HTMLButtonElement>('.rule-test-row button[aria-label="测试已添加规则"]')!
    runBtn.click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result')?.textContent).toContain('不排除该路径')

    rules.validate.mockResolvedValueOnce({
      valid: false,
      errors: [{ kind: 'path', message: '试匹配路径最多 4096 字节' }],
    })
    runBtn.click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result'), '失败不得保留旧结果').toBeNull()
    expect(rulesBlock().querySelector('.rule-error')?.textContent).toContain('4096')
    wrapper.unmount()
  })

  it('正则词表只添加到正则类型，且标记为常用 chip', async () => {
    config.regexPresetRules = ['^/基金/.*$']
    const wrapper = mountEditor()
    await flushPromises()
    await selectKind('regex')

    const chip = rulesBlock().querySelector<HTMLElement>('.preset-tag .el-tag')!
    expect(chip.textContent?.trim()).toBe('^/基金/.*$')
    chip.click()
    await flushPromises()

    expect(wrapper.emitted('update:excludeRegexPatterns')![0][0]).toEqual(['^/基金/.*$'])
    expect(wrapper.emitted('update:excludePatterns')).toBeUndefined()
    wrapper.unmount()
  })

  it('存为常用只写当前类型的词表字段', async () => {
    config.regexPresetRules = []
    const wrapper = mountEditor({ excludeRegexPatterns: ['^/股票/.*$'] })
    await flushPromises()
    await selectKind('regex')

    const saveBtn = Array.from(rulesBlock().querySelectorAll<HTMLButtonElement>('.preset-row button'))
        .find((b) => b.textContent?.trim() === '存为常用')!
    saveBtn.click()
    await flushPromises()

    expect(config.updateConfig).toHaveBeenCalledTimes(1)
    const saved = config.updateConfig.mock.calls[0][0] as {
      share_sync: { exclude_preset_rules: string[]; exclude_regex_preset_rules: string[] }
    }
    expect(saved.share_sync.exclude_regex_preset_rules).toEqual(['^/股票/.*$'])
    expect(saved.share_sync.exclude_preset_rules, '正则不得污染 glob 词表').toEqual([])
    wrapper.unmount()
  })

  it('既有通配符标签保持原有样式与删除行为', async () => {
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()

    const tags = rulesBlock().querySelectorAll<HTMLElement>('.path-tags .el-tag')
    expect(tags).toHaveLength(1)
    expect(tags[0].textContent?.trim()).toBe('*.tmp')
    expect(tags[0].textContent, '通配符标签不加正则标记').not.toContain('正则')
    tags[0].querySelector<HTMLElement>('.el-tag__close')!.click()
    await flushPromises()
    expect(wrapper.emitted('update:excludePatterns')![0][0]).toEqual([])
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 试匹配结果快照（F1）', () => {
  const rulesBlock = () => fieldBlock('排除规则')
  const openTest = async () => {
    helpButton('测试已添加规则').click()
    await flushPromises()
    return {
      pathInput: rulesBlock().querySelector<HTMLInputElement>('.rule-test-input input')!,
      run: () =>
        rulesBlock()
            .querySelector<HTMLButtonElement>('.rule-test-row button[aria-label="测试已添加规则"]')!,
    }
  }
  const setPath = async (input: HTMLInputElement, value: string) => {
    input.value = value
    input.dispatchEvent(new Event('input'))
    await flushPromises()
  }

  it('已完成结果在路径改变后立即消失，改回也不复活旧结果', async () => {
    rules.validate.mockResolvedValue({ valid: true, errors: [], excluded: true })
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()
    const { pathInput, run } = await openTest()

    await setPath(pathInput, '/股票/按月归档/2025-01/a.txt')
    run().click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result')?.textContent).toContain('会排除该路径')

    await setPath(pathInput, '/股票/按月归档/2026-01/a.txt')
    expect(rulesBlock().querySelector('.rule-test-result'), '路径变化后结果必须失效').toBeNull()

    await setPath(pathInput, '/股票/按月归档/2025-01/a.txt')
    expect(rulesBlock().querySelector('.rule-test-result'), '改回旧路径也不能恢复旧结果').toBeNull()
    wrapper.unmount()
  })

  it('已添加规则列表变化后结果失效', async () => {
    rules.validate.mockResolvedValue({ valid: true, errors: [], excluded: true })
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()
    const { pathInput, run } = await openTest()

    await setPath(pathInput, '/股票/a.tmp')
    run().click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result')).not.toBeNull()

    await wrapper.setProps({ excludePatterns: ['*.tmp', '*.nfo'] })
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result'), '规则变化后结果必须失效').toBeNull()
    wrapper.unmount()
  })

  it('在飞请求期间路径变化时，返回的旧结果与新错误都被丢弃', async () => {
    let release: (v: unknown) => void = () => {}
    rules.validate.mockReturnValue(new Promise((resolve) => {
      release = resolve
    }))
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()
    const { pathInput, run } = await openTest()

    await setPath(pathInput, '/旧/路径')
    run().click()
    await flushPromises()
    await setPath(pathInput, '/新/路径')

    release({ valid: true, errors: [], excluded: true })
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result'), '过期结果不得显示').toBeNull()

    // 旧请求以错误返回时同样不得挂在新输入上
    let fail: (e: unknown) => void = () => {}
    rules.validate.mockReturnValue(new Promise((_, reject) => {
      fail = reject
    }))
    run().click()
    await flushPromises()
    await setPath(pathInput, '/再改/路径')
    fail(Object.assign(new Error('boom'), { response: { data: { message: 'backend down' } } }))
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-error'), '过期错误不得显示').toBeNull()
    wrapper.unmount()
  })

  it('新一次测试开始时先清掉上一条结果', async () => {
    rules.validate.mockResolvedValueOnce({ valid: true, errors: [], excluded: true })
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()
    const { pathInput, run } = await openTest()

    await setPath(pathInput, '/股票/a.tmp')
    run().click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result')).not.toBeNull()

    let release: (v: unknown) => void = () => {}
    rules.validate.mockReturnValue(new Promise((resolve) => {
      release = resolve
    }))
    run().click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result'), '新测试进行中不得保留旧结果').toBeNull()

    release({ valid: true, errors: [], excluded: false })
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test-result')?.textContent).toContain('不排除该路径')
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 词表点选保留草稿（F3）', () => {
  const rulesBlock = () => fieldBlock('排除规则')
  const textarea = () => rulesBlock().querySelector<HTMLTextAreaElement>('.exclude-input textarea')!
  const switchToRegex = async () => {
    rulesBlock().querySelector<HTMLElement>('.rule-kind-select .el-select__wrapper')!.click()
    await flushPromises()
    const option = visibleSelectDropdownItems()
        .find((o) => o.textContent?.trim() === '正则表达式')
    expect(option, '下拉框应有「正则表达式」选项').toBeTruthy()
    option!.click()
    await flushPromises()
  }
  const typeDraft = async (text: string) => {
    textarea().value = text
    textarea().dispatchEvent(new Event('input'))
    await flushPromises()
  }

  it('点击通配符词表不清空未提交的草稿', async () => {
    config.presetRules = ['*.nfo']
    const wrapper = mountEditor()
    await flushPromises()
    await typeDraft('*尚未提交的规则*')

    rulesBlock().querySelector<HTMLElement>('.preset-tag .el-tag')!.click()
    await flushPromises()

    expect(wrapper.emitted('update:excludePatterns')![0][0]).toEqual(['*.nfo'])
    expect(textarea().value, '旁路点选必须保留草稿').toBe('*尚未提交的规则*')
    wrapper.unmount()
  })

  it('点击正则词表同样保留草稿，且只写正则模型', async () => {
    config.regexPresetRules = ['^/股票/.*$']
    const wrapper = mountEditor()
    await flushPromises()
    await switchToRegex()
    await typeDraft('^/草稿/.*$')

    rulesBlock().querySelector<HTMLElement>('.preset-tag .el-tag')!.click()
    await flushPromises()

    expect(wrapper.emitted('update:excludeRegexPatterns')![0][0]).toEqual(['^/股票/.*$'])
    expect(wrapper.emitted('update:excludePatterns')).toBeUndefined()
    expect(textarea().value).toBe('^/草稿/.*$')
    wrapper.unmount()
  })

  it('textarea 提交后仍清空自己提交的那段文本', async () => {
    const wrapper = mountEditor()
    await flushPromises()
    await typeDraft('*.tmp')

    rulesBlock()
        .querySelectorAll<HTMLButtonElement>('.exclude-input-row button')[0]
        .click()
    await flushPromises()

    expect(wrapper.emitted('update:excludePatterns')![0][0]).toEqual(['*.tmp'])
    expect(textarea().value).toBe('')
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 规则试匹配面板', () => {
  const rulesBlock = () => fieldBlock('排除规则')
  /** 「测试规则」开关（头部操作区内的那个，不是面板里的「测试」按钮） */
  const testToggle = () =>
      rulesBlock().querySelector<HTMLButtonElement>('.path-actions button[aria-label="测试已添加规则"]')!

  it('测试规则开关带状态箭头，按钮文案不变且图标对辅助技术隐藏', async () => {
    const wrapper = mountEditor()
    await flushPromises()

    const btn = testToggle()
    expect(btn.tagName, '开关必须是原生按钮，键盘可激活').toBe('BUTTON')
    expect(btn.disabled, '开关不应禁用').toBe(false)
    expect(btn.textContent?.trim(), '按钮文案保持不变').toBe('测试规则')
    expect(btn.getAttribute('aria-expanded'), '默认折叠').toBe('false')

    const caret = btn.querySelector<HTMLElement>('.help-caret')!
    expect(caret, '应有状态箭头').toBeTruthy()
    expect(caret.getAttribute('aria-hidden'), '箭头应为装饰性').toBe('true')
    expect(caret.querySelector('svg'), '应渲染箭头图标').toBeTruthy()
    const closedCaret = caret.innerHTML

    btn.click()
    await flushPromises()

    expect(testToggle().getAttribute('aria-expanded')).toBe('true')
    expect(
        testToggle().querySelector<HTMLElement>('.help-caret')!.innerHTML,
        '展开后箭头应切换',
    ).not.toBe(closedCaret)
    expect(rulesBlock().querySelector('.rule-test'), '展开后显示试匹配面板').toBeTruthy()
    wrapper.unmount()
  })

  it('折叠后再展开面板，展开态与面板可见性同步', async () => {
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()

    testToggle().click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test')).toBeTruthy()

    testToggle().click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test'), '折叠后隐藏面板').toBeNull()
    expect(testToggle().getAttribute('aria-expanded')).toBe('false')

    testToggle().click()
    await flushPromises()
    expect(rulesBlock().querySelector('.rule-test'), '再次展开显示面板').toBeTruthy()
    expect(testToggle().getAttribute('aria-expanded')).toBe('true')
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 常用规则保存回读', () => {
  const rulesBlock = () => fieldBlock('排除规则')
  const saveButton = () =>
      Array.from(rulesBlock().querySelectorAll<HTMLButtonElement>('.preset-row button'))
          .find((b) => b.textContent?.trim() === '存为常用')!
  const chips = () => Array.from(rulesBlock().querySelectorAll<HTMLElement>('.preset-tag .el-tag'))
  const textarea = () => rulesBlock().querySelector<HTMLTextAreaElement>('.exclude-input textarea')!
  const savedConfig = (index: number) =>
      config.updateConfig.mock.calls[index][0] as {
        share_sync: { exclude_preset_rules: string[]; exclude_regex_preset_rules: string[] }
        ui: { remember_netdisk_path: boolean; account_quick_bar: boolean }
        download: { max_global_threads: number }
      }

  /** 打开真实下拉框并选择新增规则类型 */
  async function selectKind(kind: 'glob' | 'regex') {
    rulesBlock().querySelector<HTMLElement>('.rule-kind-select .el-select__wrapper')!.click()
    await flushPromises()
    const label = kind === 'regex' ? '正则表达式' : '通配符'
    const option = visibleSelectDropdownItems()
        .find((o) => o.textContent?.trim() === label)
    expect(option, `下拉框应有「${label}」选项`).toBeTruthy()
    option!.click()
    await flushPromises()
  }

  it('保存通配符词表后重载可点选应用，且保留正则词表与无关配置', async () => {
    config.regexPresetRules = ['^/基金/.*$']
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()

    await saveButton().click()
    await flushPromises()

    expect(config.updateConfig).toHaveBeenCalledTimes(1)
    const saved = savedConfig(0)
    expect(saved.share_sync.exclude_preset_rules).toEqual(['*.tmp'])
    expect(saved.share_sync.exclude_regex_preset_rules, '保存通配符不得改动正则词表')
        .toEqual(['^/基金/.*$'])
    expect(saved.ui, '无关配置必须原样保留')
        .toEqual({ remember_netdisk_path: true, account_quick_bar: false })
    expect(saved.download, '无关配置必须原样保留').toEqual({ max_global_threads: 8 })

    wrapper.unmount()
    document.body.innerHTML = ''

    // 重载：词表从持久化配置读回
    const reloaded = mountEditor()
    await flushPromises()
    expect(chips().map((c) => c.textContent?.trim()), '重载后应读到已保存的词表').toEqual(['*.tmp'])

    // 未提交的草稿不因点选而丢失
    textarea().value = '*尚未提交的草稿*'
    textarea().dispatchEvent(new Event('input'))
    await flushPromises()

    chips()[0].click()
    await flushPromises()

    expect(reloaded.emitted('update:excludePatterns')![0][0]).toEqual(['*.tmp'])
    expect(textarea().value, '点选不得清空未提交的草稿').toBe('*尚未提交的草稿*')
    reloaded.unmount()
  })

  it('保存正则词表保留通配符词表且重复保存去重，重载后按正则类型点选', async () => {
    config.presetRules = ['*.nfo']
    const wrapper = mountEditor({ excludeRegexPatterns: ['^/股票/.*$'] })
    await flushPromises()
    await selectKind('regex')

    await saveButton().click()
    await flushPromises()

    const saved = savedConfig(0)
    expect(saved.share_sync.exclude_regex_preset_rules).toEqual(['^/股票/.*$'])
    expect(saved.share_sync.exclude_preset_rules, '保存正则不得清空通配符词表').toEqual(['*.nfo'])

    wrapper.unmount()
    document.body.innerHTML = ''

    // 重载后重复保存同一条：不得产生重复词条
    const again = mountEditor({ excludeRegexPatterns: ['^/股票/.*$'] })
    await flushPromises()
    await selectKind('regex')
    await saveButton().click()
    await flushPromises()
    expect(savedConfig(1).share_sync.exclude_regex_preset_rules, '重复保存不得产生重复词条')
        .toEqual(['^/股票/.*$'])

    again.unmount()
    document.body.innerHTML = ''

    const reloaded = mountEditor()
    await flushPromises()
    await selectKind('regex')
    expect(chips().map((c) => c.textContent?.trim())).toEqual(['^/股票/.*$'])

    chips()[0].click()
    await flushPromises()
    expect(reloaded.emitted('update:excludeRegexPatterns')![0][0]).toEqual(['^/股票/.*$'])
    expect(reloaded.emitted('update:excludePatterns'), '正则词表不得写入通配符模型').toBeUndefined()
    reloaded.unmount()
  })

  it('后端拒绝保存时不提示成功、不改持久化状态，且可重试', async () => {
    const success = vi.spyOn(ElMessage, 'success').mockImplementation(() => undefined as never)
    const error = vi.spyOn(ElMessage, 'error').mockImplementation(() => undefined as never)

    for (const reason of ['分享同步排除规则校验失败: 第 1 条正则无法编译', '服务端不支持该配置']) {
      const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
      await flushPromises()

      config.updateConfig.mockRejectedValueOnce(new Error(reason))
      await saveButton().click()
      await flushPromises()

      expect(success, `失败（${reason}）不得提示成功`).not.toHaveBeenCalled()
      expect(error, `失败（${reason}）必须给出错误提示`).toHaveBeenCalled()
      expect(error.mock.calls[0][0]).toContain('保存常用规则失败')
      expect(config.presetRules, `失败（${reason}）不得改动持久化词表`).toEqual([])
      expect(saveButton().disabled, '失败后按钮应恢复可用').toBe(false)

      wrapper.unmount()
      document.body.innerHTML = ''
    }

    // 恢复后可重试成功
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()
    await saveButton().click()
    await flushPromises()
    expect(config.updateConfig).toHaveBeenCalledTimes(3)
    expect(config.presetRules).toEqual(['*.tmp'])
    expect(success, '重试成功才提示成功').toHaveBeenCalledTimes(1)

    success.mockRestore()
    error.mockRestore()
    wrapper.unmount()
  })

  it('网络失败时不提示成功、不改持久化状态', async () => {
    const success = vi.spyOn(ElMessage, 'success').mockImplementation(() => undefined as never)
    const error = vi.spyOn(ElMessage, 'error').mockImplementation(() => undefined as never)
    const wrapper = mountEditor({ excludePatterns: ['*.tmp'] })
    await flushPromises()

    config.updateConfig.mockRejectedValueOnce(new Error('Network Error'))
    await saveButton().click()
    await flushPromises()

    expect(success, '网络失败不得提示成功').not.toHaveBeenCalled()
    expect(error.mock.calls[0][0]).toContain('Network Error')
    expect(config.presetRules, '网络失败不得改动持久化词表').toEqual([])
    expect(saveButton().disabled, '失败后按钮应恢复可用').toBe(false)

    success.mockRestore()
    error.mockRestore()
    wrapper.unmount()
  })

  it('合法但当前无样本匹配的正则仍可存为常用（不要求命中）', async () => {
    const wrapper = mountEditor({ excludeRegexPatterns: ['^/当前没有样本的目录/.*$'] })
    await flushPromises()
    await selectKind('regex')

    await saveButton().click()
    await flushPromises()

    expect(config.updateConfig).toHaveBeenCalledTimes(1)
    expect(savedConfig(0).share_sync.exclude_regex_preset_rules)
        .toEqual(['^/当前没有样本的目录/.*$'])
    expect(rules.validate, '保存词表不做试匹配，不要求命中样本').not.toHaveBeenCalled()
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 选择就绪与导航竞态', () => {
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
  }

  function deferred<T>(): Deferred<T> {
    let resolve!: (value: T) => void
    const promise = new Promise<T>((res) => {
      resolve = res
    })
    return { promise, resolve }
  }

  function rootBreadcrumb(): HTMLElement {
    const btn = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '根目录')
    expect(btn, '选择器应有「根目录」面包屑').toBeTruthy()
    return btn!
  }

  it('确定按钮跟随真实就绪：事务中禁用，导航取消后按旧选择恢复', async () => {
    const pending = deferred<{ files: typeof DIR_FILES }>()
    let dirCalls = 0
    transfer.previewShareDir.mockImplementation(async () => {
      dirCalls++
      if (dirCalls === 1) return { files: DIR_FILES }
      return pending.promise
    })
    const wrapper = mountEditor({ includePaths: ['/剧集'] })
    await flushPromises()

    browseButton('同步内容').click()
    await flushPromises()
    fileItem('剧集').click()
    await flushPromises()

    // 取消被目录覆盖的子项：需要展开全部分页，事务进行中就绪必须为 false
    checkboxOf(fileItem('000.mp4'))!.click()
    await flushPromises()
    expect(document.body.textContent).toContain('正在展开目录')
    expect((confirmButton() as HTMLButtonElement).disabled, '事务中不得确认').toBe(true)

    // 事务未结束时导航：原子取消，旧选择不变，确定按钮按真实就绪恢复
    rootBreadcrumb().click()
    await flushPromises()
    expect(document.body.textContent).not.toContain('正在展开目录')
    expect((confirmButton() as HTMLButtonElement).disabled, '导航取消后应可确认旧选择').toBe(false)

    // 迟到的展开结果不得改变就绪或选择
    pending.resolve({ files: DIR_FILES })
    await flushPromises()
    expect((confirmButton() as HTMLButtonElement).disabled).toBe(false)

    confirmButton().click()
    await flushPromises()
    const emitted = wrapper.emitted('update:includePaths')!
    expect(emitted[emitted.length - 1][0], '确认写入的仍是原选择').toEqual(['/剧集'])
    wrapper.unmount()
  })
})

describe('ShareIncludeExcludeEditor 拆成逐项选择的提示', () => {
  function followLostAlert(): HTMLElement | null {
    return document.body.querySelector<HTMLElement>('.follow-lost-alert')
  }

  it('打开选择器、新增勾选或取消显式选中项时都不提示', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集'] })
    await flushPromises()
    browseButton('同步内容').click()
    await flushPromises()
    expect(followLostAlert()).toBeNull()

    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()
    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()
    expect(followLostAlert()).toBeNull()
    wrapper.unmount()
  })

  it('整个分享下取消一项时提示整个分享改为逐项选择，并建议改用排除内容', async () => {
    const wrapper = mountEditor()
    await flushPromises()
    browseButton('同步内容').click()
    await flushPromises()
    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()

    const alert = followLostAlert()
    expect(alert, '应出现提示').toBeTruthy()
    const text = alert!.textContent ?? ''
    expect(text).toContain('整个分享已改为逐项选择')
    expect(text).toContain('之后新增的内容将不再自动同步')
    expect(text).toContain('「排除内容」')
    // 只是提示，不拦截确认
    expect((confirmButton() as HTMLButtonElement).disabled).toBe(false)
    wrapper.unmount()
  })

  it('已选目录里取消子项时提示该目录；重新整体勾选后提示消失', async () => {
    const wrapper = mountEditor({ includePaths: ['/剧集'] })
    await flushPromises()
    browseButton('同步内容').click()
    await flushPromises()
    fileItem('剧集').click()
    await flushPromises()
    checkboxOf(fileItem('01.mp4'))!.click()
    await flushPromises()

    expect(followLostAlert()?.textContent ?? '').toContain('「/剧集」已改为逐项选择')

    document.body.querySelector<HTMLElement>('.breadcrumb button')!.click()
    await flushPromises()
    checkboxOf(fileItem('剧集'))!.click()
    await flushPromises()
    expect(followLostAlert()).toBeNull()
    wrapper.unmount()
  })

  it('提示中的目录去掉合成命名空间前缀', async () => {
    // 根列表本身带合成命名空间时，提示与已选标签一致，只展示逻辑路径
    transfer.previewShareFiles.mockResolvedValue({
      files: [
        { fs_id: 1, is_dir: true, path: '/sharelink1-2/剧集', size: 0, name: '剧集' },
        { fs_id: 2, is_dir: false, path: '/sharelink1-2/说明.txt', size: 10, name: '说明.txt' },
      ],
      share_info: SHARE_INFO,
    })
    transfer.previewShareDir.mockResolvedValue({
      files: [
        { fs_id: 11, is_dir: false, path: '/sharelink1-2/剧集/01.mp4', size: 5, name: '01.mp4' },
        { fs_id: 12, is_dir: false, path: '/sharelink1-2/剧集/02.mp4', size: 5, name: '02.mp4' },
      ],
    })
    const wrapper = mountEditor({ includePaths: ['/sharelink1-2/剧集'] })
    await flushPromises()
    browseButton('同步内容').click()
    await flushPromises()
    fileItem('剧集').click()
    await flushPromises()
    checkboxOf(fileItem('01.mp4'))!.click()
    await flushPromises()

    const text = followLostAlert()?.textContent ?? ''
    expect(text).toContain('「/剧集」已改为逐项选择')
    expect(text).not.toContain('sharelink')
    wrapper.unmount()
  })

  it('取消后重新打开选择器不残留上一次的提示', async () => {
    const wrapper = mountEditor()
    await flushPromises()
    browseButton('同步内容').click()
    await flushPromises()
    checkboxOf(fileItem('说明.txt'))!.click()
    await flushPromises()
    expect(followLostAlert()).toBeTruthy()

    cancelButton().click()
    await flushPromises()
    browseButton('同步内容').click()
    await flushPromises()
    expect(followLostAlert()).toBeNull()
    wrapper.unmount()
  })

  it('排除内容里取消被选中目录的子项时使用排除语义的文案', async () => {
    const wrapper = mountEditor({ excludePaths: ['/剧集'] })
    await flushPromises()
    browseButton('排除内容').click()
    await flushPromises()
    fileItem('剧集').click()
    await flushPromises()
    checkboxOf(fileItem('01.mp4'))!.click()
    await flushPromises()

    const text = followLostAlert()?.textContent ?? ''
    expect(text).toContain('「/剧集」已改为逐项排除')
    expect(text).toContain('之后新增的内容不会再被自动排除')
    expect(text).not.toContain('自动同步')
    wrapper.unmount()
  })

  it('同步内容使用说明提到取消个别项目的影响', async () => {
    const wrapper = mountEditor()
    await flushPromises()
    helpButton('同步内容使用说明').click()
    await flushPromises()
    expect(helpText('同步内容')).toContain('之后新增的内容不再自动同步')
    expect(helpText('同步内容')).toContain('「排除内容」')
    wrapper.unmount()
  })

  it('包含规则添加通配符后发出 includePatterns，不影响排除规则', async () => {
    const wrapper = mountEditor()
    await flushPromises()
    const block = fieldBlock('包含规则')
    const input = block.querySelector<HTMLTextAreaElement>('.exclude-input textarea')!
    input.value = '*SE*\n*.mp4'
    input.dispatchEvent(new Event('input'))
    await flushPromises()
    const add = Array.from(block.querySelectorAll<HTMLButtonElement>('.exclude-input-row button'))
        .find((b) => b.textContent?.trim() === '添加')!
    add.click()
    await flushPromises()
    expect(wrapper.emitted('update:includePatterns')![0][0]).toEqual(['*SE*', '*.mp4'])
    expect(wrapper.emitted('update:excludePatterns')).toBeUndefined()
    wrapper.unmount()
  })

  it('包含规则说明说明白名单语义', async () => {
    const wrapper = mountEditor()
    await flushPromises()
    helpButton('包含规则使用说明').click()
    await flushPromises()
    const help = fieldBlock('包含规则').querySelector('.exclude-help')
    expect(help?.textContent).toContain('至少命中一条才会同步')
    expect(help?.textContent).toContain('目录不会因为名称未命中就被跳过')
    expect(help?.textContent).toContain('仍会再应用排除规则')
    wrapper.unmount()
  })
})
