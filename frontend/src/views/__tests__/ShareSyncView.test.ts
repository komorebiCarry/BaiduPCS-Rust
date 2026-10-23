import { flushPromises, mount } from '@vue/test-utils'
import { ref } from 'vue'
import ElementPlus from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import ShareSyncView from '../ShareSyncView.vue'

const api = vi.hoisted(() => ({
  listSubscriptions: vi.fn(),
  updateSubscription: vi.fn(),
  deleteSubscription: vi.fn(),
  setSubscriptionEnabled: vi.fn(),
  triggerSubscription: vi.fn(),
  resumeSubscription: vi.fn(),
  cancelScan: vi.fn(),
  listRuns: vi.fn(),
  getRun: vi.fn(),
  listRunItems: vi.fn(),
  listSubtasks: vi.fn(),
}))

const editorStub = vi.hoisted(() => ({
  validateRules: vi.fn<() => Promise<{ ok: boolean; message?: string }>>(async () => ({ ok: true })),
}))

vi.mock('@/api/shareSync', () => ({
  listSubscriptions: api.listSubscriptions,
  updateSubscription: api.updateSubscription,
  deleteSubscription: api.deleteSubscription,
  setSubscriptionEnabled: api.setSubscriptionEnabled,
  triggerSubscription: api.triggerSubscription,
  resumeSubscription: api.resumeSubscription,
  cancelScan: api.cancelScan,
  listRuns: api.listRuns,
  getRun: api.getRun,
  listRunItems: api.listRunItems,
  listSubtasks: api.listSubtasks,
  getExcludeRuleCapabilities: vi.fn(async () => ({
    supports_regex: true,
    regex_limits: { max_rules: 64, max_rule_bytes: 1024, max_total_bytes: 32768, max_test_path_bytes: 4096 },
  })),
  validateExcludeRules: editorStub.validateRules,
}))

const ws = vi.hoisted(() => ({
  handler: null as ((e: { detail: unknown }) => void) | null,
  subscribe: vi.fn(),
  onShareSyncEvent: vi.fn((cb: (e: { detail: unknown }) => void) => {
    ws.handler = cb
    return () => {}
  }),
  onConnectionStateChange: vi.fn(() => () => {}),
}))

vi.mock('@/utils/websocket', () => ({
  getWebSocketClient: () => ws,
  connectWebSocket: vi.fn(),
}))

const auth = vi.hoisted(() => ({
  hasMultipleAccounts: false,
  activeUid: 1 as number | null,
  accounts: [{ uid: 1, username: 'u', nickname: null }],
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => auth,
}))

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(false),
}))

vi.mock('@/utils/backendHealth', () => ({
  createAdaptivePoller: () => ({ start: vi.fn(), stop: vi.fn() }),
}))

vi.mock('@/api/config', () => ({
  getConfig: vi.fn().mockResolvedValue({ download: { recent_directory: '/tmp', default_directory: '/tmp' } }),
  updateRecentDirDebounced: vi.fn(),
  setDefaultDownloadDir: vi.fn(),
}))

const SUB = {
  id: 'sub-1',
  name: '测试订阅',
  share_url: 'https://pan.baidu.com/s/1y7CluAbCdEfGh',
  password: null,
  include_paths: [],
  exclude_patterns: [],
  exclude_paths: ['/广告'],
  targets: [{ kind: 'local', local_path: '/tmp' }],
  conflict_strategy: 'overwrite',
  delete_missing: false,
  poll_config: { enabled: true, mode: 'interval', interval_secs: 1800 },
  enabled: true,
  created_at: '2026-01-01T00:00:00Z',
  updated_at: '2026-01-01T00:00:00Z',
  owner_uid: 1,
}

function mountView() {
  return mount(ShareSyncView, {
    attachTo: document.body,
    global: {
      plugins: [ElementPlus],
      stubs: {
        AccountFilter: true,
        AccountBadge: true,
        TransferDialog: true,
        ShareIncludeExcludeEditor: {
          name: 'ShareIncludeExcludeEditor',
          props: [
            'shareUrl', 'password', 'includePaths', 'excludePatterns',
            'excludeRegexPatterns', 'excludePaths', 'ownerUid', 'ownerLoggedIn',
          ],
          emits: ['update:includePaths', 'update:excludePatterns', 'update:excludeRegexPatterns', 'update:excludePaths'],
          template: '<div class="include-exclude-editor-stub" />',
          setup(_props: unknown, { expose }: { expose: (e: Record<string, unknown>) => void }) {
            expose({ validateRules: editorStub.validateRules })
          },
        },
        CronExprEditor: true,
        FilePickerModal: {
          name: 'FilePickerModal',
          props: ['modelValue', 'mode', 'title'],
          template: '<div class="file-picker-stub" :data-visible="modelValue === true ? \'true\' : \'false\'" />',
        },
      },
    },
  })
}

/** 扫描进度卡片里的取消按钮 */
function cancelButton(): HTMLElement | null {
  return document.body.querySelector<HTMLElement>('.scan-cancel-btn')
}

function scanCurrent(): HTMLElement | null {
  return document.body.querySelector<HTMLElement>('.scan-current')
}

/** 触发一次 WS 事件 */
function emitWs(detail: unknown) {
  ws.handler?.({ detail })
}

const SCAN_PROGRESS = {
  type: 'scan_progress',
  run_id: 'run-1',
  subscription_id: 'sub-1',
  dirs_done: 3,
  dirs_pending: 2,
  files_seen: 10,
  current_dir: '/剧集',
  attempt: 1,
  cached_hits: 0,
}

beforeEach(() => {
  ws.handler = null
  api.listSubscriptions.mockReset().mockResolvedValue([SUB])
  api.cancelScan.mockReset().mockResolvedValue({
    subscription_id: 'sub-1',
    run_id: 'run-1',
    cancelled: true,
  })
  api.listRuns.mockReset().mockResolvedValue([])
  api.listSubtasks.mockReset().mockResolvedValue([])
  api.updateSubscription.mockReset().mockResolvedValue(undefined)
})

afterEach(() => {
  document.body.innerHTML = ''
})

describe('ShareSyncView 订阅编辑弹窗同步目标', () => {
  const SUB2 = {
    ...SUB,
    id: 'sub-2',
    name: '另一份订阅',
    targets: [{ kind: 'netdisk', remote_path: '/另一份网盘目录', save_fs_id: 0 }],
  }

  /** 订阅卡片里的「编辑」按钮（按订阅名定位卡片） */
  function editButton(name: string): HTMLElement {
    const card = Array.from(document.body.querySelectorAll<HTMLElement>('.config-card'))
        .find((el) => el.textContent?.includes(name))
    const btn = Array.from((card ?? document.body).querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '编辑')
    expect(btn, `「${name}」应有编辑按钮`).toBeTruthy()
    return btn!
  }

  async function mountAndOpen(name: string) {
    const wrapper = mountView()
    await flushPromises()
    editButton(name).click()
    await flushPromises()
    return wrapper
  }

  const dialog = () => document.body.querySelector<HTMLElement>('.subscription-dialog')
  const switches = () => Array.from(document.body.querySelectorAll<HTMLElement>('.subscription-dialog .el-switch'))
  const pathInputs = () => Array.from(document.body.querySelectorAll<HTMLInputElement>('.subscription-dialog .target-path input'))
  const pickButton = () => document.body.querySelector<HTMLButtonElement>('.subscription-dialog .target-path .el-input-group__append button')
  const pickerStub = () => document.body.querySelector<HTMLElement>('.file-picker-stub')

  it('对话框使用居中布局并固定头部/底部、主体滚动', async () => {
    const wrapper = await mountAndOpen(SUB.name)

    const el = dialog()
    expect(el, '应渲染编辑弹窗').toBeTruthy()
    expect(el!.classList.contains('is-align-center'), '应启用居中').toBe(true)
    expect(el!.querySelector('.el-dialog__header')).toBeTruthy()
    expect(el!.querySelector('.el-dialog__body .el-form'), '表单应在对话框主体内').toBeTruthy()
    expect(el!.querySelector('.el-dialog__footer')?.textContent).toContain('保存')
    wrapper.unmount()
  })

  it('关闭网盘目标后路径仍可见，重新开启原样恢复', async () => {
    const wrapper = await mountAndOpen(SUB.name)

    const [netdiskSwitch] = switches()
    const [netdiskInput] = pathInputs()
    expect(netdiskInput.disabled, '网盘目标默认关闭，输入框应禁用').toBe(true)

    netdiskSwitch.click()
    await flushPromises()
    expect(netdiskInput.disabled).toBe(false)

    netdiskInput.value = '/我的资源/同步'
    netdiskInput.dispatchEvent(new Event('input'))
    await flushPromises()

    netdiskSwitch.click()
    await flushPromises()
    expect(pathInputs()[0].disabled, '关闭后仍禁用展示').toBe(true)
    expect(pathInputs()[0].value, '关闭后应保留草稿路径').toBe('/我的资源/同步')

    netdiskSwitch.click()
    await flushPromises()
    expect(pathInputs()[0].value, '重新开启应恢复草稿').toBe('/我的资源/同步')
    wrapper.unmount()
  })

  it('仅剩一个目标时不能关闭，并提示至少保留一个', async () => {
    const wrapper = await mountAndOpen(SUB.name)

    const [, localSwitch] = switches()
    expect(localSwitch.classList.contains('is-checked'), '本地目标默认启用').toBe(true)

    localSwitch.click()
    await flushPromises()

    expect(switches()[1].classList.contains('is-checked'), '不可关闭最后一个目标').toBe(true)
    expect(document.body.textContent).toContain('至少需要开启一个同步目标')
    wrapper.unmount()
  })

  it('保存只提交启用的目标，本地模式随网盘目标推导', async () => {
    api.updateSubscription.mockResolvedValue({ ...SUB })
    const wrapper = await mountAndOpen(SUB.name)

    // 仅本地：分享直下
    Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '保存')!.click()
    await flushPromises()
    expect(api.updateSubscription).toHaveBeenCalledTimes(1)
    let req = api.updateSubscription.mock.calls[0][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual([
      { kind: 'local', local_path: '/tmp', mode: 'share_direct' },
    ])

    // 开启网盘后再保存：本地改为转存并下载
    editButton(SUB.name).click()
    await flushPromises()
    switches()[0].click()
    await flushPromises()
    const netdiskInput = pathInputs()[0]
    netdiskInput.value = '/我的资源/同步'
    netdiskInput.dispatchEvent(new Event('input'))
    await flushPromises()
    Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '保存')!.click()
    await flushPromises()

    expect(api.updateSubscription).toHaveBeenCalledTimes(2)
    req = api.updateSubscription.mock.calls[1][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual([
      { kind: 'netdisk', remote_path: '/我的资源/同步', save_fs_id: 0 },
      { kind: 'local', local_path: '/tmp', mode: 'transfer_and_download' },
    ])
    wrapper.unmount()
  })

  it('本地目标关闭时选择按钮禁用且不打开目录选择器', async () => {
    const wrapper = await mountAndOpen(SUB.name)

    const [, localSwitch] = switches()
    // 先开启网盘，才能关闭本地目标
    switches()[0].click()
    await flushPromises()
    localSwitch.click()
    await flushPromises()

    const btn = pickButton()
    expect(btn, '本地行的选择按钮应始终存在').toBeTruthy()
    expect(btn!.disabled, '本地目标关闭时选择按钮应禁用').toBe(true)
    expect(pickerStub()?.getAttribute('data-visible'), '目录选择器不应打开').toBe('false')
    expect(pathInputs()[1].disabled, '本地路径输入应禁用').toBe(true)
    wrapper.unmount()
  })

  it('禁用目标的草稿不跨订阅泄漏', async () => {
    api.listSubscriptions.mockResolvedValue([SUB2, SUB])
    const wrapper = mountView()
    await flushPromises()

    editButton(SUB2.name).click()
    await flushPromises()
    expect(pathInputs()[0].value, '另一份订阅的网盘路径').toBe('/另一份网盘目录')
    switches()[0].click()
    await flushPromises()
    expect(pathInputs()[0].value, '关闭后仍保留该订阅草稿').toBe('/另一份网盘目录')

    Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '取消')!.click()
    await flushPromises()

    editButton(SUB.name).click()
    await flushPromises()
    expect(switches()[0].classList.contains('is-checked'), '另一个订阅默认无网盘目标').toBe(false)
    expect(pathInputs()[0].value, '不得显示上一个订阅的草稿').toBe('/')
    wrapper.unmount()
  })

  it('取消不写接口，关闭后草稿重置', async () => {
    const wrapper = await mountAndOpen(SUB.name)

    switches()[0].click()
    await flushPromises()
    Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '取消')!.click()
    await flushPromises()

    expect(api.updateSubscription).not.toHaveBeenCalled()
    editButton(SUB.name).click()
    await flushPromises()
    expect(pathInputs()[0].value, '关闭后草稿应重置').toBe('/')
    wrapper.unmount()
  })
})

describe('ShareSyncView 取消扫描', () => {
  it.each(['run_cancelled', 'run_completed', 'run_failed', 'diff_detected'])(
    '迟到的旧轮 %s 事件不得清掉新轮扫描',
    async (type) => {
      const wrapper = mountView()
      try {
        await flushPromises()
        emitWs(SCAN_PROGRESS)
        emitWs({ type: 'run_started', run_id: 'run-2', subscription_id: 'sub-1' })
        emitWs({ ...SCAN_PROGRESS, run_id: 'run-2', current_dir: '/新轮' })
        await flushPromises()
        emitWs({ type, run_id: 'run-1', subscription_id: 'sub-1', error: '旧轮错误' })
        await flushPromises()
        expect(cancelButton()).toBeTruthy()
        cancelButton()!.click()
        await flushPromises()
        expect(api.cancelScan).toHaveBeenCalledWith('sub-1', 'run-2')
        expect(cancelButton()).toBeNull()
      } finally {
        wrapper.unmount()
      }
    },
  )

  it('实际 API 客户端把观察到的 run_id 放入取消请求体', async () => {
    const { rawApiClient } = await import('@/api/client')
    const actual = await vi.importActual<typeof import('@/api/shareSync')>('@/api/shareSync')
    const post = vi.spyOn(rawApiClient, 'post').mockResolvedValueOnce({
      data: { success: true, data: { subscription_id: 'sub-1', run_id: 'run-2', cancelled: true } },
    })
    try {
      await expect(actual.cancelScan('sub-1', 'run-2')).resolves.toEqual({
        subscription_id: 'sub-1', run_id: 'run-2', cancelled: true,
      })
      expect(post).toHaveBeenCalledWith('/share-sync/subscriptions/sub-1/cancel-scan', { run_id: 'run-2' })
    } finally {
      post.mockRestore()
    }
  })

  it('旧轮取消的 REST 响应不得清除新轮扫描', async () => {
    let resolveCancel!: (value: { subscription_id: string; run_id: string; cancelled: boolean }) => void
    api.cancelScan.mockReturnValueOnce(new Promise(resolve => { resolveCancel = resolve }))
    const wrapper = mountView()
    try {
      await flushPromises()
      emitWs(SCAN_PROGRESS)
      await flushPromises()
      cancelButton()!.click()
      await flushPromises()
      emitWs({ type: 'run_cancelled', run_id: 'run-1', subscription_id: 'sub-1' })
      emitWs({ type: 'run_started', run_id: 'run-2', subscription_id: 'sub-1' })
      emitWs({ ...SCAN_PROGRESS, run_id: 'run-2', current_dir: '/新轮' })
      await flushPromises()
      expect(cancelButton()).toBeTruthy()
      resolveCancel({ subscription_id: 'sub-1', run_id: 'run-1', cancelled: true })
      await flushPromises()
      expect(cancelButton(), '旧轮响应不应隐藏新轮取消入口').toBeTruthy()
    } finally {
      wrapper.unmount()
    }
  })

  it('扫描中显示取消按钮，点击后调用取消接口并清掉扫描卡片', async () => {
    const wrapper = mountView()
    await flushPromises()

    // 扫描进度由 WS 事件驱动
    emitWs(SCAN_PROGRESS)
    await flushPromises()

    const btn = cancelButton()
    expect(btn, '扫描中应显示取消按钮').toBeTruthy()
    expect(btn!.getAttribute('aria-label')).toBe('取消本次扫描')

    btn!.click()
    await flushPromises()

    expect(api.cancelScan).toHaveBeenCalledTimes(1)
    // 取消必须绑定到用户观察到的那一轮（后端据此拒绝迟到的旧请求）
    expect(api.cancelScan).toHaveBeenCalledWith('sub-1', 'run-1')
    // REST 成功后立刻清掉扫描卡片（WS 断线时也能复位）
    expect(cancelButton()).toBeNull()
    wrapper.unmount()
  })

  it('取消失败时如实报错，扫描卡片保留', async () => {
    api.cancelScan.mockRejectedValueOnce({
      response: { status: 409, data: { message: '该轮同步已进入执行阶段，无法取消扫描' } },
    })
    const wrapper = mountView()
    await flushPromises()

    emitWs(SCAN_PROGRESS)
    await flushPromises()

    cancelButton()!.click()
    await flushPromises()

    // 取消只停扫描，不改变订阅启用状态
    expect(api.setSubscriptionEnabled).not.toHaveBeenCalled()
    expect(cancelButton(), '取消失败时扫描卡片应保留').toBeTruthy()
    wrapper.unmount()
  })

  it('收到 run_cancelled 事件后清掉扫描卡片', async () => {
    const wrapper = mountView()
    await flushPromises()

    emitWs(SCAN_PROGRESS)
    await flushPromises()
    expect(cancelButton()).toBeTruthy()

    emitWs({ type: 'run_cancelled', run_id: 'run-1', subscription_id: 'sub-1' })
    await flushPromises()

    expect(cancelButton()).toBeNull()
    wrapper.unmount()
  })
})

describe('ShareSyncView 扫描当前路径展示', () => {
  function scanCurrentPath(): HTMLElement | null {
    return document.body.querySelector<HTMLElement>('.scan-current-path')
  }

  function scanCurrentLabel(): HTMLElement | null {
    return document.body.querySelector<HTMLElement>('.scan-current-label')
  }

  it('完整多级路径存于单行路径节点与 title，省略只交给 CSS', async () => {
    const full = '/剧集/第一季/高清 1080P/第 01 集'
    const wrapper = mountView()
    await flushPromises()

    emitWs({ ...SCAN_PROGRESS, current_dir: full })
    await flushPromises()

    const el = scanCurrent()
    expect(el, '应显示当前扫描目录').toBeTruthy()
    expect(el!.getAttribute('title'), 'title 始终是完整路径').toBe(full)
    expect(el!.textContent, '路径正文应完整呈现，而不是只挂在 title 上').toContain(full)
    expect(el!.textContent).not.toContain('…')
    expect(scanCurrentLabel()!.textContent, '「当前」标签独立且不进入省略容器').toBe('当前：')
    expect(scanCurrentPath()!.querySelector('bdi'), '路径内容需 LTR 隔离').toBeTruthy()
    expect(scanCurrentPath()!.textContent).toBe(full)
    wrapper.unmount()
  })

  it('混合中文、拉丁、空格、数字与标点的顺序原样保留', async () => {
    const full = '/我的 资源/Season 01/第1集 1080P (2026) [HEVC]'
    const wrapper = mountView()
    await flushPromises()

    emitWs({ ...SCAN_PROGRESS, current_dir: full })
    await flushPromises()

    expect(scanCurrentPath()!.textContent, '不得改写或重排路径字符').toBe(full)
    expect(scanCurrent()!.getAttribute('title')).toBe(full)
    wrapper.unmount()
  })

  it('路径行位于可滚动的 .ss-content 内，末行才能滚出底部标签栏', async () => {
    const wrapper = mountView()
    await flushPromises()

    emitWs({ ...SCAN_PROGRESS, current_dir: '/剧集/第一季/第 01 集' })
    await flushPromises()

    const el = scanCurrent()
    expect(el!.closest('.ss-content'), '路径行必须在可滚动容器内，末行才不会被底部标签栏盖住').toBeTruthy()
    wrapper.unmount()
  })

  it('根路径与短路径原样展示', async () => {
    const wrapper = mountView()
    await flushPromises()

    emitWs({ ...SCAN_PROGRESS, current_dir: '/' })
    await flushPromises()
    expect(scanCurrent()!.textContent).toContain('/')
    expect(scanCurrent()!.textContent).not.toContain('…')
    expect(scanCurrentPath()!.textContent).toBe('/')
    expect(scanCurrent()!.getAttribute('title')).toBe('/')

    emitWs({ ...SCAN_PROGRESS, current_dir: '/剧集' })
    await flushPromises()
    expect(scanCurrent()!.textContent).toContain('/剧集')
    expect(scanCurrentPath()!.textContent).toBe('/剧集')
    expect(scanCurrent()!.getAttribute('title')).toBe('/剧集')
    wrapper.unmount()
  })

  it('新的扫描帧替换旧路径', async () => {
    const wrapper = mountView()
    await flushPromises()

    emitWs({ ...SCAN_PROGRESS, current_dir: '/剧集/第一季' })
    await flushPromises()
    expect(scanCurrentPath()!.textContent).toBe('/剧集/第一季')

    const next = '/剧集/第二季/第 02 集'
    emitWs({ ...SCAN_PROGRESS, current_dir: next })
    await flushPromises()
    expect(scanCurrentPath()!.textContent).toBe(next)
    expect(scanCurrent()!.getAttribute('title')).toBe(next)
    expect(scanCurrent()!.textContent).not.toContain('/剧集/第一季')
    wrapper.unmount()
  })

  it('超长无分隔片段完整保留，结尾名称不被 JS 截断', async () => {
    const long = '/资料库/' + 'A'.repeat(120)
    const wrapper = mountView()
    await flushPromises()

    emitWs({ ...SCAN_PROGRESS, current_dir: long })
    await flushPromises()

    expect(scanCurrentPath()!.textContent).toBe(long)
    expect(scanCurrent()!.getAttribute('title')).toBe(long)
    expect(scanCurrent()!.textContent).not.toContain('…')
    wrapper.unmount()
  })
})

describe('ShareSyncView 正则排除规则', () => {
  const WITH_REGEX = {
    ...SUB,
    exclude_patterns: ['*.tmp'],
    exclude_regex_patterns: ['^/股票/按月归档/2025.*$'],
  }

  function editButton(name: string): HTMLElement {
    const card = Array.from(document.body.querySelectorAll<HTMLElement>('.config-card'))
        .find((el) => el.textContent?.includes(name))
    const btn = Array.from((card ?? document.body).querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '编辑')
    expect(btn, `「${name}」应有编辑按钮`).toBeTruthy()
    return btn!
  }

  it('卡片摘要分别统计通配符与正则条数', async () => {
    api.listSubscriptions.mockResolvedValue([WITH_REGEX])
    const wrapper = mountView()
    await flushPromises()

    const card = document.body.querySelector<HTMLElement>('.config-card')!
    const text = card.textContent ?? ''
    expect(text).toContain('排除通配符 1 条')
    expect(text).toContain('排除正则 1 条')
    wrapper.unmount()
  })

  it('详情弹窗为正则标签加标记，与通配符区分', async () => {
    api.listSubscriptions.mockResolvedValue([WITH_REGEX])
    const wrapper = mountView()
    await flushPromises()

    const card = document.body.querySelector<HTMLElement>('.config-card')!
    card.click()
    await flushPromises()

    const tags = Array.from(document.body.querySelectorAll<HTMLElement>('.path-tags .el-tag'))
    const regexTag = tags.find((t) => t.textContent?.includes('^/股票/按月归档/2025.*$'))
    expect(regexTag, '详情应展示正则规则').toBeTruthy()
    expect(regexTag!.textContent).toContain('正则')
    const globTag = tags.find((t) => t.textContent?.includes('*.tmp'))
    expect(globTag?.textContent, '通配符标签不加正则标记').not.toContain('正则')
    wrapper.unmount()
  })

  it('保存时提交正则字段并保持类型独立', async () => {
    api.listSubscriptions.mockResolvedValue([WITH_REGEX])
    api.updateSubscription.mockResolvedValue(WITH_REGEX)
    const wrapper = mountView()
    await flushPromises()

    editButton(WITH_REGEX.name).click()
    await flushPromises()
    const save = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '保存')!
    save.click()
    await flushPromises()

    expect(api.updateSubscription).toHaveBeenCalledTimes(1)
    const req = api.updateSubscription.mock.calls[0][1] as {
      exclude_patterns: string[]
      exclude_regex_patterns: string[]
    }
    expect(req.exclude_patterns).toEqual(['*.tmp'])
    expect(req.exclude_regex_patterns).toEqual(['^/股票/按月归档/2025.*$'])
    wrapper.unmount()
  })

  it('正则校验未通过时不提交保存', async () => {
    api.listSubscriptions.mockResolvedValue([WITH_REGEX])
    editorStub.validateRules.mockResolvedValueOnce({
      ok: false,
      message: '正则第 1 条：无法编译正则',
    })
    const wrapper = mountView()
    await flushPromises()

    editButton(WITH_REGEX.name).click()
    await flushPromises()
    const save = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '保存')!
    save.click()
    await flushPromises()

    expect(editorStub.validateRules).toHaveBeenCalled()
    expect(api.updateSubscription, '校验失败不得写接口').not.toHaveBeenCalled()
    expect(document.body.textContent).toContain('无法编译正则')
    wrapper.unmount()
  })

  it('提交前归一化正则列表（去空白、去重、保序）', async () => {
    const messy = {
      ...SUB,
      exclude_regex_patterns: ['  ^/a$  ', '^/a$', '', '^/b$'],
    }
    api.listSubscriptions.mockResolvedValue([messy])
    api.updateSubscription.mockResolvedValue(messy)
    const wrapper = mountView()
    await flushPromises()

    editButton(messy.name).click()
    await flushPromises()
    const save = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '保存')!
    save.click()
    await flushPromises()

    const req = api.updateSubscription.mock.calls[0][1] as { exclude_regex_patterns: string[] }
    expect(req.exclude_regex_patterns).toEqual(['^/a$', '^/b$'])
    wrapper.unmount()
  })

  it('旧响应缺少正则字段时按空处理，不报错', async () => {
    const legacy = { ...SUB }
    delete (legacy as { exclude_regex_patterns?: string[] }).exclude_regex_patterns
    api.listSubscriptions.mockResolvedValue([legacy])
    api.updateSubscription.mockResolvedValue(legacy)
    const wrapper = mountView()
    await flushPromises()

    const card = document.body.querySelector<HTMLElement>('.config-card')!
    expect(card.textContent).not.toContain('排除正则')

    editButton(legacy.name).click()
    await flushPromises()
    const save = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '保存')!
    save.click()
    await flushPromises()

    const req = api.updateSubscription.mock.calls[0][1] as { exclude_regex_patterns: string[] }
    expect(req.exclude_regex_patterns).toEqual([])
    wrapper.unmount()
  })
})
