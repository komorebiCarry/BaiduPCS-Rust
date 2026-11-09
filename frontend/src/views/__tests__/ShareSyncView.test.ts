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
  accounts: [{ uid: 1, username: 'u', nickname: null }] as Array<{ uid: number; username: string; nickname: string | null }>,
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => auth,
}))

const mobile = vi.hoisted(() => ({ value: false }))

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(mobile.value),
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
            'excludeRegexPatterns', 'includePatterns', 'includeRegexPatterns',
            'excludePaths', 'ownerUid', 'ownerLoggedIn',
          ],
          emits: [
            'update:includePaths', 'update:excludePatterns', 'update:excludeRegexPatterns',
            'update:includePatterns', 'update:includeRegexPatterns', 'update:excludePaths',
          ],
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
  mobile.value = false
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

describe('ShareSyncView 排除内容文案', () => {
  it('卡片摘要与详情都用「排除内容」措辞', async () => {
    const wrapper = mountView()
    await flushPromises()

    const card = document.body.querySelector<HTMLElement>('.config-card')!
    expect(card.textContent).toContain('排除内容 1 项')
    expect(card.textContent, '不再使用「排除目录」措辞').not.toContain('排除目录')

    card.click()
    await flushPromises()

    const labels = Array.from(
        document.body.querySelectorAll<HTMLElement>('.el-descriptions__label'),
    ).map((el) => el.textContent?.trim())
    expect(labels).toContain('排除内容')
    expect(labels, '详情不再使用「排除目录」措辞').not.toContain('排除目录')
    wrapper.unmount()
  })

  it('详情标签只省略合成命名空间，其他路径原样显示', async () => {
    api.listSubscriptions.mockResolvedValue([{
      ...SUB,
      include_paths: ['/sharelink1-2/A/报告 .txt', '/apps/bypy/B'],
      exclude_paths: ['/sharelink1-2/C/广告. '],
    }])
    const wrapper = mountView()
    await flushPromises()

    document.body.querySelector<HTMLElement>('.config-card')!.click()
    await flushPromises()

    const tags = Array.from(document.body.querySelectorAll<HTMLElement>('.path-tags .el-tag'))
        .map((tag) => tag.textContent?.trim())
    expect(tags).toContain('/A/报告 .txt')
    expect(tags).toContain('/apps/bypy/B')
    expect(tags).toContain('/C/广告.')
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

describe('ShareSyncView 跨账号目标（#179）', () => {
  const saveButton = () => Array.from(document.body.querySelectorAll<HTMLElement>('button'))
      .find((b) => b.textContent?.trim() === '保存')!

  function editButton(name: string): HTMLElement {
    const card = Array.from(document.body.querySelectorAll<HTMLElement>('.config-card'))
        .find((el) => el.textContent?.includes(name))
    return Array.from((card ?? document.body).querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '编辑')!
  }

  afterEach(() => {
    auth.hasMultipleAccounts = false
    auth.accounts = [{ uid: 1, username: 'u', nickname: null }]
  })

  it('老订阅保存时不写入账号字段', async () => {
    api.updateSubscription.mockResolvedValue({ ...SUB })
    const wrapper = mountView()
    await flushPromises()
    editButton(SUB.name).click()
    await flushPromises()
    expect(document.body.querySelector('.subscription-dialog .target-account-row'), '单账号老订阅不展示账号控件').toBeNull()
    saveButton().click()
    await flushPromises()
    const req = api.updateSubscription.mock.calls[0][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual([{ kind: 'local', local_path: '/tmp', mode: 'share_direct' }])
    wrapper.unmount()
  })

  it('已有跨账号目标原样保留（归集网盘 + 默认下载账号），卡片标明账号', async () => {
    const sub = {
      ...SUB,
      targets: [
        { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
        { kind: 'local', local_path: '/tmp', mode: 'transfer_and_download', use_default_download_account: true },
        { kind: 'netdisk', remote_path: '/归集', save_fs_id: 0, account_uid: 2 },
      ],
    }
    api.listSubscriptions.mockResolvedValue([sub])
    api.updateSubscription.mockResolvedValue(sub)
    const wrapper = mountView()
    await flushPromises()

    const card = document.body.querySelector<HTMLElement>('.config-card')!
    // #183：带账号的目标写作「网盘→<账号名>」，账号不在列表时回退 UID
    expect(card.textContent).toContain('网盘→UID:2')
    expect(card.textContent).toContain('本地(默认下载账号)')

    editButton(sub.name).click()
    await flushPromises()
    expect(document.body.querySelectorAll('.subscription-dialog .extra-netdisk-row').length).toBe(1)
    saveButton().click()
    await flushPromises()

    const req = api.updateSubscription.mock.calls[0][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual([
      { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
      { kind: 'local', local_path: '/tmp', mode: 'transfer_and_download', use_default_download_account: true },
      { kind: 'netdisk', remote_path: '/归集', save_fs_id: 0, account_uid: 2 },
    ])
    wrapper.unmount()
  })

  it('多账号可新增归集目标，默认选未占用账号；同账号重复时拒绝保存', async () => {
    auth.hasMultipleAccounts = true
    auth.accounts = [
      { uid: 1, username: 'u', nickname: null },
      { uid: 2, username: 'v', nickname: null },
    ]
    const sub = { ...SUB, targets: [{ kind: 'netdisk', remote_path: '/主', save_fs_id: 0 }] }
    api.listSubscriptions.mockResolvedValue([sub])
    api.updateSubscription.mockResolvedValue(sub)
    const wrapper = mountView()
    await flushPromises()
    editButton(sub.name).click()
    await flushPromises()

    const addBtn = () => Array.from(document.body.querySelectorAll<HTMLElement>('.subscription-dialog button'))
        .find((b) => b.textContent?.includes('同时转存到其他账号的网盘'))!
    addBtn().click()
    await flushPromises()
    saveButton().click()
    await flushPromises()
    expect(api.updateSubscription).toHaveBeenCalledTimes(1)
    let req = api.updateSubscription.mock.calls[0][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual([
      { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
      { kind: 'netdisk', remote_path: '/主', save_fs_id: 0, account_uid: 2 },
    ])

    // 再次打开并新增两行：第二行已无空闲账号，回落到所属账号 → 与主网盘目标同账号，校验拦截
    editButton(sub.name).click()
    await flushPromises()
    addBtn().click()
    await flushPromises()
    addBtn().click()
    await flushPromises()
    expect(document.body.querySelectorAll('.subscription-dialog .extra-netdisk-row').length).toBe(2)
    saveButton().click()
    await flushPromises()
    expect(api.updateSubscription).toHaveBeenCalledTimes(1)
    expect(document.body.textContent).toContain('多个网盘目标需分属不同账号')

    // 移除重复行后可保存
    const removeBtn = document.body.querySelector<HTMLElement>('.subscription-dialog [aria-label="移除归集目标 #2"]')!
    removeBtn.click()
    await flushPromises()
    saveButton().click()
    await flushPromises()
    expect(api.updateSubscription).toHaveBeenCalledTimes(2)
    req = api.updateSubscription.mock.calls[1][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual([
      { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
      { kind: 'netdisk', remote_path: '/主', save_fs_id: 0, account_uid: 2 },
    ])
    wrapper.unmount()
  })
  const dialogSwitches = () => Array.from(document.body.querySelectorAll<HTMLElement>('.subscription-dialog .el-switch'))
  const hints = () => Array.from(document.body.querySelectorAll<HTMLElement>('.subscription-dialog .sync-mode-hint'))
      .map((el) => el.textContent?.trim() ?? '')

  it('关闭「转存到网盘」后归集目标隐藏且不提交；重开对话框开关保持关闭', async () => {
    auth.hasMultipleAccounts = true
    auth.accounts = [
      { uid: 1, username: 'u', nickname: null },
      { uid: 2, username: 'v', nickname: null },
    ]
    const sub = {
      ...SUB,
      targets: [
        { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
        { kind: 'local', local_path: '/tmp', mode: 'transfer_and_download' },
        { kind: 'netdisk', remote_path: '/归集', save_fs_id: 0, account_uid: 2 },
      ],
    }
    api.listSubscriptions.mockResolvedValue([sub])
    const wrapper = mountView()
    await flushPromises()
    editButton(sub.name).click()
    await flushPromises()
    const addBtn = () => Array.from(document.body.querySelectorAll<HTMLElement>('.subscription-dialog button'))
        .find((b) => b.textContent?.includes('同时转存到其他账号的网盘'))
    expect(document.body.querySelectorAll('.subscription-dialog .extra-netdisk-row').length).toBe(1)
    expect(addBtn()).toBeTruthy()

    dialogSwitches()[0].click()
    await flushPromises()
    expect(document.body.querySelectorAll('.subscription-dialog .extra-netdisk-row').length, '归集行随开关隐藏').toBe(0)
    expect(addBtn(), '新增按钮随开关隐藏').toBeUndefined()

    let saved: typeof sub | null = null
    api.updateSubscription.mockImplementation(async (_id: string, req: { targets: typeof sub.targets }) => {
      saved = { ...sub, targets: req.targets }
      return saved
    })
    saveButton().click()
    await flushPromises()
    const req = api.updateSubscription.mock.calls[0][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual([{ kind: 'local', local_path: '/tmp', mode: 'share_direct' }])

    // 往返稳定：用保存后的目标重新打开，网盘开关仍关闭、没有 UID 2 的目标被提升为主网盘
    api.listSubscriptions.mockResolvedValue([saved!])
    wrapper.unmount()
    const wrapper2 = mountView()
    await flushPromises()
    editButton(sub.name).click()
    await flushPromises()
    expect(dialogSwitches()[0].classList.contains('is-checked')).toBe(false)
    expect(document.body.querySelectorAll('.subscription-dialog .extra-netdisk-row').length).toBe(0)
    wrapper2.unmount()
  })

  it('关闭后再开启「转存到网盘」恢复本次会话里的归集目标', async () => {
    auth.hasMultipleAccounts = true
    auth.accounts = [
      { uid: 1, username: 'u', nickname: null },
      { uid: 2, username: 'v', nickname: null },
    ]
    const sub = {
      ...SUB,
      targets: [
        { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
        { kind: 'local', local_path: '/tmp', mode: 'transfer_and_download' },
        { kind: 'netdisk', remote_path: '/归集', save_fs_id: 0, account_uid: 2 },
      ],
    }
    api.listSubscriptions.mockResolvedValue([sub])
    api.updateSubscription.mockResolvedValue(sub)
    const wrapper = mountView()
    await flushPromises()
    editButton(sub.name).click()
    await flushPromises()
    dialogSwitches()[0].click()
    await flushPromises()
    dialogSwitches()[0].click()
    await flushPromises()
    expect(document.body.querySelectorAll('.subscription-dialog .extra-netdisk-row').length).toBe(1)
    saveButton().click()
    await flushPromises()
    const req = api.updateSubscription.mock.calls[0][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual(sub.targets)
    wrapper.unmount()
  })

  it('A 账号存网盘 + B 账号下载到本地：提示不再矛盾，本地按分享直下保存', async () => {
    auth.hasMultipleAccounts = true
    auth.accounts = [
      { uid: 1, username: 'u', nickname: null },
      { uid: 2, username: 'v', nickname: null },
    ]
    const sub = {
      ...SUB,
      targets: [
        { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
        { kind: 'local', local_path: '/tmp', mode: 'transfer_and_download', download_uid: 2 },
      ],
    }
    api.listSubscriptions.mockResolvedValue([sub])
    api.updateSubscription.mockResolvedValue(sub)
    const wrapper = mountView()
    await flushPromises()
    editButton(sub.name).click()
    await flushPromises()
    const text = hints().join('\n')
    expect(text).not.toContain('从网盘副本下载到本地')
    expect(text).toContain('UID: 2')
    expect(text).toContain('分享直下')
    expect(text).toContain('网盘目标照常转存并保留')
    saveButton().click()
    await flushPromises()
    const req = api.updateSubscription.mock.calls[0][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets).toEqual([
      { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
      { kind: 'local', local_path: '/tmp', mode: 'share_direct', download_uid: 2 },
    ])
    wrapper.unmount()
  })

  it('下载账号同时有归集网盘目标时，提示从该账号网盘副本下载', async () => {
    auth.hasMultipleAccounts = true
    auth.accounts = [
      { uid: 1, username: 'u', nickname: null },
      { uid: 2, username: 'v', nickname: null },
    ]
    const sub = {
      ...SUB,
      targets: [
        { kind: 'netdisk', remote_path: '/主', save_fs_id: 0 },
        { kind: 'local', local_path: '/tmp', mode: 'share_direct', download_uid: 2 },
        { kind: 'netdisk', remote_path: '/归集', save_fs_id: 0, account_uid: 2 },
      ],
    }
    api.listSubscriptions.mockResolvedValue([sub])
    api.updateSubscription.mockResolvedValue(sub)
    const wrapper = mountView()
    await flushPromises()
    editButton(sub.name).click()
    await flushPromises()
    const text = hints().join('\n')
    expect(text).toContain('从下载账号（UID: 2）的网盘副本下载到本地，网盘保留。')
    expect(text).not.toContain('分享直下')
    saveButton().click()
    await flushPromises()
    const req = api.updateSubscription.mock.calls[0][1] as { targets: Array<Record<string, unknown>> }
    expect(req.targets[1]).toEqual({ kind: 'local', local_path: '/tmp', mode: 'transfer_and_download', download_uid: 2 })
    wrapper.unmount()
  })
})

describe('ShareSyncView 运行详情文件动作筛选', () => {
  const FACETS = {
    status_counts: { completed: 7200, failed: 157, skipped: 120, downloading: 7 },
    action_counts: { added: 7000, modified: 400, skipped: 84 },
  }
  const RUN_DETAIL = {
    id: 'run-9',
    started_at: 1,
    finished_at: 2,
    status: 'completed_with_errors',
    total_count: 7484,
    added_count: 7000,
    modified_count: 400,
    removed_count: 0,
    unchanged_count: 0,
    failed_count: 157,
    skipped_count: 84,
    overwritten_count: 400,
    error: null,
    items: [],
    item_total_count: 7484,
    item_page: 1,
    item_page_size: 100,
    item_facets: FACETS,
  }

  type Vm = {
    openRun: (id: string) => Promise<void>
    onRunItemsPageChange: (page: number) => void
    applyRunItemFilter: () => void
    runItemFilter: { status: string; action: string; q: string }
    runItemStatusOptions: Array<{ value: string; label: string }>
    runItemActionOptions: Array<{ value: string; label: string }>
    runItemsHeaderCount: string
    currentRun: { item_page: number; item_total_count: number } | null
  }

  async function mountAndOpenRun() {
    api.getRun.mockReset().mockResolvedValue(structuredClone(RUN_DETAIL))
    api.listRunItems.mockReset().mockImplementation(
        async (_id: string, page: number, pageSize: number, filter?: { status?: string }) => ({
          items: [],
          total: filter?.status === 'failed' ? 157 : 7484,
          page,
          page_size: pageSize,
          facets: FACETS,
        }),
    )
    const wrapper = mount(ShareSyncView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        // happy-dom 下 el-table 的 MutationObserver 不可用，表格本身不在本组测试范围
        stubs: {
          AccountFilter: true, AccountBadge: true, TransferDialog: true,
          ShareIncludeExcludeEditor: true, CronExprEditor: true, FilePickerModal: true,
          'el-table': true, 'el-table-column': true,
        },
      },
    })
    await flushPromises()
    const vm = wrapper.vm as unknown as Vm
    await vm.openRun('run-9')
    await flushPromises()
    return { wrapper, vm }
  }

  it('筛选选项显示整个 run 的分组计数，进行中合并未终态状态', async () => {
    const { wrapper, vm } = await mountAndOpenRun()
    const status = Object.fromEntries(vm.runItemStatusOptions.map((o) => [o.value, o.label]))
    expect(status).toEqual({
      all: '全部状态（7484）',
      failed: '失败（157）',
      completed: '完成（7200）',
      skipped: '跳过（120）',
      excluded: '已排除（0）',
      in_progress: '进行中（7）',
    })
    const action = Object.fromEntries(vm.runItemActionOptions.map((o) => [o.value, o.label]))
    expect(action).toEqual({
      all: '全部动作（7484）',
      added: '新增（7000）',
      modified: '修改（400）',
      removed: '删除（0）',
      skipped: '跳过（84）',
      excluded: '已排除（0）',
    })
    expect(vm.runItemsHeaderCount).toBe('7484')
    wrapper.unmount()
  })

  it('点「只看失败」按失败筛选、回到第 1 页，total 用筛选后的条数', async () => {
    const { wrapper, vm } = await mountAndOpenRun()
    vm.onRunItemsPageChange(40)
    await flushPromises()
    expect(vm.currentRun!.item_page).toBe(40)

    const link = document.body.querySelector<HTMLElement>('.run-failed-link')
    expect(link, '有失败项时应显示「只看失败」').toBeTruthy()
    link!.click()
    await flushPromises()

    expect(api.listRunItems).toHaveBeenLastCalledWith('run-9', 1, 100, { status: 'failed', action: 'all', q: '' })
    expect(vm.currentRun!.item_page).toBe(1)
    expect(vm.currentRun!.item_total_count).toBe(157)
    expect(vm.runItemsHeaderCount).toBe('筛选出 157 / 共 7484')
    // 翻页沿用当前筛选
    vm.onRunItemsPageChange(2)
    await flushPromises()
    expect(api.listRunItems).toHaveBeenLastCalledWith('run-9', 2, 100, { status: 'failed', action: 'all', q: '' })
    wrapper.unmount()
  })

  it('路径关键字输入防抖后查询，换 run 时筛选归零', async () => {
    vi.useFakeTimers()
    try {
      const { wrapper, vm } = await mountAndOpenRun()
      const input = document.body.querySelector<HTMLInputElement>('.run-item-keyword-filter input')
      expect(input).toBeTruthy()
      input!.value = 'S01'
      input!.dispatchEvent(new Event('input'))
      await flushPromises()
      expect(api.listRunItems).not.toHaveBeenCalled()
      vi.advanceTimersByTime(300)
      await flushPromises()
      expect(api.listRunItems).toHaveBeenCalledTimes(1)
      expect(api.listRunItems).toHaveBeenLastCalledWith('run-9', 1, 100, { status: 'all', action: 'all', q: 'S01' })

      await vm.openRun('run-9')
      await flushPromises()
      expect(vm.runItemFilter).toEqual({ status: 'all', action: 'all', q: '' })
      wrapper.unmount()
    } finally {
      vi.useRealTimers()
    }
  })

  it('慢的旧请求回包不覆盖新筛选结果', async () => {
    const { wrapper, vm } = await mountAndOpenRun()
    let resolveOld!: (v: unknown) => void
    api.listRunItems.mockImplementationOnce(() => new Promise((r) => { resolveOld = r }))
    vm.onRunItemsPageChange(5)
    vm.runItemFilter.status = 'failed'
    vm.applyRunItemFilter()
    await flushPromises()
    expect(vm.currentRun!.item_total_count).toBe(157)
    resolveOld({ items: [], total: 7484, page: 5, page_size: 100, facets: FACETS })
    await flushPromises()
    expect(vm.currentRun!.item_total_count).toBe(157)
    expect(vm.currentRun!.item_page).toBe(1)
    wrapper.unmount()
  })

  it('API 客户端只在有筛选时附加查询参数（未筛选时与旧请求一致）', async () => {
    const { rawApiClient } = await import('@/api/client')
    const actual = await vi.importActual<typeof import('@/api/shareSync')>('@/api/shareSync')
    const get = vi.spyOn(rawApiClient, 'get').mockResolvedValue({ data: { success: true, data: {} } })
    try {
      await actual.listRunItems('r', 2, 100)
      expect(get).toHaveBeenLastCalledWith('/share-sync/runs/r/items', { params: { page: 2, page_size: 100 } })
      await actual.listRunItems('r', 1, 100, { status: 'all', action: 'all', q: '  ' })
      expect(get).toHaveBeenLastCalledWith('/share-sync/runs/r/items', { params: { page: 1, page_size: 100 } })
      await actual.listRunItems('r', 1, 100, { status: 'failed', action: 'modified', q: ' 第1集 ' })
      expect(get).toHaveBeenLastCalledWith('/share-sync/runs/r/items', {
        params: { page: 1, page_size: 100, status: 'failed', action: 'modified', q: '第1集' },
      })
      await actual.getRun('r', 1, 100, { status: 'failed' })
      expect(get).toHaveBeenLastCalledWith('/share-sync/runs/r', {
        params: { page: 1, page_size: 100, status: 'failed' },
      })
    } finally {
      get.mockRestore()
    }
  })
})

describe('ShareSyncView 选中路径提交保真', () => {
  function editButtonOf(name: string): HTMLElement {
    const card = Array.from(document.body.querySelectorAll<HTMLElement>('.config-card'))
        .find((el) => el.textContent?.includes(name))
    const btn = Array.from((card ?? document.body).querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '编辑')
    expect(btn, `「${name}」应有编辑按钮`).toBeTruthy()
    return btn!
  }

  function saveButton(): HTMLElement {
    const btn = Array.from(document.body.querySelectorAll<HTMLElement>('button'))
        .find((b) => b.textContent?.trim() === '保存')
    expect(btn, '应有「保存」按钮').toBeTruthy()
    return btn!
  }

  type EditorSub = Omit<typeof SUB, 'include_paths' | 'exclude_paths' | 'targets'> & {
    include_paths: string[]
    exclude_paths: string[]
    targets: Array<Record<string, unknown>>
  }

  async function openEditor(sub: EditorSub) {
    api.listSubscriptions.mockResolvedValue([sub])
    api.updateSubscription.mockResolvedValue({ ...sub })
    const wrapper = mountView()
    await flushPromises()
    editButtonOf(sub.name).click()
    await flushPromises()
    return wrapper
  }

  it('保存时按原字节保留选中路径的文件名空白', async () => {
    const withSpaces = {
      ...SUB,
      include_paths: ['/剧集/报告. '],
      exclude_paths: ['/剧集/广告. '],
    }
    const wrapper = await openEditor(withSpaces)

    saveButton().click()
    await flushPromises()

    expect(api.updateSubscription).toHaveBeenCalledTimes(1)
    const req = api.updateSubscription.mock.calls[0][1] as {
      include_paths: string[]
      exclude_paths: string[]
    }
    expect(req.include_paths, '同步内容保留文件名结尾空格').toEqual(['/剧集/报告. '])
    expect(req.exclude_paths, '排除内容保留文件名结尾空格').toEqual(['/剧集/广告. '])
    wrapper.unmount()
  })

  it('仅结尾空格不同的两条选中路径都保留，不会被去重成一个', async () => {
    const pair = {
      ...SUB,
      include_paths: ['/剧集/报告.', '/剧集/报告. '],
      exclude_paths: [],
    }
    const wrapper = await openEditor(pair)

    saveButton().click()
    await flushPromises()

    const req = api.updateSubscription.mock.calls[0][1] as { include_paths: string[] }
    expect(req.include_paths, '仅结尾空格不同即两个文件，不能去重').toEqual([
      '/剧集/报告.',
      '/剧集/报告. ',
    ])
    wrapper.unmount()
  })

  it('选中路径只规整分隔符，手工输入的网盘目标仍按原规则 trim', async () => {
    const messy = {
      ...SUB,
      include_paths: ['/剧集//报告. //'],
      exclude_paths: ['//剧集/广告. '],
      targets: [
        { kind: 'netdisk' as const, remote_path: '/我的资源', save_fs_id: 0 },
        { kind: 'local' as const, local_path: '/tmp', mode: 'transfer_and_download' as const },
      ],
    }
    const wrapper = await openEditor(messy)

    const netdiskInput = document.body.querySelector<HTMLInputElement>(
      '.subscription-dialog .target-path input',
    )!
    netdiskInput.value = '  /我的资源/同步  '
    netdiskInput.dispatchEvent(new Event('input'))
    await flushPromises()

    saveButton().click()
    await flushPromises()

    const req = api.updateSubscription.mock.calls[0][1] as {
      include_paths: string[]
      exclude_paths: string[]
      targets: Array<Record<string, unknown>>
    }
    expect(req.include_paths, '只合并冗余斜杠，文件名空白不动').toEqual(['/剧集/报告. '])
    expect(req.exclude_paths).toEqual(['/剧集/广告. '])
    expect(req.targets[0].remote_path, '手工目标路径仍走 trim 归一').toBe('/我的资源/同步')
    wrapper.unmount()
  })
})

describe('ShareSyncView 运行详情显示命中的排除规则（#183）', () => {
  const EXCLUDED_ITEMS = [
    { id: 1, path: '/剧集/花絮', action: 'excluded', target: 'none', transfer_task_id: null, download_task_id: null,
      status: 'excluded', versioned_old_path: null, error: null, reason: 'excluded:glob:花絮' },
    { id: 2, path: '/剧集/a.txt', action: 'excluded', target: 'none', transfer_task_id: null, download_task_id: null,
      status: 'excluded', versioned_old_path: null, error: null, reason: 'excluded:regex:\\.txt$' },
    { id: 3, path: '/广告', action: 'excluded', target: 'none', transfer_task_id: null, download_task_id: null,
      status: 'excluded', versioned_old_path: null, error: null, reason: 'excluded:path:/广告' },
  ]
  const FACETS = {
    status_counts: { completed: 10, excluded: 3 },
    action_counts: { added: 10, excluded: 3 },
  }
  const RUN = {
    id: 'run-x', started_at: 1, finished_at: 2, status: 'completed', total_count: 10,
    added_count: 10, modified_count: 0, removed_count: 0, unchanged_count: 0, failed_count: 0,
    skipped_count: 0, overwritten_count: 0, error: null,
    items: EXCLUDED_ITEMS, item_total_count: 13, item_page: 1, item_page_size: 100, item_facets: FACETS,
  }

  type Vm = {
    openRun: (id: string) => Promise<void>
    runItemStatusOptions: Array<{ value: string; label: string }>
    runItemActionOptions: Array<{ value: string; label: string }>
    describeReason: (r: unknown, c: unknown, v: string | null | undefined) => string
    describeAction: (r: unknown, c: unknown, v: string) => string
    describeItemStatus: (r: unknown, c: unknown, v: string) => string
    describeTarget: (r: unknown, c: unknown, v: string) => string
  }

  async function open(run: Record<string, unknown>) {
    api.getRun.mockReset().mockResolvedValue(structuredClone(run))
    api.listRunItems.mockReset().mockResolvedValue({ items: [], total: 3, page: 1, page_size: 100, facets: FACETS })
    const wrapper = mount(ShareSyncView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        stubs: {
          AccountFilter: true, AccountBadge: true, TransferDialog: true,
          ShareIncludeExcludeEditor: true, CronExprEditor: true, FilePickerModal: true,
          'el-table': true, 'el-table-column': true,
        },
      },
    })
    await flushPromises()
    const vm = wrapper.vm as unknown as Vm
    await vm.openRun('run-x')
    await flushPromises()
    return { wrapper, vm }
  }

  it('已排除条目：动作 / 状态 / 目标 / 原因都汉化，原因写明命中的规则', async () => {
    const { wrapper, vm } = await open(RUN)
    expect(vm.describeAction(null, null, 'excluded')).toBe('已排除')
    expect(vm.describeItemStatus(null, null, 'excluded')).toBe('已排除')
    expect(vm.describeTarget(null, null, 'none')).toBe('—')
    expect(vm.describeTarget(null, null, 'netdisk')).toBe('网盘')
    expect(vm.describeTarget(null, null, 'netdisk@2002')).toBe('网盘→UID:2002')
    expect(vm.describeReason(null, null, 'excluded:glob:花絮')).toBe('命中通配符 花絮')
    expect(vm.describeReason(null, null, 'excluded:regex:\\.txt$')).toBe('命中正则 \\.txt$')
    expect(vm.describeReason(null, null, 'excluded:path:/广告')).toBe('命中排除内容 /广告')
    expect(vm.describeReason(null, null, 'excluded:glob:a:b')).toBe('命中通配符 a:b')
    // 原有原因不受影响
    expect(vm.describeReason(null, null, 'quota_full')).toBe('网盘空间不足')
    wrapper.unmount()
  })

  it('筛选下拉含「已排除」，计数来自分组统计', async () => {
    const { wrapper, vm } = await open(RUN)
    expect(vm.runItemStatusOptions.find((o) => o.value === 'excluded')?.label).toBe('已排除（3）')
    expect(vm.runItemActionOptions.find((o) => o.value === 'excluded')?.label).toBe('已排除（3）')
    wrapper.unmount()
  })

  it('详情显示已排除数，点击按已排除筛选；超出上限时注明只记录了前 N 条', async () => {
    const { wrapper } = await open({ ...RUN, excluded_count: 812, excluded_truncated: true })
    const link = document.body.querySelector<HTMLElement>('.run-excluded-link')
    expect(link?.textContent).toContain('812')
    expect(document.body.querySelector('.run-excluded-truncated')?.textContent).toContain('明细只记录了前 3 条')
    link!.click()
    await flushPromises()
    expect(api.listRunItems).toHaveBeenLastCalledWith('run-x', 1, 100, { status: 'excluded', action: 'all', q: '' })
    wrapper.unmount()
  })

  it('排除明细已被清理的旧 run：只显示计数和说明，不给查看链接', async () => {
    const { wrapper } = await open({
      ...RUN,
      excluded_count: 4,
      excluded_truncated: true,
      items: [],
      item_facets: { status_counts: { completed: 10 }, action_counts: { added: 10 } },
    })
    expect(document.body.querySelector('.run-excluded-link')).toBeNull()
    const note = document.body.querySelectorAll('.run-excluded-truncated')
    expect(note).toHaveLength(1)
    expect(note[0].textContent).toContain('明细已清理')
    wrapper.unmount()
  })

  it('老 run（没有 excluded_count）不显示已排除行', async () => {
    const { wrapper } = await open({ ...RUN, items: [], item_facets: { status_counts: { completed: 10 }, action_counts: { added: 10 } } })
    expect(document.body.querySelector('.run-excluded-link')).toBeNull()
    wrapper.unmount()
  })
})

describe('ShareSyncView 所属账号与网盘账号（#183）', () => {
  const SUB_X = {
    ...SUB,
    owner_uid: 1001,
    targets: [{ kind: 'netdisk', remote_path: '/转存', save_fs_id: 0, account_uid: 2002 }],
  }

  afterEach(() => {
    auth.hasMultipleAccounts = false
    auth.accounts = [{ uid: 1, username: 'u', nickname: null }]
  })

  function setAccounts() {
    auth.hasMultipleAccounts = true
    auth.accounts = [
      { uid: 1001, username: 'a', nickname: '账号A' },
      { uid: 2002, username: 'b', nickname: '账号B' },
    ]
  }

  function mountReal() {
    // AccountBadge 用真组件，验证「所属：」前缀
    return mount(ShareSyncView, {
      attachTo: document.body,
      global: {
        plugins: [ElementPlus],
        stubs: {
          AccountFilter: true, TransferDialog: true, ShareIncludeExcludeEditor: true,
          CronExprEditor: true, FilePickerModal: true,
        },
      },
    })
  }

  it('订阅卡片徽章写「所属：账号A」，路径写「网盘→账号B」（找不到账号时回退 UID）', async () => {
    setAccounts()
    api.listSubscriptions.mockResolvedValue([SUB_X])
    const wrapper = mountReal()
    await flushPromises()
    const card = document.body.querySelector<HTMLElement>('.config-card')!
    expect(card.querySelector('.task-account-badge')?.textContent).toContain('所属：账号A')
    expect(card.textContent).toContain('网盘→账号B')
    const vm = wrapper.vm as unknown as { describeTarget: (r: unknown, c: unknown, v: string) => string }
    expect(vm.describeTarget(null, null, 'local@2002')).toBe('本地→账号B')
    wrapper.unmount()

    auth.accounts = [{ uid: 1001, username: 'a', nickname: '账号A' }]
    const w2 = mountReal()
    await flushPromises()
    expect(document.body.querySelector('.config-card')!.textContent).toContain('网盘→UID:2002')
    w2.unmount()
  })

  it('编辑弹窗提示网盘账号只决定转存目的地，不改所属账号', async () => {
    setAccounts()
    api.listSubscriptions.mockResolvedValue([SUB_X])
    const wrapper = mountReal()
    await flushPromises()
    const card = document.body.querySelector<HTMLElement>('.config-card')!
    Array.from(card.querySelectorAll<HTMLElement>('button')).find((b) => b.textContent?.trim() === '编辑')!.click()
    await flushPromises()
    const hint = document.body.querySelector<HTMLElement>('.subscription-dialog .owner-hint')
    expect(hint, '应显示所属账号提示').toBeTruthy()
    expect(hint!.textContent).toContain('只决定转存到哪个账号的网盘')
    expect(hint!.textContent).toContain('所属：账号A')
    wrapper.unmount()
  })

  it('移动端根节点带 is-mobile（订阅卡片头部改为上下排列）', async () => {
    mobile.value = true
    const wrapper = mountView()
    await flushPromises()
    expect(document.body.querySelector('.share-sync-view')?.classList.contains('is-mobile')).toBe(true)
    wrapper.unmount()
    mobile.value = false
    const w2 = mountView()
    await flushPromises()
    expect(document.body.querySelector('.share-sync-view')?.classList.contains('is-mobile')).toBe(false)
    w2.unmount()
  })
})
