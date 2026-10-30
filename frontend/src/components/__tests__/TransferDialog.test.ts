import { flushPromises, mount } from '@vue/test-utils'
import { ref } from 'vue'
import ElementPlus from 'element-plus'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import TransferDialog from '../TransferDialog.vue'

const transfer = vi.hoisted(() => ({
  createTransfer: vi.fn(),
  previewShareFiles: vi.fn(),
}))

vi.mock('@/api/transfer', () => ({
  createTransfer: transfer.createTransfer,
  previewShareFiles: transfer.previewShareFiles,
  TransferErrorCodes: {
    NEED_PASSWORD: 1001,
    INVALID_PASSWORD: 1002,
    SHARE_EXPIRED: 1003,
    SHARE_NOT_FOUND: 1004,
    MANAGER_NOT_READY: 1005,
    TASK_NOT_FOUND: 1006,
    INSUFFICIENT_SPACE: 1007,
    TRANSFER_FAILED: 1008,
    DOWNLOAD_FAILED: 1009,
  },
}))

const config = vi.hoisted(() => ({
  getTransferConfig: vi.fn(),
  getConfig: vi.fn(),
}))

vi.mock('@/api/config', () => ({
  getTransferConfig: config.getTransferConfig,
  getConfig: config.getConfig,
  updateRecentDirDebounced: vi.fn(),
  setDefaultDownloadDir: vi.fn(),
}))

// 订阅创建与规则校验：正则列表只经后者把关
const shareSync = vi.hoisted(() => ({
  createSubscription: vi.fn(),
  validateRules: vi.fn(),
}))

vi.mock('@/api/shareSync', () => ({
  createSubscription: shareSync.createSubscription,
  getExcludeRuleCapabilities: vi.fn(async () => ({
    supports_regex: true,
    regex_limits: { max_rules: 64, max_rule_bytes: 1024, max_total_bytes: 32768, max_test_path_bytes: 4096 },
  })),
  validateExcludeRules: shareSync.validateRules,
}))

// 可变的 auth 桩：跨账号用例打开多账号
const authState = vi.hoisted(() => ({ activeUid: 1, hasMultipleAccounts: false }))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => authState,
}))

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(false),
}))

const TARGET_ITEM = { fs_id: 1, is_dir: false, path: '/a.txt', size: 10, name: 'a.txt' }

async function mountDialog(lockKeepSync = true) {
  // 先关闭再打开，触发 el-dialog 的 open 钩子（handleOpen 初始化表单与 keepSync）
  const wrapper = mount(TransferDialog, {
    props: { modelValue: false, lockKeepSync },
    attachTo: document.body,
    global: {
      plugins: [ElementPlus],
      stubs: {
        NetdiskPathSelector: true,
        ShareFileSelector: true,
        CronExprEditor: true,
        FilePickerModal: true,
        // 账号选择桩：点击切到 uid=2
        AccountSelect: {
          name: 'AccountSelect',
          props: ['modelValue'],
          emits: ['update:modelValue'],
          template: `<button class="pick-account-2" @click="$emit('update:modelValue', 2)">acc-{{ modelValue }}</button>`,
        },
        // 编辑器桩：暴露 validateRules，并提供按钮触发正则模型更新
        ShareIncludeExcludeEditor: {
          name: 'ShareIncludeExcludeEditor',
          props: [
            'shareUrl', 'password', 'includePaths', 'excludePatterns',
            'excludeRegexPatterns', 'excludePaths', 'ownerUid', 'ownerLoggedIn',
          ],
          emits: ['update:includePaths', 'update:excludePatterns', 'update:excludeRegexPatterns', 'update:excludePaths'],
          template: `
            <div class="editor-stub">
              <button class="emit-regex" @click="$emit('update:excludeRegexPatterns', ['^/股票/按月归档/2025.*$'])">set-regex</button>
              <button class="emit-glob" @click="$emit('update:excludePatterns', ['*.tmp'])">set-glob</button>
            </div>
          `,
          setup(_props: unknown, { expose }: { expose: (e: Record<string, unknown>) => void }) {
            expose({ validateRules: shareSync.validateRules })
          },
        },
      },
    },
  })
  await wrapper.setProps({ modelValue: true })
  await flushPromises()
  return wrapper
}

/** 输入步骤里唯一的分享链接输入框 */
function shareUrlInput(): HTMLInputElement {
  return document.body.querySelector<HTMLInputElement>(
      '.el-dialog input[placeholder="请粘贴百度网盘分享链接"]',
  )!
}

function createButton(): HTMLButtonElement {
  const btn = Array.from(document.body.querySelectorAll<HTMLButtonElement>('button'))
      .find((b) => b.textContent?.trim() === '创建同步订阅')
  expect(btn, '应有「创建同步订阅」按钮').toBeTruthy()
  return btn!
}

beforeEach(() => {
  authState.activeUid = 1
  authState.hasMultipleAccounts = false
  transfer.createTransfer.mockReset().mockResolvedValue({ task_id: 'task-1' })
  config.getTransferConfig.mockReset().mockResolvedValue({
    recent_save_path: '/我的资源',
    recent_save_fs_id: 0,
    default_behavior: 'transfer_only',
  })
  config.getConfig.mockReset().mockResolvedValue({
    download: { download_dir: '/tmp', ask_each_time: false },
    share_sync: { exclude_preset_rules: [], exclude_regex_preset_rules: [] },
  })
  shareSync.createSubscription.mockReset().mockResolvedValue({ id: 'sub-new' })
  shareSync.validateRules.mockReset().mockResolvedValue({ ok: true })
  transfer.previewShareFiles.mockReset().mockResolvedValue({ files: [TARGET_ITEM], share_info: null })
})

afterEach(() => {
  document.body.innerHTML = ''
})

describe('TransferDialog 订阅创建与正则排除', () => {
  it('正则模型随创建请求一起提交，通配符与正则互不混淆', async () => {
    const wrapper = await mountDialog()
    await flushPromises()

    shareUrlInput().value = 'https://pan.baidu.com/s/1y7CluAbCdEfGh'
    shareUrlInput().dispatchEvent(new Event('input'))
    await flushPromises()

    document.body.querySelector<HTMLElement>('.emit-glob')!.click()
    document.body.querySelector<HTMLElement>('.emit-regex')!.click()
    await flushPromises()
    expect(shareSync.validateRules).not.toHaveBeenCalled()

    createButton().click()
    await flushPromises()

    expect(shareSync.validateRules).toHaveBeenCalledTimes(1)
    expect(shareSync.createSubscription).toHaveBeenCalledTimes(1)
    const req = shareSync.createSubscription.mock.calls[0][0] as {
      exclude_patterns: string[]
      exclude_regex_patterns: string[]
    }
    expect(req.exclude_patterns).toEqual(['*.tmp'])
    expect(req.exclude_regex_patterns).toEqual(['^/股票/按月归档/2025.*$'])
    wrapper.unmount()
  })

  it('未设置正则时请求仍带空正则列表（不静默省略字段）', async () => {
    const wrapper = await mountDialog()
    await flushPromises()

    shareUrlInput().value = 'https://pan.baidu.com/s/1y7CluAbCdEfGh'
    shareUrlInput().dispatchEvent(new Event('input'))
    await flushPromises()
    createButton().click()
    await flushPromises()

    const req = shareSync.createSubscription.mock.calls[0][0] as {
      exclude_regex_patterns: string[]
    }
    expect(req.exclude_regex_patterns).toEqual([])
    wrapper.unmount()
  })

  it('正则校验失败时阻止创建并保留提示', async () => {
    shareSync.validateRules.mockResolvedValueOnce({
      ok: false,
      message: '正则第 1 条：无法编译正则',
    })
    const wrapper = await mountDialog()
    await flushPromises()

    shareUrlInput().value = 'https://pan.baidu.com/s/1y7CluAbCdEfGh'
    shareUrlInput().dispatchEvent(new Event('input'))
    await flushPromises()
    document.body.querySelector<HTMLElement>('.emit-regex')!.click()
    await flushPromises()

    createButton().click()
    await flushPromises()

    expect(shareSync.validateRules).toHaveBeenCalled()
    expect(shareSync.createSubscription, '校验失败不得创建订阅').not.toHaveBeenCalled()
    expect(document.body.textContent).toContain('无法编译正则')
    wrapper.unmount()
  })

  it('重新打开对话框时清空正则草稿', async () => {
    const wrapper = await mountDialog()
    await flushPromises()

    shareUrlInput().value = 'https://pan.baidu.com/s/1y7CluAbCdEfGh'
    shareUrlInput().dispatchEvent(new Event('input'))
    await flushPromises()
    document.body.querySelector<HTMLElement>('.emit-regex')!.click()
    await flushPromises()

    await wrapper.setProps({ modelValue: false })
    await flushPromises()
    await wrapper.setProps({ modelValue: true })
    await flushPromises()

    createButton().click()
    await flushPromises()
    const req = shareSync.createSubscription.mock.calls[0][0] as {
      exclude_regex_patterns: string[]
    }
    expect(req.exclude_regex_patterns, '重开不得残留上次的正则').toEqual([])
    wrapper.unmount()
  })
})

describe('TransferDialog 跨账号保存', () => {
  function findButton(text: string): HTMLButtonElement {
    const btn = Array.from(document.body.querySelectorAll<HTMLButtonElement>('button'))
        .find((b) => b.textContent?.trim() === text)
    expect(btn, `应有「${text}」按钮`).toBeTruthy()
    return btn!
  }

  it('单账号时不显示目标账号选择，订阅归属活跃账号', async () => {
    const wrapper = await mountDialog()
    expect(document.body.querySelector('.pick-account-2')).toBeNull()
    shareUrlInput().value = 'https://pan.baidu.com/s/1y7CluAbCdEfGh'
    shareUrlInput().dispatchEvent(new Event('input'))
    await flushPromises()
    createButton().click()
    await flushPromises()
    const req = shareSync.createSubscription.mock.calls[0][0] as { owner_uid?: number }
    expect(req.owner_uid).toBe(1)
    wrapper.unmount()
  })

  it('选择其他账号后订阅归属该账号，目录选择器浏览该账号且保存位置重置为根目录', async () => {
    authState.hasMultipleAccounts = true
    config.getTransferConfig.mockResolvedValue({
      recent_save_path: '/我的资源',
      recent_save_fs_id: 42,
      default_behavior: 'transfer_only',
    })
    const wrapper = await mountDialog()
    const selector = wrapper.findComponent({ name: 'NetdiskPathSelector' })
    expect(selector.props('modelValue')).toBe('/我的资源')

    document.body.querySelector<HTMLElement>('.pick-account-2')!.click()
    await flushPromises()

    const selectorAfter = wrapper.findComponent({ name: 'NetdiskPathSelector' })
    expect(selectorAfter.props('uid')).toBe(2)
    expect(selectorAfter.props('modelValue'), '最近目录属于活跃账号，跨账号应回到根目录').toBe('/')
    expect(wrapper.findComponent({ name: 'ShareIncludeExcludeEditor' }).props('ownerUid')).toBe(2)

    shareUrlInput().value = 'https://pan.baidu.com/s/1y7CluAbCdEfGh'
    shareUrlInput().dispatchEvent(new Event('input'))
    await flushPromises()
    createButton().click()
    await flushPromises()
    const req = shareSync.createSubscription.mock.calls[0][0] as {
      owner_uid?: number
      targets: { kind: string; remote_path?: string }[]
    }
    expect(req.owner_uid).toBe(2)
    expect(req.targets[0]).toMatchObject({ kind: 'netdisk', remote_path: '/' })
    wrapper.unmount()
  })

  it('一次性转存带上目标账号 owner_uid', async () => {
    authState.hasMultipleAccounts = true
    const wrapper = await mountDialog(false)
    document.body.querySelector<HTMLElement>('.pick-account-2')!.click()
    await flushPromises()

    shareUrlInput().value = 'https://pan.baidu.com/s/1y7CluAbCdEfGh'
    shareUrlInput().dispatchEvent(new Event('input'))
    await flushPromises()
    findButton('转存全部').click()
    await flushPromises()

    expect(transfer.createTransfer).toHaveBeenCalledTimes(1)
    const req = transfer.createTransfer.mock.calls[0][0] as { owner_uid?: number; save_path?: string }
    expect(req.owner_uid).toBe(2)
    expect(req.save_path).toBe('/')
    wrapper.unmount()
  })
})
