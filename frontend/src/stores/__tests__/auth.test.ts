import { describe, expect, it, beforeEach } from 'vitest'
import { setActivePinia, createPinia } from 'pinia'
import { useAuthStore } from '../auth'
import type { AccountSummary } from '@/api/accounts'

function account(partial: Partial<AccountSummary>): AccountSummary {
  return {
    uid: 1,
    username: 'u1',
    nickname: null,
    avatar_url: null,
    vip_type: 0,
    is_active: false,
    ...partial,
  }
}

describe('authStore.activeProfile（个人信息弹窗数据源）', () => {
  beforeEach(() => setActivePinia(createPinia()))

  it('刷新页面后（旧 user 为空）仍能从账号摘要拿到用户名 / UID / 登录时间 / 空间', () => {
    const store = useAuthStore()
    store.accounts = [account({ uid: 42, username: 'cookie_user', is_active: true, login_time: 1700000000, total_space: 2048, used_space: 1024 })]
    store.activeUid = 42
    expect(store.user).toBeNull()
    expect(store.activeProfile).toMatchObject({ uid: 42, username: 'cookie_user', login_time: 1700000000, total_space: 2048, used_space: 1024 })
  })

  it('切换账号后跟随活跃账号，不再显示上一个登录账号的资料', () => {
    const store = useAuthStore()
    store.user = { uid: 1, username: 'qr_user', login_time: 1600000000 }
    store.accounts = [account({ uid: 1, username: 'qr_user', login_time: 1600000000 }), account({ uid: 2, username: 'cookie_user', login_time: 1700000000 })]
    store.activeUid = 2
    expect(store.activeProfile).toMatchObject({ uid: 2, username: 'cookie_user', login_time: 1700000000 })
  })

  it('旧后端摘要缺登录时间时，用同 UID 的 user 补齐', () => {
    const store = useAuthStore()
    store.user = { uid: 5, username: 'x', login_time: 1650000000, total_space: 10, used_space: 3 }
    store.accounts = [account({ uid: 5, username: 'x' })]
    store.activeUid = 5
    expect(store.activeProfile).toMatchObject({ uid: 5, login_time: 1650000000, total_space: 10, used_space: 3 })
  })

  it('无活跃账号时退回旧 user', () => {
    const store = useAuthStore()
    expect(store.activeProfile).toBeNull()
    store.user = { uid: 9, username: 'legacy', login_time: 1 }
    expect(store.activeProfile).toMatchObject({ uid: 9, username: 'legacy' })
  })
})
