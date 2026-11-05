// 直接读后端源码里的常量做逐字比对，避免两边默认值悄悄分叉
import backendConfigRs from '../../../../backend/src/notification/config.rs?raw'
import { describe, expect, it } from 'vitest'
import {
  DEFAULT_SHARE_SYNC_FILE_LIST_LIMIT,
  DEFAULT_WEBHOOK_BODY_TEMPLATE,
  MAX_SHARE_SYNC_FILE_LIST_LIMIT,
  defaultNotificationConfig,
  normalizeNotificationConfig,
} from '../config'

function rustConst(name: string, pattern: RegExp): string {
  const m = pattern.exec(backendConfigRs)
  expect(m, `后端应定义 ${name}`).toBeTruthy()
  return m![1].replace(/\r\n/g, '\n')
}

describe('通知配置与后端保持一致', () => {
  it('默认 Webhook 模板逐字一致，并包含 {{files}}', () => {
    const backend = rustConst(
      'DEFAULT_WEBHOOK_BODY_TEMPLATE',
      /pub const DEFAULT_WEBHOOK_BODY_TEMPLATE: &str = r#"([\s\S]*?)"#;/,
    )
    expect(DEFAULT_WEBHOOK_BODY_TEMPLATE).toBe(backend)
    expect(DEFAULT_WEBHOOK_BODY_TEMPLATE).toContain('"files": "{{files}}"')
  })

  it('列出文件数的默认值 / 上限与后端一致', () => {
    expect(String(DEFAULT_SHARE_SYNC_FILE_LIST_LIMIT)).toBe(
      rustConst('DEFAULT_SHARE_SYNC_FILE_LIST_LIMIT', /pub const DEFAULT_SHARE_SYNC_FILE_LIST_LIMIT: u32 = (\d+);/),
    )
    expect(String(MAX_SHARE_SYNC_FILE_LIST_LIMIT)).toBe(
      rustConst('MAX_SHARE_SYNC_FILE_LIST_LIMIT', /pub const MAX_SHARE_SYNC_FILE_LIST_LIMIT: u32 = (\d+);/),
    )
    expect(DEFAULT_SHARE_SYNC_FILE_LIST_LIMIT).toBe(5)
  })

  it('旧后端缺字段时补默认值，0（不列文件）原样保留', () => {
    expect(defaultNotificationConfig().share_sync_file_list_limit).toBe(5)
    expect(normalizeNotificationConfig({ enabled: true }).share_sync_file_list_limit).toBe(5)
    expect(normalizeNotificationConfig({ share_sync_file_list_limit: 0 }).share_sync_file_list_limit).toBe(0)
    expect(normalizeNotificationConfig({ share_sync_file_list_limit: 12 }).share_sync_file_list_limit).toBe(12)
  })
})
