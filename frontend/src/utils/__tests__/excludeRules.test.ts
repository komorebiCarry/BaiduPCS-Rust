import { describe, expect, it } from 'vitest'
import {
  describeRuleError,
  describeRuleErrorFrom,
  mergeRules,
  normalizeRuleList,
  parseRuleInput,
  ruleKindLabel,
  sameRuleList,
} from '../excludeRules'

describe('excludeRules 输入解析', () => {
  it('按换行拆分并保留行内标点', () => {
    expect(parseRuleInput('*.tmp\n*第1，2集*\r\n\n  *.nfo  ')).toEqual([
      '*.tmp',
      '*第1，2集*',
      '*.nfo',
    ])
    expect(parseRuleInput('  ')).toEqual([])
  })

  it('合并去重且保序、不修改入参', () => {
    const existing = ['*.tmp']
    const merged = mergeRules(existing, ['*.bak', ' *.tmp ', ''])
    expect(merged).toEqual(['*.tmp', '*.bak'])
    expect(existing).toEqual(['*.tmp'])
    expect(merged).not.toBe(existing)
  })

  it('缺字段的旧响应归一为空列表', () => {
    expect(normalizeRuleList(undefined)).toEqual([])
    expect(normalizeRuleList(null)).toEqual([])
    expect(normalizeRuleList(['^/a$', ' ^/a$ ', ''])).toEqual(['^/a$'])
  })

  it('列表比较用于丢弃过期异步结果', () => {
    expect(sameRuleList(['a', 'b'], ['a', 'b'])).toBe(true)
    expect(sameRuleList(['a', 'b'], ['b', 'a'])).toBe(false)
    expect(sameRuleList(['a'], ['a', 'b'])).toBe(false)
    expect(sameRuleList([], [])).toBe(true)
  })

  it('类型展示名与默认语义', () => {
    expect(ruleKindLabel('glob')).toBe('通配符')
    expect(ruleKindLabel('regex')).toBe('正则表达式')
  })

  it('错误文案按 1 起行号展示', () => {
    expect(describeRuleError({ kind: 'regex', index: 0, message: '无法编译正则' }))
        .toBe('正则第 1 条：无法编译正则')
    expect(describeRuleError({ kind: 'regex', index: 2, message: '不能为空' }))
        .toBe('正则第 3 条：不能为空')
    expect(describeRuleError({ kind: 'rules', message: '最多 64 条' }))
        .toBe('规则最多 64 条')
    expect(describeRuleError({ kind: 'path', message: '路径过长' }))
        .toBe('测试路径路径过长')
  })

  it('新增批次错误换算成输入框行号，非新增错误保留整表下标', () => {
    expect(describeRuleErrorFrom({ kind: 'regex', index: 3, message: '无法编译正则' }, 'regex', 2))
        .toBe('正则本次新增第 2 条：无法编译正则')
    // 已存在规则自身的错误仍用整份列表下标
    expect(describeRuleErrorFrom({ kind: 'regex', index: 0, message: '不能为空' }, 'regex', 2))
        .toBe('正则第 1 条：不能为空')
    // 另一类规则的错误不换算
    expect(describeRuleErrorFrom({ kind: 'glob', index: 4, message: '过长' }, 'regex', 2))
        .toBe('通配符第 5 条：过长')
  })
})
