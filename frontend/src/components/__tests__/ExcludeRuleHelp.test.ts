import { mount } from '@vue/test-utils'
import ElementPlus from 'element-plus'
import { describe, expect, it } from 'vitest'
import ExcludeRuleHelp from '../ExcludeRuleHelp.vue'

function mountHelp(props: Record<string, unknown> = {}) {
  return mount(ExcludeRuleHelp, {
    props,
    global: { plugins: [ElementPlus] },
  })
}

describe('ExcludeRuleHelp', () => {
  it('默认按通配符解释，文案含 * / ? 语义且不含正则提示', () => {
    const wrapper = mountHelp()
    const text = wrapper.text()
    expect(text).toContain('完整路径')
    expect(text).toContain('区分大小写')
    expect(text).toContain('匹配单个字符')
    expect(text).not.toContain('Rust')
    expect(text).not.toContain('lookaround')
    wrapper.unmount()
  })

  it('kind=regex 时说明 Rust 语法、限制与整路径示例', () => {
    const wrapper = mountHelp({ kind: 'regex' })
    const text = wrapper.text()
    expect(text).toContain('Rust 正则语法')
    expect(text).toContain('不支持 lookaround')
    expect(text).toContain('反向引用')
    expect(text).toContain('^/股票/按月归档/2025.*$')
    expect(text).toContain('全部内容')
    expect(text).toContain('彼此独立')
    // 正则说明不混入 glob 的 `?` 语义
    expect(text).not.toContain('匹配单个字符')
    wrapper.unmount()
  })

  it('glob 与 regex 说明都保持折叠前的纯文本块，只渲染一个根节点', () => {
    for (const kind of ['glob', 'regex'] as const) {
      const wrapper = mountHelp({ kind })
      expect(wrapper.findAll('.exclude-help')).toHaveLength(1)
      wrapper.unmount()
    }
  })
})
