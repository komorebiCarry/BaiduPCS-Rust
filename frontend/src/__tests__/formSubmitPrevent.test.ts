/**
 * el-form 原生提交回归测试（源码级）
 *
 * 背景：el-form 渲染为原生 <form>，表单里只有一个文本输入框时，浏览器会把回车当作"隐式提交"，
 * 以 GET 方式提交到当前地址 —— 整页刷新、页面状态丢失（网盘"新建文件夹/重命名"回车后回到根目录且操作未执行）。
 * Element Plus 的 el-button 是 type="button"，不会被当作提交按钮，所以每个 el-form 都必须显式处理 submit：
 * 用 `@submit.prevent`，需要回车提交的表单写成 `@submit.prevent="handler"`。
 * happy-dom 不模拟隐式提交，这里以源码断言兜底；真实回车已用无头浏览器验证。
 *
 * 模板用 Vue 官方 SFC 解析器（vue/compiler-sfc）解析成 AST 再检查，不用正则切标签：
 * 属性值里出现 `>`（如 :rules="a > 0 ? x : y"）、多行标签、任意修饰符顺序都能正确识别。
 */
import { describe, it, expect } from 'vitest'
import { parse } from 'vue/compiler-sfc'

// Vite 在构建期把源码以字符串形式内联进来（不依赖 node:fs，vue-tsc 也无需 node 类型）
const SOURCES = import.meta.glob(['../**/*.vue', '!../**/__tests__/**'], {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>

// @vue/compiler-core 的 NodeTypes（compiler-sfc 未导出该枚举）
const ELEMENT = 1
const DIRECTIVE = 7

interface TemplateNode {
  type: number
  tag?: string
  props?: TemplateProp[]
  children?: TemplateNode[]
  loc: { start: { line: number; offset: number } }
}

interface TemplateProp {
  type: number
  name: string
  arg?: { content?: string } | null
  modifiers?: Array<string | { content: string }>
  loc: { end: { offset: number } }
}

interface ElFormTag {
  line: number
  /** 开始标签源码（空白折叠，用于报错展示与自检） */
  openTag: string
  props: TemplateProp[]
}

/** 解析 SFC 模板，返回所有 <el-form> / <ElForm> 元素（不含 el-form-item 等） */
function findElForms(source: string, filename = 'anonymous.vue'): ElFormTag[] {
  const { descriptor, errors } = parse(source, { filename })
  if (errors.length) throw new Error(`${filename} 解析失败: ${errors.map(String).join('; ')}`)
  const root = descriptor.template?.ast as TemplateNode | undefined
  const result: ElFormTag[] = []
  const walk = (node: TemplateNode) => {
    if (node.type === ELEMENT && (node.tag === 'el-form' || node.tag === 'ElForm')) {
      // 开始标签 = 元素起点到最后一个属性之后的第一个 `>`（属性值里的 `>` 已被跳过）
      const props = node.props ?? []
      const attrsEnd = Math.max(node.loc.start.offset, ...props.map((p) => p.loc.end.offset))
      const openTag = source.slice(node.loc.start.offset, source.indexOf('>', attrsEnd) + 1)
      result.push({ line: node.loc.start.line, openTag: openTag.replace(/\s+/g, ' '), props })
    }
    node.children?.forEach(walk)
  }
  if (root) walk(root)
  return result
}

/** 是否有 submit 监听且带 .prevent 修饰符（任意修饰符组合、@ 或 v-on: 写法、有无 handler 均可） */
function hasSubmitPrevent(form: Pick<ElFormTag, 'props'>): boolean {
  return form.props.some((p) =>
    p.type === DIRECTIVE &&
    p.name === 'on' &&
    p.arg?.content === 'submit' &&
    (p.modifiers ?? []).some((m) => (typeof m === 'string' ? m : m.content) === 'prevent'),
  )
}

/** submit 监听绑定的 handler 表达式（无则 null） */
function submitHandler(form: ElFormTag): string | null {
  const p = form.props.find((x) => x.type === DIRECTIVE && x.name === 'on' && x.arg?.content === 'submit') as
    (TemplateProp & { exp?: { content?: string } }) | undefined
  return p?.exp?.content?.trim() ?? null
}

const sfc = (template: string) => `<template>\n${template}\n</template>\n`

describe('el-form submit 检测器自检', () => {
  it('属性值含 > 时不截断标签', () => {
    const forms = findElForms(sfc('<el-form :rules="a > 0 ? x : y" @submit.prevent><el-input /></el-form>'))
    expect(forms).toHaveLength(1)
    expect(hasSubmitPrevent(forms[0])).toBe(true)
    const bad = findElForms(sfc('<el-form :rules="a > 0 ? x : y" class="c"><el-input /></el-form>'))
    expect(hasSubmitPrevent(bad[0])).toBe(false)
  })

  it('任意修饰符顺序 / v-on: 写法 / 有无 handler 都认可', () => {
    for (const attr of [
      '@submit.prevent',
      '@submit.stop.prevent',
      '@submit.prevent.stop',
      'v-on:submit.prevent',
      'v-on:submit.stop.prevent="go"',
      '@submit.prevent="go()"',
    ]) {
      const [form] = findElForms(sfc(`<el-form ${attr}></el-form>`))
      expect(hasSubmitPrevent(form), attr).toBe(true)
    }
  })

  it('没有 .prevent 的 submit 监听、其他事件的 .prevent 都不算', () => {
    for (const attr of ['@submit="go"', '@submit.stop', 'v-on:submit="go"', '@keydown.enter.prevent', '@click.prevent', 'data-submit-prevent']) {
      const [form] = findElForms(sfc(`<el-form ${attr}></el-form>`))
      expect(hasSubmitPrevent(form), attr).toBe(false)
    }
  })

  it('排除 el-form-item，识别嵌套、多行和 PascalCase 写法', () => {
    const forms = findElForms(sfc(`
      <div v-if="ok">
        <el-form
          :model="m"
          :label-width="w > 100 ? '120px' : '80px'"
          @submit.stop.prevent="save"
        >
          <el-form-item label="a"><el-input /></el-form-item>
        </el-form>
      </div>
      <ElForm><el-form-item /></ElForm>
    `))
    expect(forms.map((f) => f.line)).toEqual([4, 12])
    expect(forms[0].openTag).toBe(
      `<el-form :model="m" :label-width="w > 100 ? '120px' : '80px'" @submit.stop.prevent="save" >`,
    )
    expect(forms[1].openTag).toBe('<ElForm>')
    expect(hasSubmitPrevent(forms[0])).toBe(true)
    expect(submitHandler(forms[0])).toBe('save')
    expect(hasSubmitPrevent(forms[1])).toBe(false)
  })
})

describe('el-form submit', () => {
  it('源码收录正常', () => {
    expect(Object.keys(SOURCES).length).toBeGreaterThan(10)
    expect(SOURCES['../views/FilesView.vue']).toBeTruthy()
  })

  it('每个 el-form 都处理 submit（阻止回车整页刷新）', () => {
    const missing: string[] = []
    let total = 0
    for (const [path, src] of Object.entries(SOURCES)) {
      for (const form of findElForms(src, path)) {
        total++
        if (!hasSubmitPrevent(form)) missing.push(`${path}:${form.line}: ${form.openTag}`)
      }
    }
    expect(total).toBeGreaterThan(10)
    expect(missing).toEqual([])
  })

  it('网盘新建文件夹/重命名：回车经表单 submit 提交，输入框不再挂 keyup.enter', () => {
    const src = SOURCES['../views/FilesView.vue']
    const handlers = findElForms(src, 'FilesView.vue').map(submitHandler)
    expect(handlers).toContain('handleCreateFolder')
    expect(handlers).toContain('handleRenameConfirm')
    expect(src).not.toMatch(/@keyup\.enter="handle(CreateFolder|RenameConfirm)"/)
  })
})
