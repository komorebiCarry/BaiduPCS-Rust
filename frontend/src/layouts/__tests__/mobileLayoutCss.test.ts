/**
 * 移动端布局 CSS 回归测试（源码级）
 *
 * 背景：MainLayout 移动端 `.main-content` 曾写成
 *   height: calc(100vh - 60px - 56px - env(safe-area-inset-bottom, 0));
 * env() 回退值 `0` 无单位，代入后 calc(长度 - 数字) 在计算值阶段无效，整条 height 失效退回 auto，
 * 内容区被内容撑高并被外层 overflow:hidden 裁掉 —— 离线下载 / 分享管理 / 系统设置 / 下载管理 / 自动备份
 * 在手机竖屏下无法滚动。happy-dom 不做布局计算，这里以源码断言兜底；真实滚动已用无头浏览器 390x844 验证。
 */
import { describe, it, expect } from 'vitest'

// Vite 在构建期把源码以字符串形式内联进来（不依赖 node:fs，vue-tsc 也无需 node 类型）
const SOURCES = import.meta.glob(['../../**/*.vue', '../../**/*.scss', '../../**/*.css', '!../../**/__tests__/**'], {
  query: '?raw',
  import: 'default',
  eager: true,
}) as Record<string, string>

function sources(prefix = ''): [string, string][] {
  return Object.entries(SOURCES).filter(([path]) => path.startsWith(prefix))
}

function readMainLayoutStyle(): string {
  const src = SOURCES['../MainLayout.vue'] ?? SOURCES['../../layouts/MainLayout.vue']
  expect(src, 'MainLayout.vue 未被 glob 收录').toBeTruthy()
  return src.slice(src.indexOf('<style'))
}

describe('mobile layout css', () => {
  it('env() 的回退值必须带单位（无单位 0 会使 calc 整条声明失效）', () => {
    const offenders: string[] = []
    const all = sources()
    expect(all.length).toBeGreaterThan(20)
    for (const [file, src] of all) {
      src.split('\n').forEach((line: string, i: number) => {
        if (line.trim().startsWith('//')) return
        if (/env\(\s*[\w-]+\s*,\s*0\s*\)/.test(line)) offenders.push(`${file}:${i + 1}: ${line.trim()}`)
      })
    }
    expect(offenders).toEqual([])
  })

  it('MainLayout 移动端内容区高度扣除顶部栏、底部标签栏与安全区，且纵向 flex 链可收缩', () => {
    const style = readMainLayoutStyle()
    expect(style).toContain('height: calc(100vh - 60px - 56px - env(safe-area-inset-bottom, 0px));')
    expect(style).toContain('height: calc(100dvh - 60px - 56px - env(safe-area-inset-bottom, 0px));')
    expect(style).toMatch(/\.main-container\s*\{[^}]*min-height:\s*0;/)
    expect(style).toMatch(/\.main-content\s*\{[^}]*min-height:\s*0;/)
  })

  it('各页面不再自行用 100vh 硬编码移动端高度（统一由 .main-content 约束）', () => {
    const offenders: string[] = []
    const views = sources('../../views/')
    expect(views.length).toBeGreaterThan(5)
    for (const [file, src] of views) {
      if (/calc\(100vh - 60px - 56px/.test(src)) offenders.push(file)
    }
    expect(offenders).toEqual([])
  })
})
