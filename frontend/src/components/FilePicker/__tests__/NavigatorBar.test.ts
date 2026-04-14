import { mount } from '@vue/test-utils'
import { nextTick, ref } from 'vue'
import ElementPlus from 'element-plus'
import { describe, expect, it, vi } from 'vitest'
import NavigatorBar from '../NavigatorBar.vue'

vi.mock('@/utils/responsive', () => ({
  useIsMobile: () => ref(false),
}))

describe('NavigatorBar 可折叠搜索', () => {
  it('上传模式下搜索默认折叠，点击搜索图标后展开', async () => {
    const wrapper = mount(NavigatorBar, {
      attachTo: document.body,
      props: {
        currentPath: '/Users/cj',
        canGoBack: true,
        canGoForward: true,
        canGoUp: true,
        mode: 'upload',
      },
      global: {
        plugins: [ElementPlus],
      },
    })

    const searchBox = wrapper.find('.search-box')
    expect(searchBox.classes()).not.toContain('is-expanded')

    await searchBox.find('.search-action').trigger('click')
    await nextTick()

    expect(searchBox.classes()).toContain('is-expanded')
    expect(wrapper.find('.path-input-wrapper').exists()).toBe(true)

    wrapper.unmount()
  })

  it('下载模式下搜索默认展开', async () => {
    const wrapper = mount(NavigatorBar, {
      props: {
        currentPath: '/Users/cj',
        canGoBack: true,
        canGoForward: true,
        canGoUp: true,
        mode: 'download',
      },
      global: {
        plugins: [ElementPlus],
      },
    })

    const searchBox = wrapper.find('.search-box')
    expect(searchBox.classes()).toContain('is-expanded')

    wrapper.unmount()
  })

  it('有搜索内容时保持展开状态', async () => {
    const wrapper = mount(NavigatorBar, {
      attachTo: document.body,
      props: {
        currentPath: '/Users/cj',
        canGoBack: true,
        canGoForward: true,
        canGoUp: true,
        mode: 'upload',
      },
      global: {
        plugins: [ElementPlus],
      },
    })

    const searchBox = wrapper.find('.search-box')
    await searchBox.find('.search-action').trigger('click')
    await nextTick()

    const input = searchBox.find('input')
    await input.setValue('test')
    await nextTick()

    expect(searchBox.classes()).toContain('is-expanded')
    expect(searchBox.classes()).toContain('has-text')

    wrapper.unmount()
  })
})
