import { createRouter, createWebHistory } from 'vue-router'
import type { RouteRecordRaw } from 'vue-router'
import { useAuthStore } from '@/stores/auth'

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    redirect: '/login'
  },
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/LoginView.vue'),
    meta: { title: '登录' }
  },
  {
    path: '/',
    component: () => import('@/layouts/MainLayout.vue'),
    meta: { requiresAuth: true },
    children: [
      {
        path: '/files',
        name: 'Files',
        component: () => import('@/views/FilesView.vue'),
        meta: { title: '文件管理' }
      },
      {
        path: '/downloads',
        name: 'Downloads',
        component: () => import('@/views/DownloadsView.vue'),
        meta: { title: '下载管理' }
      },
      {
        path: '/settings',
        name: 'Settings',
        component: () => import('@/views/SettingsView.vue'),
        meta: { title: '系统设置' }
      }
    ]
  }
]

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes
})

// 路由守卫
router.beforeEach(async (to, _from, next) => {
  const authStore = useAuthStore()

  // 设置页面标题
  if (to.meta.title) {
    document.title = `${to.meta.title} - 百度网盘 Rust 客户端`
  }

  // 如果访问登录页，检查是否已登录
  if (to.path === '/login') {
    // 如果未登录，先尝试获取用户信息
    if (!authStore.isLoggedIn) {
      try {
        await authStore.fetchUserInfo()
        // 获取成功，说明已登录，重定向到文件页
        console.log('✅ 已登录，重定向到文件页')
        next('/files')
        return
      } catch (error) {
        // 获取失败，说明未登录，允许访问登录页
        console.log('❌ 未登录，显示登录页')
        next()
        return
      }
    } else {
      // 已经登录，重定向到文件页
      console.log('✅ 已登录（缓存），重定向到文件页')
      next('/files')
      return
    }
  }

  // 检查是否需要认证
  if (to.meta.requiresAuth) {
    // 如果未登录，尝试从后端获取用户信息
    if (!authStore.isLoggedIn) {
      try {
        await authStore.fetchUserInfo()
        next()
      } catch (error) {
        // 获取失败，跳转到登录页
        next('/login')
      }
    } else {
      next()
    }
  } else {
    // 不需要认证的页面直接放行
    next()
  }
})

export default router

