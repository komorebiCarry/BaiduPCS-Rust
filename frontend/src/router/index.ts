import { createRouter, createWebHistory } from 'vue-router'
import type { RouteRecordRaw } from 'vue-router'
import { useAuthStore } from '@/stores/auth'
import { useWebAuthStore } from '@/stores/webAuth'

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    redirect: '/login'
  },
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/LoginView.vue'),
    meta: { title: '登录', requiresBaiduAuth: true }
  },
  {
    path: '/web-login',
    name: 'WebLogin',
    component: () => import('@/views/WebLoginView.vue'),
    meta: { title: 'Web 认证登录', skipWebAuth: true }
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
        meta: { title: '网盘管理' }
      },
      {
        path: '/local',
        name: 'LocalFiles',
        component: () => import('@/views/LocalFilesView.vue'),
        meta: { title: '本地文件' }
      },
      {
        path: '/downloads',
        name: 'Downloads',
        component: () => import('@/views/DownloadsView.vue'),
        meta: { title: '下载管理' }
      },
      {
        path: '/uploads',
        name: 'Uploads',
        component: () => import('@/views/UploadsView.vue'),
        meta: { title: '上传管理' }
      },
      {
        path: '/transfers',
        name: 'Transfers',
        component: () => import('@/views/TransfersView.vue'),
        meta: { title: '转存管理' }
      },
      {
        path: '/autobackup',
        name: 'AutoBackup',
        component: () => import('@/views/AutoBackupView.vue'),
        meta: { title: '自动备份' }
      },
      {
        path: '/share-sync',
        name: 'ShareSync',
        component: () => import('@/views/ShareSyncView.vue'),
        meta: { title: '分享同步' }
      },
      {
        path: '/cloud-dl',
        name: 'CloudDl',
        component: () => import('@/views/CloudDlView.vue'),
        meta: { title: '离线下载' }
      },
      {
        path: '/shares',
        name: 'Shares',
        component: () => import('@/views/SharesView.vue'),
        meta: { title: '分享管理' }
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

/**
 * 路由守卫
 *
 * 认证流程说明：
 * 1. Web 认证（可选）：用于保护整个 Web 界面的访问，防止未授权用户访问管理界面
 * 2. 百度账号认证：用于访问百度网盘 API，需要登录百度账号
 *
 * 安全说明：
 * - 真正的安全保障在后端中间件，它会验证每个 API 请求的令牌
 * - 前端路由守卫只是用户体验优化
 * - 如果用户未认证，后端会返回 419，前端会自动跳转到登录页
 */

// 标记是否已初始化 Web 认证
let webAuthInitialized = false
// 标记是否已检查过百度登录状态
let baiduAuthChecked = false

router.beforeEach(async (to, _from, next) => {
  const authStore = useAuthStore()
  const webAuthStore = useWebAuthStore()

  // 设置页面标题
  if (to.meta.title) {
    document.title = `${to.meta.title} - 百度网盘 Rust 客户端`
  }

  // ========== 第一层：Web 认证检查 ==========
  // 跳过 Web 认证的页面（如 Web 登录页本身）
  if (to.meta.skipWebAuth) {
    next()
    return
  }

  // 只在首次访问时初始化 Web 认证
  if (!webAuthInitialized) {
    webAuthInitialized = true
    // 异步初始化，不阻塞路由
    webAuthStore.initialize().then(() => {
      webAuthStore.checkAuthStatus().catch(err => {
        console.error('获取 Web 认证状态失败:', err)
      })
    })
  }

  // 如果已经知道需要认证且未通过，重定向到 Web 登录页
  if (webAuthStore.authConfig && webAuthStore.isAuthEnabled && !webAuthStore.isAuthenticated) {
    next('/web-login')
    return
  }

  // ========== 第二层：百度账号认证检查（多账号版） ==========
  //
  // 多账号语义补丁：
  //   1. 首次进入任意页面时优先 `fetchAccountList()` 拉取全量账号 + active_uid，
  //      作为新的"是否已登录"判定来源；
  //      - 成功且 active_uid != null   → 视为已登录
  //      - 成功但 active_uid == null   → 视为未登录（账号列表可能为空）
  //      - 网络失败                     → fallback 到旧路径 `fetchUserInfo()`
  //   2. `/login?mode=add` 即使已登录也允许进入（添加新账号场景）；
  //      其它情况下访问 /login 且已登录则跳 /files。

  if (to.meta.requiresBaiduAuth || to.path === '/login') {
    // 添加账号：保留登录页可访问性
    if (to.path === '/login' && to.query.mode === 'add') {
      next()
      return
    }

    // 已确认登录态 → 直接跳 /files
    if (authStore.isLoggedIn) {
      next('/files')
      return
    }

    // 首次检查
    if (!baiduAuthChecked) {
      baiduAuthChecked = true
      try {
        await authStore.fetchAccountList()
        if (authStore.activeUid !== null) {
          next('/files')
          return
        }
        // 账号列表为空 / 无活跃账号 → 显示登录页
        next()
        return
      } catch (err) {
        console.warn('[router] fetchAccountList 失败，fallback 到 fetchUserInfo:', err)
        try {
          await authStore.fetchUserInfo()
          next('/files')
          return
        } catch {
          next()
          return
        }
      }
    }
    // 已检查过且未登录
    next()
    return
  }

  // 受保护页面
  if (to.meta.requiresAuth) {
    // 已登录直接放行；首次进入也需要拉一次账号列表（用于 UI 渲染账号切换器/chip）
    if (authStore.isLoggedIn) {
      if (!authStore.accountsLoaded) {
        // 后台异步拉取，不阻塞导航；失败仅打日志
        authStore.fetchAccountList().catch((err) => {
          console.warn('[router] 后台拉取账号列表失败:', err)
        })
      }
      next()
      return
    }

    // 未确认登录态：首次进入受保护页面时尝试 fetchAccountList
    if (!baiduAuthChecked) {
      baiduAuthChecked = true
      try {
        await authStore.fetchAccountList()
        if (authStore.activeUid !== null) {
          next()
          return
        }
      } catch (err) {
        console.warn('[router] fetchAccountList 失败，fallback 到 fetchUserInfo:', err)
        try {
          await authStore.fetchUserInfo()
          next()
          return
        } catch {
          /* fallthrough → /login */
        }
      }
    }
    next('/login')
    return
  }

  next()
})

export default router

