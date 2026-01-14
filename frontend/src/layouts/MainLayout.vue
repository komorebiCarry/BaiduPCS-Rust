<template>
  <el-container class="main-layout" :class="{ 'is-mobile': isMobile }">
    <!-- PC端侧边栏 -->
    <el-aside v-if="!isMobile" :width="isCollapse ? '64px' : '200px'" class="sidebar">
      <div class="logo">
        <el-icon :size="32" color="#409eff">
          <FolderOpened />
        </el-icon>
        <transition name="fade">
          <span v-if="!isCollapse" class="logo-text">网盘客户端</span>
        </transition>
      </div>

      <el-menu
          :default-active="activeMenu"
          :collapse="isCollapse"
          :collapse-transition="false"
          class="sidebar-menu"
          router
      >
        <el-menu-item index="/files">
          <el-icon><Files /></el-icon>
          <template #title>文件管理</template>
        </el-menu-item>

        <el-menu-item index="/downloads">
          <el-icon><Download /></el-icon>
          <template #title>下载管理</template>
        </el-menu-item>

        <el-menu-item index="/uploads">
          <el-icon><Upload /></el-icon>
          <template #title>上传管理</template>
        </el-menu-item>

        <el-menu-item index="/transfers">
          <el-icon><Share /></el-icon>
          <template #title>转存管理</template>
        </el-menu-item>

        <el-menu-item index="/autobackup">
          <el-icon><Refresh /></el-icon>
          <template #title>自动备份</template>
        </el-menu-item>

        <el-menu-item index="/settings">
          <el-icon><Setting /></el-icon>
          <template #title>系统设置</template>
        </el-menu-item>
      </el-menu>

      <div class="sidebar-footer">
        <el-button
            :icon="isCollapse ? Expand : Fold"
            circle
            @click="toggleCollapse"
        />
      </div>
    </el-aside>

    <!-- 移动端抽屉导航 -->
    <el-drawer
        v-model="drawerVisible"
        :with-header="false"
        direction="ltr"
        size="240px"
        class="mobile-drawer"
        :z-index="2000"
    >
      <div class="drawer-content">
        <div class="drawer-logo">
          <el-icon :size="32" color="#409eff">
            <FolderOpened />
          </el-icon>
          <span>网盘客户端</span>
        </div>

        <el-menu
            :default-active="activeMenu"
            :collapse-transition="false"
            class="drawer-menu"
            router
            @select="handleMenuSelect"
        >
          <el-menu-item index="/files">
            <el-icon><Files /></el-icon>
            <span>文件管理</span>
          </el-menu-item>

          <el-menu-item index="/downloads">
            <el-icon><Download /></el-icon>
            <span>下载管理</span>
          </el-menu-item>

          <el-menu-item index="/uploads">
            <el-icon><Upload /></el-icon>
            <span>上传管理</span>
          </el-menu-item>

          <el-menu-item index="/transfers">
            <el-icon><Share /></el-icon>
            <span>转存管理</span>
          </el-menu-item>

          <el-menu-item index="/autobackup">
            <el-icon><Refresh /></el-icon>
            <span>自动备份</span>
          </el-menu-item>

          <el-menu-item index="/settings">
            <el-icon><Setting /></el-icon>
            <span>系统设置</span>
          </el-menu-item>
        </el-menu>

        <!-- 抽屉底部用户信息 -->
        <div class="drawer-footer">
          <div class="drawer-user" @click="handleUserClick">
            <el-avatar :size="36" :src="userAvatar">
              <el-icon><User /></el-icon>
            </el-avatar>
            <span class="drawer-username">{{ username }}</span>
          </div>
          <el-button type="danger" plain size="small" @click="handleLogout">
            <el-icon><SwitchButton /></el-icon>
            退出
          </el-button>
        </div>
      </div>
    </el-drawer>

    <!-- 主内容区 -->
    <el-container class="main-container">
      <!-- 顶部栏 -->
      <el-header height="60px" class="top-header">
        <div class="header-left">
          <!-- 移动端菜单按钮 -->
          <el-button
              v-if="isMobile"
              :icon="Menu"
              circle
              class="mobile-menu-btn"
              @click="drawerVisible = true"
          />
          <h3>{{ pageTitle }}</h3>
        </div>

        <div class="header-right">
          <el-dropdown @command="handleCommand">
            <div class="user-info">
              <el-avatar :size="32" :src="userAvatar">
                <el-icon><User /></el-icon>
              </el-avatar>
              <span v-if="!isMobile" class="username">{{ username }}</span>
              <el-icon><CaretBottom /></el-icon>
            </div>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item command="profile">
                  <el-icon><User /></el-icon>
                  个人信息
                </el-dropdown-item>
                <el-dropdown-item command="logout" divided>
                  <el-icon><SwitchButton /></el-icon>
                  退出登录
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
        </div>
      </el-header>

      <!-- 内容区 -->
      <el-main class="main-content" :class="{ 'has-tabbar': isMobile }">
        <router-view v-slot="{ Component }">
          <transition name="fade-slide" mode="out-in">
            <component :is="Component" />
          </transition>
        </router-view>
      </el-main>
    </el-container>

    <!-- 移动端底部导航栏 -->
    <div v-if="isMobile" class="mobile-tabbar">
      <div
          v-for="item in tabbarItems"
          :key="item.path"
          class="tabbar-item"
          :class="{ active: activeMenu === item.path }"
          @click="navigateTo(item.path)"
      >
        <el-icon :size="22">
          <component :is="item.icon" />
        </el-icon>
        <span class="tabbar-label">{{ item.label }}</span>
      </div>
    </div>

    <!-- 个人信息弹窗 -->
    <UserProfileDialog v-model="profileDialogVisible" :user="authStore.user" />
  </el-container>
</template>

<script setup lang="ts">
import { ref, computed, watch, markRaw } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessageBox, ElMessage } from 'element-plus'
import { useAuthStore } from '@/stores/auth'
import { useIsMobile } from '@/utils/responsive'
import UserProfileDialog from '@/components/UserProfileDialog.vue'
import {
  FolderOpened,
  Files,
  Download,
  Upload,
  Setting,
  User,
  CaretBottom,
  SwitchButton,
  Expand,
  Fold,
  Share,
  Menu,
  Refresh,
} from '@element-plus/icons-vue'

const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()

// 响应式检测
const isMobile = useIsMobile()

// 状态
const isCollapse = ref(false)
const drawerVisible = ref(false)
const profileDialogVisible = ref(false)

// 底部导航栏配置
const tabbarItems = [
  { path: '/files', label: '文件', icon: markRaw(Files) },
  { path: '/downloads', label: '下载', icon: markRaw(Download) },
  { path: '/uploads', label: '上传', icon: markRaw(Upload) },
  { path: '/autobackup', label: '备份', icon: markRaw(Refresh) },
  { path: '/settings', label: '设置', icon: markRaw(Setting) },
]

// 计算属性
const activeMenu = computed(() => route.path)
const username = computed(() => authStore.username || '未登录')
const userAvatar = computed(() => authStore.avatar)

const pageTitle = computed(() => {
  const titles: Record<string, string> = {
    '/files': '文件管理',
    '/downloads': '下载管理',
    '/uploads': '上传管理',
    '/transfers': '转存管理',
    '/autobackup': '自动备份',
    '/settings': '系统设置',
  }
  return titles[route.path] || '百度网盘'
})

// 切换侧边栏折叠状态
function toggleCollapse() {
  isCollapse.value = !isCollapse.value
}

// 底部导航栏点击
function navigateTo(path: string) {
  router.push(path)
}

// 抽屉菜单选择（关闭抽屉）
function handleMenuSelect() {
  drawerVisible.value = false
}

// 抽屉用户点击
function handleUserClick() {
  profileDialogVisible.value = true
}

// 抽屉退出登录
async function handleLogout() {
  try {
    await ElMessageBox.confirm('确定要退出登录吗？', '退出确认', {
      confirmButtonText: '确定',
      cancelButtonText: '取消',
      type: 'warning',
    })
    drawerVisible.value = false
    await authStore.logout()
    ElMessage.success('已退出登录')
    router.push('/login')
  } catch (error) {
    if (error !== 'cancel') {
      console.error('退出登录失败:', error)
    }
  }
}

// 下拉菜单命令处理
async function handleCommand(command: string) {
  switch (command) {
    case 'profile':
      profileDialogVisible.value = true
      break
    case 'logout':
      await handleLogout()
      break
  }
}

// 监听路由变化，移动端自动关闭抽屉
watch(
    () => route.path,
    () => {
      if (isMobile.value) {
        drawerVisible.value = false
      }
    }
)
</script>

<style scoped lang="scss">
.main-layout {
  width: 100%;
  height: 100vh;
  display: flex;
  flex-direction: row;

  &.is-mobile {
    flex-direction: column;
  }
}

.main-container {
  flex: 1;
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.sidebar {
  display: flex;
  flex-direction: column;
  background: #304156;
  transition: width 0.3s;
  overflow: hidden;
  flex-shrink: 0;

  .logo {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    height: 60px;
    padding: 0 20px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);

    .logo-text {
      font-size: 18px;
      font-weight: 600;
      color: white;
      white-space: nowrap;
    }
  }

  .sidebar-menu {
    flex: 1;
    border-right: none;
    background: transparent;

    :deep(.el-menu-item) {
      color: rgba(255, 255, 255, 0.7);

      &:hover {
        background-color: rgba(255, 255, 255, 0.1) !important;
        color: white;
      }

      &.is-active {
        background-color: #409eff !important;
        color: white;
      }
    }
  }

  .sidebar-footer {
    display: flex;
    justify-content: center;
    padding: 20px 0;
    border-top: 1px solid rgba(255, 255, 255, 0.1);
  }
}

.top-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: white;
  border-bottom: 1px solid #e0e0e0;
  padding: 0 20px;
  flex-shrink: 0;

  .header-left {
    display: flex;
    align-items: center;
    gap: 12px;

    .mobile-menu-btn {
      flex-shrink: 0;
    }

    h3 {
      margin: 0;
      font-size: 18px;
      color: #333;
    }
  }

  .header-right {
    .user-info {
      display: flex;
      align-items: center;
      gap: 8px;
      cursor: pointer;
      padding: 8px 12px;
      border-radius: 8px;
      transition: background-color 0.2s;

      &:hover {
        background-color: #f5f5f5;
      }

      .username {
        font-size: 14px;
        color: #333;
      }
    }
  }
}

.main-content {
  padding: 0;
  background: #f5f5f5;
  overflow: hidden;
  flex: 1;

  // 移动端有底部导航栏时，增加底部内边距
  &.has-tabbar {
    padding-bottom: 0;
  }
}

// =====================
// 移动端抽屉导航样式
// =====================
.drawer-content {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: #304156;

  .drawer-logo {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    height: 60px;
    padding: 0 20px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    color: white;
    font-size: 18px;
    font-weight: 600;
  }

  .drawer-menu {
    flex: 1;
    border-right: none;
    background: transparent;

    :deep(.el-menu-item) {
      height: 56px;
      line-height: 56px;
      color: rgba(255, 255, 255, 0.7);
      font-size: 15px;

      .el-icon {
        margin-right: 12px;
      }

      &:hover {
        background-color: rgba(255, 255, 255, 0.1) !important;
        color: white;
      }

      &.is-active {
        background-color: #409eff !important;
        color: white;
      }
    }
  }

  .drawer-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 16px 20px;
    border-top: 1px solid rgba(255, 255, 255, 0.1);
    background: rgba(0, 0, 0, 0.1);

    .drawer-user {
      display: flex;
      align-items: center;
      gap: 12px;
      cursor: pointer;

      .drawer-username {
        color: white;
        font-size: 14px;
      }
    }
  }
}

// 抽屉全局样式覆盖
:global(.mobile-drawer) {
  .el-drawer__body {
    padding: 0 !important;
  }
}

// =====================
// 移动端底部导航栏样式
// =====================
.mobile-tabbar {
  display: flex;
  justify-content: space-around;
  align-items: center;
  height: 56px;
  background: white;
  border-top: 1px solid #e0e0e0;
  position: fixed;
  bottom: 0;
  left: 0;
  right: 0;
  z-index: 1000;
  // iOS 安全区域适配
  padding-bottom: env(safe-area-inset-bottom, 0);

  .tabbar-item {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    flex: 1;
    height: 100%;
    padding: 6px 0;
    color: #909399;
    cursor: pointer;
    transition: color 0.2s;
    // 最小触摸区域 44x44
    min-width: 44px;
    min-height: 44px;

    // 触摸反馈
    &:active {
      background-color: rgba(64, 158, 255, 0.1);
    }

    &.active {
      color: #409eff;

      .tabbar-label {
        font-weight: 600;
      }
    }

    .tabbar-label {
      font-size: 11px;
      margin-top: 2px;
    }
  }
}

// 移动端内容区底部留白（为底部导航栏留空间）
.is-mobile {
  .main-content {
    height: calc(100vh - 60px - 56px - env(safe-area-inset-bottom, 0));
  }
}

// =====================
// 过渡动画
// =====================
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.2s;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}

.fade-slide-enter-active,
.fade-slide-leave-active {
  transition: all 0.2s;
}

.fade-slide-enter-from {
  opacity: 0;
  transform: translateX(-10px);
}

.fade-slide-leave-to {
  opacity: 0;
  transform: translateX(10px);
}

// =====================
// 移动端响应式调整
// =====================
@media (max-width: 767px) {
  .top-header {
    padding: 0 12px;

    .header-left {
      h3 {
        font-size: 16px;
      }
    }
  }
}
</style>

