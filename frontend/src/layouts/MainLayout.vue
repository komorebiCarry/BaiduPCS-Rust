<template>
  <el-container class="main-layout">
    <!-- 侧边栏 -->
    <el-aside :width="isCollapse ? '64px' : '200px'" class="sidebar">
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

    <!-- 主内容区 -->
    <el-container>
      <!-- 顶部栏 -->
      <el-header height="60px" class="top-header">
        <div class="header-left">
          <h3>{{ pageTitle }}</h3>
        </div>

        <div class="header-right">
          <el-dropdown @command="handleCommand">
            <div class="user-info">
              <el-avatar :size="32" :src="userAvatar">
                <el-icon><User /></el-icon>
              </el-avatar>
              <span class="username">{{ username }}</span>
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
      <el-main class="main-content">
        <router-view v-slot="{ Component }">
          <transition name="fade-slide" mode="out-in">
            <component :is="Component" />
          </transition>
        </router-view>
      </el-main>
    </el-container>
  </el-container>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessageBox, ElMessage } from 'element-plus'
import { useAuthStore } from '@/stores/auth'
import {
  FolderOpened,
  Files,
  Download,
  Setting,
  User,
  CaretBottom,
  SwitchButton,
  Expand,
  Fold,
} from '@element-plus/icons-vue'

const route = useRoute()
const router = useRouter()
const authStore = useAuthStore()

// 状态
const isCollapse = ref(false)

// 计算属性
const activeMenu = computed(() => route.path)
const username = computed(() => authStore.username || '未登录')
const userAvatar = computed(() => authStore.avatar)

const pageTitle = computed(() => {
  const titles: Record<string, string> = {
    '/files': '文件管理',
    '/downloads': '下载管理',
    '/settings': '系统设置',
  }
  return titles[route.path] || '百度网盘'
})

// 切换侧边栏折叠状态
function toggleCollapse() {
  isCollapse.value = !isCollapse.value
}

// 下拉菜单命令处理
async function handleCommand(command: string) {
  switch (command) {
    case 'profile':
      ElMessage.info('个人信息功能开发中...')
      break
    case 'logout':
      try {
        await ElMessageBox.confirm('确定要退出登录吗？', '退出确认', {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        })
        await authStore.logout()
        ElMessage.success('已退出登录')
        router.push('/login')
      } catch (error) {
        if (error !== 'cancel') {
          console.error('退出登录失败:', error)
        }
      }
      break
  }
}

// 监听路由变化，自动折叠侧边栏（移动端）
watch(
  () => route.path,
  () => {
    if (window.innerWidth < 768) {
      isCollapse.value = true
    }
  }
)
</script>

<style scoped lang="scss">
.main-layout {
  width: 100%;
  height: 100vh;
}

.sidebar {
  display: flex;
  flex-direction: column;
  background: #304156;
  transition: width 0.3s;
  overflow: hidden;

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

  .header-left {
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
}

// 过渡动画
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
</style>

