<template>
  <el-dropdown
      v-if="visible"
      trigger="click"
      placement="bottom-end"
      :hide-on-click="true"
      class="account-switcher"
      @command="onCommand"
  >
    <span class="account-switcher__trigger">
      <el-avatar
          :src="activeAvatar"
          :size="28"
          class="account-switcher__avatar"
      >
        <el-icon><UserFilled /></el-icon>
      </el-avatar>
      <span class="account-switcher__name">{{ activeName }}</span>
      <el-icon class="account-switcher__caret"><ArrowDown /></el-icon>
    </span>

    <template #dropdown>
      <el-dropdown-menu class="account-switcher__menu">
        <div class="account-switcher__header">
          <el-icon><UserFilled /></el-icon>
          <span>切换账号</span>
          <el-tag size="small" type="info" effect="plain">
            {{ authStore.accounts.length }} 个账号
          </el-tag>
        </div>

        <el-dropdown-item
            v-for="acc in authStore.accounts"
            :key="acc.uid"
            :command="`switch:${acc.uid}`"
            :disabled="acc.uid === authStore.activeUid || switching"
            class="account-switcher__item"
        >
          <el-avatar
              :src="acc.avatar_url || ''"
              :size="24"
              class="account-switcher__item-avatar"
          >
            <el-icon><UserFilled /></el-icon>
          </el-avatar>
          <span class="account-switcher__item-name">
            {{ acc.nickname || acc.username }}
          </span>
          <el-tag v-if="acc.uid === authStore.activeUid" size="small" type="primary" effect="plain">
            当前
          </el-tag>
          <el-tag v-else-if="acc.vip_type" size="small" type="warning" effect="plain">
            VIP{{ acc.vip_type }}
          </el-tag>
        </el-dropdown-item>

        <el-dropdown-item divided command="add">
          <el-icon><Plus /></el-icon>
          添加账号
        </el-dropdown-item>
        <el-dropdown-item command="manage">
          <el-icon><Setting /></el-icon>
          账号管理
        </el-dropdown-item>

        <!-- 当前账号个人操作（与账号列表隔一条 divider） -->
        <el-dropdown-item divided command="profile">
          <el-icon><User /></el-icon>
          当前账号个人信息
        </el-dropdown-item>
        <el-dropdown-item command="logout">
          <el-icon><SwitchButton /></el-icon>
          退出当前账号
        </el-dropdown-item>
        <el-dropdown-item v-if="showWebLogout" command="webLogout">
          <el-icon><Lock /></el-icon>
          退出 Web 认证
        </el-dropdown-item>
      </el-dropdown-menu>
    </template>
  </el-dropdown>
</template>

<script setup lang="ts">
/**
 * 账号切换器
 *
 * 嵌入 MainLayout 顶部栏。仅在 `accounts.length >= 1` 时显示（单账号也显示触发器，
 * 仅在多账号时下拉列表才有可切换项）。
 *
 * 命令：
 *   - `switch:<uid>` 切换账号 → store.switchAccount() → 后端发 Switched + ListChanged
 *   - `add`          跳转 /login?mode=add（保留当前 active_uid）
 *   - `manage`       跳转 /settings?tab=accounts
 */
import { ref, computed } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import {
  ArrowDown,
  UserFilled,
  Plus,
  Setting,
  User,
  SwitchButton,
  Lock,
} from '@element-plus/icons-vue'
import { useAuthStore } from '@/stores/auth'

/**
 * Props
 *   - showWebLogout: 是否显示"退出 Web 认证"项，由父组件根据 webAuthStore.isAuthEnabled 传入
 *
 * Emits
 *   - profile    点击个人信息，父组件打开 UserProfileDialog
 *   - logout     点击退出百度账号，父组件调用 authStore.logout()
 *   - web-logout 点击退出 Web 认证
 */
defineProps<{
  showWebLogout?: boolean
}>()

const emit = defineEmits<{
  profile: []
  logout: []
  'web-logout': []
}>()

const router = useRouter()
const authStore = useAuthStore()

const switching = ref(false)

const visible = computed(() => authStore.accounts.length > 0)

const activeName = computed(() => {
  const acc = authStore.activeAccount
  if (acc) return acc.nickname || acc.username
  return authStore.username || '未登录'
})

const activeAvatar = computed(() => {
  return authStore.activeAccount?.avatar_url || authStore.avatar || ''
})

async function onCommand(cmd: string | number) {
  const c = String(cmd)
  if (c === 'add') {
    router.push({ path: '/login', query: { mode: 'add' } })
    return
  }
  if (c === 'manage') {
    router.push({ path: '/settings', query: { tab: 'accounts' } })
    return
  }
  if (c === 'profile') {
    emit('profile')
    return
  }
  if (c === 'logout') {
    emit('logout')
    return
  }
  if (c === 'webLogout') {
    emit('web-logout')
    return
  }
  if (c.startsWith('switch:')) {
    const uid = Number(c.slice('switch:'.length))
    if (!Number.isFinite(uid)) return
    if (uid === authStore.activeUid) return
    switching.value = true
    try {
      await authStore.switchAccount(uid)
      const target = authStore.accounts.find((a) => a.uid === uid)
      ElMessage.success(`已切换到 ${target?.nickname || target?.username || `UID:${uid}`}`)
    } catch (err: any) {
      console.error('切换账号失败:', err)
      ElMessage.error(err?.response?.data?.message || err?.message || '切换账号失败')
    } finally {
      switching.value = false
    }
  }
}
</script>

<style scoped>
.account-switcher__trigger {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 8px;
  border-radius: 6px;
  cursor: pointer;
  transition: background-color 0.15s ease;
  outline: none;
}

.account-switcher__trigger:hover,
.account-switcher__trigger:focus {
  background-color: var(--el-fill-color-light);
}

.account-switcher__avatar {
  flex: 0 0 auto;
}

.account-switcher__name {
  max-width: 120px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  font-size: 14px;
  color: var(--el-text-color-primary);
}

.account-switcher__caret {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.account-switcher__menu {
  min-width: 240px;
}

.account-switcher__header {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 8px 12px 4px 12px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.account-switcher__item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.account-switcher__item-avatar {
  flex: 0 0 auto;
}

.account-switcher__item-name {
  flex: 1 1 auto;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
