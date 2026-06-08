<template>
  <el-card class="setting-card" shadow="hover">
    <template #header>
      <div class="card-header">
        <el-icon :size="20" color="#409eff"><UserFilled /></el-icon>
        <span>账号管理</span>
        <el-tag size="small" type="info" effect="plain" class="card-header__count">
          {{ authStore.accounts.length }} 个账号
        </el-tag>
        <div class="card-header__actions">
          <el-button :icon="Refresh" :loading="refreshing" circle size="small" @click="refresh" />
          <el-button type="primary" size="small" @click="addAccount">
            <el-icon><Plus /></el-icon>
            添加账号
          </el-button>
        </div>
      </div>
    </template>

    <!-- 空状态 -->
    <el-empty v-if="authStore.accounts.length === 0" description="暂无账号" :image-size="80">
      <el-button type="primary" @click="addAccount">
        <el-icon><Plus /></el-icon>
        登录新账号
      </el-button>
    </el-empty>

    <!-- 账号表格 -->
    <el-table v-else :data="authStore.accounts" stripe row-key="uid" class="account-table">
      <el-table-column label="账号" min-width="220">
        <template #default="{ row }">
          <div class="account-cell">
            <el-avatar :src="row.avatar_url || ''" :size="32">
              <el-icon><UserFilled /></el-icon>
            </el-avatar>
            <div class="account-cell__info">
              <span class="account-cell__name">
                {{ row.nickname || row.username }}
              </span>
              <span class="account-cell__sub">{{ row.username }}</span>
            </div>
          </div>
        </template>
      </el-table-column>

      <el-table-column label="UID" prop="uid" width="140" />

      <el-table-column label="会员" width="120">
        <template #default="{ row }">
          <el-tag v-if="row.vip_type" type="warning" effect="plain" size="small">
            VIP{{ row.vip_type }}
          </el-tag>
          <span v-else class="account-cell__sub">普通用户</span>
        </template>
      </el-table-column>

      <el-table-column label="状态" width="120">
        <template #default="{ row }">
          <el-tag v-if="row.uid === authStore.activeUid" type="primary" effect="dark" size="small">
            当前活跃
          </el-tag>
          <el-tag v-else type="info" effect="plain" size="small">
            空闲
          </el-tag>
        </template>
      </el-table-column>

      <el-table-column label="操作" width="200" align="right">
        <template #default="{ row }">
          <el-button
              v-if="row.uid !== authStore.activeUid"
              type="primary"
              size="small"
              text
              :loading="busyUid === row.uid && busyAction === 'switch'"
              @click="switchTo(row.uid)"
          >
            切换
          </el-button>
          <el-button
              type="danger"
              size="small"
              text
              :loading="busyUid === row.uid && busyAction === 'delete'"
              @click="confirmDelete(row)"
          >
            删除
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <!-- 服务端只读模式提示 -->
    <el-alert
        v-if="authStore.readonlyMode"
        type="warning"
        show-icon
        :closable="false"
        class="account-readonly-banner"
        title="服务端已进入只读保护模式"
        description="服务端检测到数据异常，已进入只读保护模式以避免数据损坏。请先重启后端再次尝试自动修复；若重启后仍未恢复，请还原升级前备份的 config/ 与 wal/ 目录后重启；系统也会在升级迁移前自动备份到 config/backups/pre_migration_<时间戳>/。"
    />
  </el-card>
</template>

<script setup lang="ts">
/**
 * 账号管理 Section
 *
 * 嵌入 SettingsView 内作为独立设置分区 `#section-accounts`。
 * 提供：
 *   - 账号列表（昵称 / 用户名 / UID / VIP / 活跃状态）
 *   - 添加账号（→ /login?mode=add）
 *   - 切换活跃账号（HTTP，store.switchAccount）
 *   - 强制删除账号（HTTP，store.deleteAccount(uid, force=true)）
 *   - 服务端只读模式提示横幅
 *
 * 服务端事件 `AccountEvent::ListChanged` 会通过 WS 推送并刷新 store，
 * 无需在此组件再主动 fetch（除非用户点击刷新按钮）。
 */
import { ref } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Plus, UserFilled, Refresh } from '@element-plus/icons-vue'
import { useAuthStore } from '@/stores/auth'
import type { AccountSummary } from '@/api/accounts'

const router = useRouter()
const authStore = useAuthStore()

const refreshing = ref(false)
const busyUid = ref<number | null>(null)
const busyAction = ref<'switch' | 'delete' | null>(null)

async function refresh() {
  refreshing.value = true
  try {
    await authStore.fetchAccountList()
    ElMessage.success(`已刷新（${authStore.accounts.length} 个账号）`)
  } catch (err: any) {
    ElMessage.error(err?.message || '刷新账号列表失败')
  } finally {
    refreshing.value = false
  }
}

function addAccount() {
  router.push({ path: '/login', query: { mode: 'add' } })
}

async function switchTo(uid: number) {
  busyUid.value = uid
  busyAction.value = 'switch'
  try {
    await authStore.switchAccount(uid)
    const target = authStore.accounts.find((a) => a.uid === uid)
    ElMessage.success(`已切换到 ${target?.nickname || target?.username || `UID:${uid}`}`)
  } catch (err: any) {
    ElMessage.error(err?.response?.data?.message || err?.message || '切换账号失败')
  } finally {
    busyUid.value = null
    busyAction.value = null
  }
}

async function confirmDelete(row: AccountSummary) {
  const isActive = row.uid === authStore.activeUid
  const isLast = authStore.accounts.length === 1

  let warn = `确认强制删除账号 「${row.nickname || row.username}」 (UID: ${row.uid}) ?`
  if (isActive) {
    warn += isLast
        ? '\n\n该账号是当前活跃账号，且为唯一账号。删除后将进入未登录状态，需重新扫码登录。'
        : '\n\n该账号是当前活跃账号；删除后服务端将自动切换到其它账号。'
  }
  warn += '\n\n强制删除会立即关闭该账号的所有运行中任务，且无法恢复。'

  try {
    await ElMessageBox.confirm(warn, '删除账号', {
      type: 'warning',
      confirmButtonText: '确认删除',
      cancelButtonText: '取消',
      confirmButtonClass: 'el-button--danger',
    })
  } catch {
    return // 用户取消
  }

  busyUid.value = row.uid
  busyAction.value = 'delete'
  try {
    const newActiveUid = await authStore.deleteAccount(row.uid, true)
    if (newActiveUid === null) {
      ElMessage.success('账号已删除，已进入未登录状态')
      // 多账号 store 已清空 activeUid；跳转登录页
      router.push('/login')
    } else {
      ElMessage.success('账号已删除')
    }
  } catch (err: any) {
    ElMessage.error(err?.response?.data?.message || err?.message || '删除账号失败')
  } finally {
    busyUid.value = null
    busyAction.value = null
  }
}
</script>

<style scoped>
.setting-card {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  align-items: center;
  gap: 8px;
}

.card-header__count {
  margin-left: 4px;
}

.card-header__actions {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 8px;
}

.account-cell {
  display: flex;
  align-items: center;
  gap: 12px;
}

.account-cell__info {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.account-cell__name {
  font-weight: 500;
  font-size: 14px;
  color: var(--el-text-color-primary);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.account-cell__sub {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.account-readonly-banner {
  margin-top: 16px;
}
</style>
