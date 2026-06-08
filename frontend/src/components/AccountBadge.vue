<template>
  <el-tag
      v-if="visible"
      :type="tagType"
      :effect="effect"
      :size="size"
      class="account-badge"
      :class="{ 'is-active': isActive }"
  >
    <el-avatar
        v-if="showAvatar && avatarUrl"
        :src="avatarUrl"
        :size="14"
        class="account-badge__avatar"
    />
    <span class="account-badge__name">{{ displayName }}</span>
  </el-tag>
</template>

<script setup lang="ts">
/**
 * 账号 chip 组件
 *
 * 在跨账号统一展示视图（DownloadsView/UploadsView/TransfersView/AutoBackupView/CloudDl）
 * 中给每条任务记录渲染所属账号 chip。
 *
 * 显示策略：
 *   1. accounts.length <= 1（无多账号语义）→ 不渲染（visible=false）；
 *   2. owner_uid 缺失（后端老路径）→ 不渲染；
 *   3. accounts 中找不到对应条目（账号已删除）→ 显示 `UID:xxxx` 并降为 info 灰色；
 *   4. 当前活跃账号 → primary 加深，其余 info 普通色。
 */
import { computed } from 'vue'
import { useAuthStore } from '@/stores/auth'
import { useOwnerName } from '@/composables/useOwnerName'

const props = withDefaults(
    defineProps<{
      /** 任务的 owner_uid（来自 backend DTO / WS event；可能 undefined） */
      ownerUid?: number | null
      /** 仅当全局有多账号时才显示，默认 true（=自动判断），传 false 强制隐藏，传 'always' 强制显示 */
      autoHide?: boolean | 'always'
      /** 是否显示账号头像 */
      showAvatar?: boolean
      /** el-tag size */
      size?: '' | 'large' | 'default' | 'small'
      /** el-tag effect */
      effect?: 'dark' | 'light' | 'plain'
    }>(),
    {
      ownerUid: null,
      autoHide: true,
      showAvatar: true,
      size: 'small',
      effect: 'plain',
    },
)

const authStore = useAuthStore()
const { ownerName, ownerAvatar, ownerSummary } = useOwnerName()

const visible = computed(() => {
  if (props.autoHide === 'always') return props.ownerUid !== null && props.ownerUid !== undefined
  if (props.autoHide === false) return false
  // auto: 仅当多账号 + ownerUid 存在时显示
  if (authStore.accounts.length <= 1) return false
  return props.ownerUid !== null && props.ownerUid !== undefined
})

const isActive = computed(() => {
  return props.ownerUid !== null && props.ownerUid === authStore.activeUid
})

const summary = computed(() => ownerSummary(props.ownerUid))
const displayName = computed(() => ownerName(props.ownerUid))
const avatarUrl = computed(() => ownerAvatar(props.ownerUid))

const tagType = computed<'primary' | 'success' | 'info' | 'warning' | 'danger' | ''>(() => {
  if (!summary.value) return 'info' // 找不到账号（已删除）
  if (isActive.value) return 'primary'
  return 'info'
})
</script>

<style scoped>
.account-badge {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding-left: 6px;
  padding-right: 8px;
  vertical-align: middle;
}

.account-badge.is-active {
  font-weight: 600;
}

.account-badge__avatar {
  flex: 0 0 auto;
  border: 1px solid var(--el-color-primary-light-7);
}

.account-badge__name {
  white-space: nowrap;
  max-width: 120px;
  overflow: hidden;
  text-overflow: ellipsis;
}
</style>
