<template>
  <el-select
      :model-value="modelValue"
      :placeholder="placeholder"
      :clearable="clearable"
      :disabled="disabled || authStore.accounts.length === 0"
      :size="size"
      class="account-select"
      @update:model-value="onChange"
  >
    <el-option
        v-if="includeAll"
        :value="null as unknown as number"
        label="全部账号"
    >
      <span class="account-select__option">
        <el-icon><User /></el-icon>
        全部账号
      </span>
    </el-option>
    <el-option
        v-for="acc in authStore.accounts"
        :key="acc.uid"
        :value="acc.uid"
        :label="optionLabel(acc)"
    >
      <span class="account-select__option">
        <el-avatar v-if="acc.avatar_url" :src="acc.avatar_url" :size="20" class="account-select__avatar" />
        <span class="account-select__name">{{ acc.nickname || acc.username }}</span>
        <el-tag v-if="acc.is_active" size="small" type="primary" effect="plain" class="account-select__tag">
          活跃
        </el-tag>
        <span class="account-select__uid">UID: {{ acc.uid }}</span>
      </span>
    </el-option>
  </el-select>
</template>

<script setup lang="ts">
/**
 * 通用账号选择器
 *
 * 用于：
 *   - 创建 AutoBackup 配置时选择 owner_uid（必填）
 *   - 创建离线下载任务时选择目标账号（必填）
 *   - 视图过滤器场景（含 "全部账号"，需 includeAll=true）
 *
 * `modelValue` 类型为 `number | null`：
 *   - `null` 在 includeAll=true 场景下表示 "全部账号"
 *   - 在创建场景（includeAll=false）由父组件做必填校验
 */
import { useAuthStore } from '@/stores/auth'
import { User } from '@element-plus/icons-vue'
import type { AccountSummary } from '@/api/accounts'

withDefaults(
    defineProps<{
      modelValue: number | null
      /** 是否包含 "全部账号" 选项（默认 false） */
      includeAll?: boolean
      placeholder?: string
      clearable?: boolean
      disabled?: boolean
      size?: '' | 'large' | 'default' | 'small'
    }>(),
    {
      includeAll: false,
      placeholder: '请选择账号',
      clearable: false,
      disabled: false,
      size: 'default',
    },
)

const emit = defineEmits<{
  (e: 'update:modelValue', value: number | null): void
  (e: 'change', value: number | null): void
}>()

const authStore = useAuthStore()

function optionLabel(acc: AccountSummary): string {
  return acc.nickname || acc.username || `UID:${acc.uid}`
}

function onChange(value: number | null | string | undefined) {
  const next = (value === null || value === undefined || value === '') ? null : (value as number)
  emit('update:modelValue', next)
  emit('change', next)
}
</script>

<style scoped>
.account-select {
  min-width: 200px;
}

.account-select__option {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  width: 100%;
}

.account-select__avatar {
  flex: 0 0 auto;
}

.account-select__name {
  flex: 1 1 auto;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.account-select__tag {
  flex: 0 0 auto;
}

.account-select__uid {
  flex: 0 0 auto;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}
</style>
