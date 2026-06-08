<template>
  <div v-if="visible" class="account-filter">
    <!-- 账号 ≤ collapseThreshold：直接平铺显示按钮组 -->
    <el-radio-group
        v-if="authStore.accounts.length <= collapseThreshold"
        :model-value="internalValue"
        :size="size"
        @update:model-value="onChange"
    >
      <el-radio-button :value="ALL_SENTINEL">
        <span class="account-filter__label">
          <!-- 占位 icon 让 "全部账号" 与有头像账号高度一致 -->
          <el-icon class="account-filter__icon"><User /></el-icon>
          全部账号
          <el-tag v-if="totalCount !== undefined" size="small" type="info" effect="plain" class="account-filter__count">
            {{ totalCount }}
          </el-tag>
        </span>
      </el-radio-button>
      <el-radio-button
          v-for="acc in authStore.accounts"
          :key="acc.uid"
          :value="acc.uid"
      >
        <span class="account-filter__label">
          <el-avatar v-if="acc.avatar_url" :src="acc.avatar_url" :size="14" class="account-filter__avatar" />
          <el-icon v-else class="account-filter__icon"><UserFilled /></el-icon>
          {{ acc.nickname || acc.username }}
          <el-tag v-if="counts && counts[acc.uid] !== undefined" size="small" type="primary" effect="plain" class="account-filter__count">
            {{ counts[acc.uid] }}
          </el-tag>
        </span>
      </el-radio-button>
    </el-radio-group>

    <!-- 账号过多：当前选中按钮 + 下拉 (防挤爆) -->
    <el-dropdown v-else trigger="click" @command="onChange">
      <el-button :size="size" class="account-filter__dropdown-trigger">
        <span class="account-filter__label">
          <template v-if="modelValue === null">
            <el-icon class="account-filter__icon"><User /></el-icon>
            全部账号
            <el-tag v-if="totalCount !== undefined" size="small" type="info" effect="plain" class="account-filter__count">
              {{ totalCount }}
            </el-tag>
          </template>
          <template v-else-if="selectedAccount">
            <el-avatar v-if="selectedAccount.avatar_url" :src="selectedAccount.avatar_url" :size="14" class="account-filter__avatar" />
            <el-icon v-else class="account-filter__icon"><UserFilled /></el-icon>
            {{ selectedAccount.nickname || selectedAccount.username }}
            <el-tag v-if="counts && counts[modelValue] !== undefined" size="small" type="primary" effect="plain" class="account-filter__count">
              {{ counts[modelValue] }}
            </el-tag>
          </template>
        </span>
        <el-icon class="el-icon--right"><ArrowDown /></el-icon>
      </el-button>
      <template #dropdown>
        <el-dropdown-menu>
          <el-dropdown-item :command="null" :class="{ 'is-active': modelValue === null }">
            <el-icon><User /></el-icon>
            <span style="margin-left: 6px">全部账号</span>
            <el-tag v-if="totalCount !== undefined" size="small" type="info" effect="plain" style="margin-left: 8px">
              {{ totalCount }}
            </el-tag>
          </el-dropdown-item>
          <el-dropdown-item
              v-for="acc in authStore.accounts"
              :key="acc.uid"
              :command="acc.uid"
              :class="{ 'is-active': modelValue === acc.uid }"
          >
            <el-avatar v-if="acc.avatar_url" :src="acc.avatar_url" :size="16" />
            <el-icon v-else><UserFilled /></el-icon>
            <span style="margin-left: 6px; max-width: 160px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;">
              {{ acc.nickname || acc.username }}
            </span>
            <el-tag v-if="counts && counts[acc.uid] !== undefined" size="small" type="primary" effect="plain" style="margin-left: 8px">
              {{ counts[acc.uid] }}
            </el-tag>
          </el-dropdown-item>
        </el-dropdown-menu>
      </template>
    </el-dropdown>
  </div>
</template>

<script setup lang="ts">
/**
 * 跨账号任务列表过滤器
 *
 * 用于 DownloadsView / UploadsView / TransfersView / AutoBackupView / CloudDlView
 * 顶部，按 owner_uid 过滤当前列表。
 *
 * 行为：
 *   - `modelValue = null`   → 显示所有账号的任务（默认）
 *   - `modelValue = <uid>`  → 仅显示该 uid 的任务
 *   - 当 `accounts.length <= 1` 时整个组件不渲染（v-if visible）
 *   - 可选传入 `counts` （`{ [uid]: number }`）在每个按钮显示任务计数 badge
 *   - 可选传入 `totalCount` 在 "全部账号" 按钮显示总数
 */
import { computed } from 'vue'
import { useAuthStore } from '@/stores/auth'
import { User, UserFilled, ArrowDown } from '@element-plus/icons-vue'

const props = withDefaults(
  defineProps<{
    /** 当前选中的过滤 UID（`null` = 全部账号） */
    modelValue: number | null
    /** 各 UID 任务数量；可选，仅作为 UI badge 展示 */
    counts?: Record<number, number>
    /** 全部账号下的任务总数；可选 */
    totalCount?: number
    /** el-radio-group 尺寸，跟随父级 el-tag/el-button size 保持视觉对齐 */
    size?: 'large' | 'default' | 'small'
    /** 折叠阈值：账号数 ≤ 此值时平铺显示按钮组，否则用 dropdown 防止挤爆 */
    collapseThreshold?: number
  }>(),
  { size: 'default', collapseThreshold: 4 }
)

const emit = defineEmits<{
  (e: 'update:modelValue', value: number | null): void
  (e: 'change', value: number | null): void
}>()

const authStore = useAuthStore()

/**
 * "全部账号" sentinel 值。
 *
 * Element Plus 的 `el-radio-button` 内部用 `===` 严格比较 modelValue 与 value，
 * 但其 prop 类型不包含 `null`（仅支持 string/number/boolean），导致直接传
 * `:value="null"` + `modelValue: null` 时**无法匹配**，"全部账号" 按钮不会高
 * 亮。
 *
 * 此处用一个不可能与真实百度 UID 冲突的字符串作为内部 sentinel：
 *   - 对外接口（modelValue / emit）保持 `number | null` 不变
 *   - 内部 radio-group 用 `internalValue: number | typeof ALL_SENTINEL` 显示
 *   - `onChange` 在出参时转回 `null`
 */
const ALL_SENTINEL = '__all__' as const

const visible = computed(() => authStore.accounts.length > 1)

const internalValue = computed<number | typeof ALL_SENTINEL>(() =>
  props.modelValue === null ? ALL_SENTINEL : props.modelValue
)

const selectedAccount = computed(() => {
  if (props.modelValue === null) return null
  return authStore.accounts.find((a) => a.uid === props.modelValue) || null
})

function onChange(value: number | null | string | boolean | undefined) {
  // ALL_SENTINEL / null / undefined 均代表 "全部账号"
  const next =
    value === ALL_SENTINEL || value === null || value === undefined
      ? null
      : (value as number)
  emit('update:modelValue', next)
  emit('change', next)
}
</script>

<style scoped>
.account-filter {
  display: inline-flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px;
  /* 注意：不要加 margin-bottom，否则会在父级 flex(align-items: center) 容器里把组件向上挤偏导致与左侧元素不对齐 */
}

.account-filter__label {
  display: inline-flex;
  align-items: center;
  gap: 6px;
}

.account-filter__avatar {
  flex: 0 0 auto;
}

.account-filter__icon {
  /* 与 el-avatar size=14 对齐，确保"全部账号"和有头像账号高度一致 */
  font-size: 14px;
  flex: 0 0 auto;
}

.account-filter__dropdown-trigger {
  /* dropdown 触发按钮高度跟随 size，避免和左侧 el-tag 错位 */
  display: inline-flex;
  align-items: center;
}

.account-filter__count {
  margin-left: 4px;
  padding: 0 6px;
  height: 18px;
  line-height: 16px;
}
</style>
