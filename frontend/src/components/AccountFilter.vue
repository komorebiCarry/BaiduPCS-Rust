<template>
  <div v-if="visible" class="account-filter" :class="{ 'account-filter--block': block }">
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
          <span class="account-filter__name">全部账号</span>
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
          <span class="account-filter__name" :title="acc.nickname || acc.username">{{ acc.nickname || acc.username }}</span>
          <el-tag v-if="counts && counts[acc.uid] !== undefined" size="small" type="primary" effect="plain" class="account-filter__count">
            {{ counts[acc.uid] }}
          </el-tag>
        </span>
      </el-radio-button>
    </el-radio-group>

    <!-- 账号过多：当前选中按钮 + 下拉 (防挤爆) -->
    <el-dropdown v-else trigger="click" class="account-filter__dropdown" @command="onChange">
      <el-button :size="size" class="account-filter__dropdown-trigger" aria-label="按账号筛选">
        <span class="account-filter__label">
          <template v-if="modelValue === null">
            <el-icon class="account-filter__icon"><User /></el-icon>
            <span class="account-filter__name">全部账号</span>
            <el-tag v-if="totalCount !== undefined" size="small" type="info" effect="plain" class="account-filter__count">
              {{ totalCount }}
            </el-tag>
          </template>
          <template v-else-if="selectedAccount">
            <el-avatar v-if="selectedAccount.avatar_url" :src="selectedAccount.avatar_url" :size="14" class="account-filter__avatar" />
            <el-icon v-else class="account-filter__icon"><UserFilled /></el-icon>
            <span class="account-filter__name" :title="selectedAccount.nickname || selectedAccount.username">{{ selectedAccount.nickname || selectedAccount.username }}</span>
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
    /**
     * 独占整行（手机工具栏用）：按钮组不折行，放得下时均分整行，放不下时横向滚动；
     * 下拉形态的触发按钮撑满整行，长名称省略显示。
     */
    block?: boolean
  }>(),
  { size: 'default', collapseThreshold: 4, block: false }
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

/* 名称包一层 span 仅供整行模式省略显示；桌面保持原样式不裁剪。
   Element Plus 的 `.el-button [class*=el-icon] + span { margin-left: 6px }` 会命中这个 span，
   与 label 自身的 gap 叠加，这里归零以保持原间距 */
.account-filter__dropdown-trigger .account-filter__name {
  margin-left: 0;
}

/* 原先计数徽标紧跟图标（中间只有文本节点），命中 Element Plus 按钮内图标后续元素的外边距规则（实测 8px）；名称包成 span 后保持原间距 */
.account-filter__dropdown-trigger .account-filter__count {
  margin-left: 8px;
}

.account-filter__count {
  margin-left: 4px;
  padding: 0 6px;
  height: 18px;
  line-height: 16px;
}

/* ===== 独占整行（手机工具栏） ===== */
.account-filter--block {
  display: flex;
  flex-wrap: nowrap;
  width: 100%;
  min-width: 0;
  overflow-x: auto;
  overflow-y: hidden;
  -webkit-overflow-scrolling: touch;
  scrollbar-width: none;
}

.account-filter--block::-webkit-scrollbar {
  display: none;
}

/* 放得下时按钮组撑满整行并均分；放不下时按内容宽度排开，由外层横向滚动，绝不折行 */
.account-filter--block :deep(.el-radio-group) {
  flex: 0 0 auto;
  flex-wrap: nowrap;
  width: max-content;
  min-width: 100%;
}

.account-filter--block :deep(.el-radio-button) {
  flex: 1 0 auto;
}

/* 统一按钮高度：带计数徽标与不带徽标的按钮等高，边框上下对齐 */
.account-filter--block :deep(.el-radio-button__inner) {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 100%;
  height: 32px;
  padding: 0 8px;
}

/* 整行模式下省掉占位图标，让 2~3 个账号在 320px 宽也尽量一屏放下 */
.account-filter--block :deep(.el-radio-button) .account-filter__icon {
  display: none;
}

.account-filter--block :deep(.el-radio-button) .account-filter__label {
  justify-content: center;
}

/* 单选按钮内部 line-height: 1，overflow: hidden 会把下划线等下伸部分裁掉，这里给足行高 */
.account-filter--block .account-filter__name {
  min-width: 0;
  max-width: 9em;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  line-height: 20px;
}

.account-filter--block .account-filter__dropdown {
  display: flex;
  width: 100%;
  min-width: 0;
}

.account-filter--block .account-filter__dropdown-trigger {
  width: 100%;
  min-width: 0;
}

/* el-button 内部包裹默认插槽的 span：撑满按钮，让名称省略、箭头贴右 */
.account-filter--block .account-filter__dropdown-trigger > :deep(span) {
  display: flex;
  align-items: center;
  flex: 1 1 auto;
  min-width: 0;
}

.account-filter--block .account-filter__dropdown-trigger .account-filter__label {
  flex: 1 1 auto;
  min-width: 0;
}

.account-filter--block .account-filter__dropdown-trigger .account-filter__name {
  max-width: none;
}
</style>
