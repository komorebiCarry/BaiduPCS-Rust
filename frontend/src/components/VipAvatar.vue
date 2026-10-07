<template>
  <span
      class="vip-avatar"
      :class="[`vip-avatar--${tier}`, { 'vip-avatar--badged': showLevel && label }]"
      :style="{ '--vip-badge-font': badgeFont }">
    <el-avatar :src="src || ''" :size="size">
      <slot>
        <el-icon><UserFilled /></el-icon>
      </slot>
    </el-avatar>
    <span v-if="showLevel && label" class="vip-avatar__badge">{{ label }}</span>
  </span>
</template>

<script setup lang="ts">
/**
 * 带会员圆环的头像
 *
 * 圆环颜色：普通用户白色 / VIP 黄色 / SVIP 紫色；右下角小字显示具体等级（如 SVIP5）。
 * 默认插槽为头像加载失败时的占位内容。
 */
import { computed } from 'vue'
import { UserFilled } from '@element-plus/icons-vue'
import { vipLabel } from '@/utils/vip'

const props = withDefaults(defineProps<{
  src?: string | null
  size?: number
  vipType?: number | null
  vipLevel?: number | null
  /** 是否显示右下角等级小字 */
  showLevel?: boolean
}>(), {
  src: '',
  size: 32,
  vipType: 0,
  vipLevel: null,
  showLevel: true,
})

const tier = computed(() => (props.vipType === 2 ? 'svip' : props.vipType === 1 ? 'vip' : 'normal'))
const label = computed(() => vipLabel(props.vipType, props.vipLevel))
const badgeFont = computed(() => `${Math.max(8, Math.round(props.size * 0.26))}px`)
</script>

<style scoped lang="scss">
.vip-avatar {
  --vip-ring: #ffffff;
  position: relative;
  display: inline-flex;
  flex: 0 0 auto;
  border: 2px solid var(--vip-ring);
  border-radius: 50%;
  line-height: 0;
}

/* 白环在浅色背景上不可见，外侧补一圈浅描边 */
.vip-avatar--normal {
  box-shadow: 0 0 0 1px var(--el-border-color);
}

.vip-avatar--vip {
  --vip-ring: #f5a623;
}

.vip-avatar--svip {
  --vip-ring: #8b5cf6;
}

/* 角标向右溢出，留出间距避免压住相邻文字 */
.vip-avatar--badged {
  margin-right: 8px;
}

.vip-avatar__badge {
  position: absolute;
  right: -8px;
  bottom: -4px;
  padding: 1px 3px;
  border: 1px solid #ffffff;
  border-radius: 6px;
  background: var(--vip-ring);
  color: #ffffff;
  font-size: var(--vip-badge-font);
  font-weight: 700;
  line-height: 1;
  white-space: nowrap;
  pointer-events: none;
}
</style>
