<template>
  <template v-if="indicator">
    <!-- PC 侧边栏：一行放不下更多，只显示 已完成/总数，速度等放到悬停提示里 -->
    <el-tooltip v-if="mode === 'sidebar'" :content="detailText" placement="right" :show-after="300">
      <span class="transfer-count" :class="{ 'has-failed': indicator.failed > 0 }">
        {{ indicator.completed }}/{{ indicator.total }}
      </span>
    </el-tooltip>

    <!-- 手机抽屉：没有悬停，数量和速度/进度分两行显示；抽屉只有 240px，失败数挪到第一行 -->
    <span v-else class="transfer-meta">
      <span class="transfer-count" :class="{ 'has-failed': indicator.failed > 0 }">
        <template v-if="indicator.failed > 0">失败 {{ indicator.failed }} · </template>{{ indicator.completed }}/{{ indicator.total }}
      </span>
      <span class="transfer-detail">{{ speedAndPercent }}</span>
    </span>

    <span
        class="transfer-bar"
        :class="{ indeterminate: indicator.percent === null, paused: indicator.paused }"
        :style="indicator.percent === null ? undefined : { width: `${indicator.percent}%` }"
    />
  </template>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { formatSpeed } from '@/api/utils'
import type { TransferIndicator } from '@/stores/taskSummary'

const props = defineProps<{
  indicator: TransferIndicator | null
  mode: 'sidebar' | 'drawer'
}>()

const speedAndPercent = computed(() => {
  const ind = props.indicator
  if (!ind) return ''
  const progress = ind.percent === null ? '扫描中' : `${ind.percent}%`
  // 全部暂停时速度恒为 0，显示「已暂停」比「0 B/s」更直观
  return `${ind.paused ? '已暂停' : formatSpeed(ind.speed)} · ${progress}`
})

const detailText = computed(() => {
  const ind = props.indicator
  if (!ind || ind.failed <= 0) return speedAndPercent.value
  return `${speedAndPercent.value} · 失败 ${ind.failed}`
})
</script>

<style scoped lang="scss">
.transfer-count {
  margin-left: auto;
  padding-left: 8px;
  font-size: 12px;
  font-variant-numeric: tabular-nums;
  color: rgba(255, 255, 255, 0.85);
  white-space: nowrap;

  &.has-failed {
    color: #f89898;
  }
}

.transfer-meta {
  margin-left: auto;
  padding-left: 8px;
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  line-height: 1.3;
  min-width: 0;

  .transfer-count {
    margin-left: 0;
    padding-left: 0;
  }

  .transfer-detail {
    font-size: 11px;
    color: rgba(255, 255, 255, 0.6);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 100%;
  }
}

// 菜单项底部的细进度条（菜单项需为 position: relative）
.transfer-bar {
  position: absolute;
  left: 0;
  bottom: 0;
  height: 2px;
  background: #67c23a;
  transition: width 0.4s ease;
  pointer-events: none;

  &.paused {
    background: #909399;
  }

  &.indeterminate {
    width: 30%;
    animation: transfer-bar-slide 1.4s ease-in-out infinite;
  }
}

@keyframes transfer-bar-slide {
  from {
    left: -30%;
  }
  to {
    left: 100%;
  }
}

@media (prefers-reduced-motion: reduce) {
  .transfer-bar.indeterminate {
    animation: none;
    left: 0;
    width: 100%;
    opacity: 0.5;
  }
}
</style>
