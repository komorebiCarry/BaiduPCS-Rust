<template>
  <div class="error-state">
    <el-result icon="error" :title="title" :sub-title="message">
      <template #extra>
        <el-button type="primary" @click="emit('retry')">重试</el-button>
      </template>
    </el-result>
  </div>
</template>

<script setup lang="ts">
withDefaults(defineProps<{
  message: string
  title?: string
}>(), {
  title: '加载失败',
})

const emit = defineEmits<{
  'retry': []
}>()
</script>

<style scoped>
.error-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  min-height: 200px;
}

.error-state :deep(.el-result__title) {
  margin-top: 12px;
}

.error-state :deep(.el-result__subtitle) {
  margin-top: 8px;
}
</style>