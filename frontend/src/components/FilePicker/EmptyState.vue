<template>
  <div class="empty-state">
    <el-empty description="此文件夹为空" :image-size="100">
      <template #image>
        <el-icon class="empty-icon"><FolderOpened /></el-icon>
      </template>
    </el-empty>
  </div>
</template>

<script setup lang="ts">
import { FolderOpened } from '@element-plus/icons-vue'
</script>

<style scoped>
.empty-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  min-height: 200px;
}

.empty-icon {
  font-size: 64px;
  color: var(--el-text-color-secondary);
  opacity: 0.5;
}
</style>