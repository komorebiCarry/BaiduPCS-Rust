<template>
  <div class="file-list">
    <table class="file-table">
      <thead>
        <tr>
          <th class="col-name">名称</th>
          <th class="col-time">修改日期</th>
          <th class="col-type">类型</th>
          <th class="col-size">大小</th>
        </tr>
      </thead>
      <tbody>
        <FileItem
          v-for="entry in entries"
          :key="entry.id"
          :entry="entry"
          :selected="selection?.id === entry.id"
          :disabled="isDisabled(entry)"
          @click="handleClick"
          @dblclick="handleDblClick"
        />
      </tbody>
    </table>
  </div>
</template>

<script setup lang="ts">
import type { FileEntry } from '@/api/filesystem'
import FileItem from './FileItem.vue'

const props = defineProps<{
  entries: FileEntry[]
  selection: FileEntry | null
  selectType?: 'file' | 'directory' | 'both'
}>()

const emit = defineEmits<{
  'select': [entry: FileEntry]
  'open': [entry: FileEntry]
}>()

// 检查条目是否禁用
function isDisabled(entry: FileEntry): boolean {
  if (props.selectType === 'file' && entry.entryType === 'directory') {
    return false // 文件夹不禁用，允许双击进入
  }
  if (props.selectType === 'directory' && entry.entryType === 'file') {
    return true
  }
  return false
}

// 单击选择
function handleClick(entry: FileEntry) {
  emit('select', entry)
}

// 双击打开
function handleDblClick(entry: FileEntry) {
  emit('open', entry)
}
</script>

<style scoped>
.file-list {
  height: 100%;
  overflow-y: auto;
}

.file-table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}

.file-table thead {
  position: sticky;
  top: 0;
  background: var(--el-fill-color-light);
  z-index: 1;
}

.file-table th {
  padding: 8px 12px;
  text-align: left;
  font-weight: 500;
  font-size: 13px;
  color: var(--el-text-color-secondary);
  border-bottom: 1px solid var(--el-border-color);
  user-select: none;
}

.col-name {
  width: 45%;
}

.col-time {
  width: 25%;
}

.col-type {
  width: 15%;
}

.col-size {
  width: 15%;
  text-align: right !important;
}

.file-table :deep(td:last-child) {
  text-align: right;
}
</style>