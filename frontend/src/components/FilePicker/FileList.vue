<template>
  <div class="file-list">
    <table class="file-table">
      <thead>
      <tr>
        <th v-if="multiple" class="col-checkbox"></th>
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
          :selected="multiple ? isMultiSelected(entry) : selection?.id === entry.id"
          :disabled="isDisabled(entry)"
          :show-checkbox="multiple"
          @click="handleClick"
          @dblclick="handleDblClick"
          @checkbox-change="handleCheckboxChange"
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
  multiSelection?: FileEntry[]
  selectType?: 'file' | 'directory' | 'both'
  multiple?: boolean
}>()

const emit = defineEmits<{
  'select': [entry: FileEntry]
  'open': [entry: FileEntry]
  'toggle-select': [entry: FileEntry]
  'select-all': []
  'clear-selection': []
}>()

// 检查条目是否在多选列表中
function isMultiSelected(entry: FileEntry): boolean {
  return props.multiSelection?.some(e => e.id === entry.id) ?? false
}

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
  if (props.multiple) {
    emit('toggle-select', entry)
  } else {
    emit('select', entry)
  }
}

// 双击打开
function handleDblClick(entry: FileEntry) {
  emit('open', entry)
}

// 复选框变化
function handleCheckboxChange(entry: FileEntry) {
  emit('toggle-select', entry)
}

</script>

<style scoped>
.file-list {
  height: 100%;
  overflow-y: auto;
}

.multi-select-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 8px 12px;
  background: linear-gradient(to bottom, #f8f9fa, #f0f2f5);
  border-bottom: 1px solid var(--el-border-color);
}

.multi-select-toolbar .el-button {
  font-size: 13px;
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

.col-checkbox {
  width: 40px;
  text-align: center;
}

.col-name {
  width: 40%;
}

.col-time {
  width: 23%;
}

.col-type {
  width: 15%;
}

.col-size {
  width: 12%;
  text-align: right !important;
}

.file-table :deep(td:last-child) {
  text-align: right;
}
</style>