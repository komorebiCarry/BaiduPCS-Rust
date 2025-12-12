<template>
  <div class="file-list" :class="{ 'is-mobile': isMobile }">
    <!-- PC端表格视图 -->
    <table v-if="!isMobile" class="file-table">
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

    <!-- 移动端卡片视图 -->
    <div v-else class="mobile-file-list">
      <div
          v-for="entry in entries"
          :key="entry.id"
          class="mobile-file-card"
          :class="{
          'is-selected': multiple ? isMultiSelected(entry) : selection?.id === entry.id,
          'is-disabled': isDisabled(entry),
          'is-directory': entry.entryType === 'directory'
        }"
          @click="handleClick(entry)"
      >
        <!-- 复选框（多选模式） -->
        <div v-if="multiple" class="card-checkbox" @click.stop>
          <el-checkbox
              :model-value="isMultiSelected(entry)"
              :disabled="isDisabled(entry)"
              @change="handleCheckboxChange(entry)"
          />
        </div>

        <!-- 文件图标和信息 -->
        <div class="card-main" @dblclick="handleDblClick(entry)">
          <el-icon :size="36" class="file-card-icon" :color="entry.entryType === 'directory' ? '#f0ad4e' : '#909399'">
            <Folder v-if="entry.entryType === 'directory'" />
            <Document v-else />
          </el-icon>
          <div class="file-card-info">
            <div class="file-card-name" :title="entry.name">{{ entry.name }}</div>
            <div class="file-card-meta">
              <span class="meta-type">{{ getTypeText(entry) }}</span>
              <span v-if="entry.entryType !== 'directory'" class="meta-divider">·</span>
              <span v-if="entry.entryType !== 'directory'" class="meta-size">{{ formatSize(entry.size) }}</span>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { FileEntry } from '@/api/filesystem'
import { useIsMobile } from '@/utils/responsive'
import { Folder, Document } from '@element-plus/icons-vue'
import FileItem from './FileItem.vue'

// 响应式检测
const isMobile = useIsMobile()

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

// 获取文件类型文本
function getTypeText(entry: FileEntry): string {
  if (entry.entryType === 'directory') {
    if (/^[A-Za-z]:$/.test(entry.name)) {
      return '本地磁盘'
    }
    return '文件夹'
  }

  const ext = entry.name.split('.').pop()?.toLowerCase()
  if (!ext) return '文件'

  const typeMap: Record<string, string> = {
    jpg: 'JPEG 图像', jpeg: 'JPEG 图像', png: 'PNG 图像', gif: 'GIF 图像',
    mp4: 'MP4 视频', avi: 'AVI 视频', mkv: 'MKV 视频',
    mp3: 'MP3 音频', wav: 'WAV 音频',
    pdf: 'PDF 文档', doc: 'Word 文档', docx: 'Word 文档',
    xls: 'Excel 表格', xlsx: 'Excel 表格',
    txt: '文本文件',
    zip: 'ZIP 压缩', rar: 'RAR 压缩',
    exe: '可执行文件',
  }

  return typeMap[ext] || `${ext.toUpperCase()} 文件`
}

// 格式化文件大小
function formatSize(bytes: number | null): string {
  if (bytes === null || bytes === undefined) return '-'
  if (bytes === 0) return '0 B'

  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))

  return (bytes / Math.pow(k, i)).toFixed(i > 0 ? 1 : 0) + ' ' + sizes[i]
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

/* =====================
   移动端卡片视图样式
   ===================== */
.mobile-file-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 8px;
}

.mobile-file-card {
  display: flex;
  align-items: center;
  padding: 12px;
  background: #f9f9f9;
  border-radius: 12px;
  cursor: pointer;
  transition: all 0.2s;
  gap: 12px;
}

.mobile-file-card:active {
  background: #f0f0f0;
  transform: scale(0.98);
}

.mobile-file-card.is-selected {
  background: var(--el-color-primary-light-9);
  border: 1px solid var(--el-color-primary-light-7);
}

.mobile-file-card.is-selected:active {
  background: var(--el-color-primary-light-8);
}

.mobile-file-card.is-disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.mobile-file-card.is-directory {
  background: #fffbf0;
}

.mobile-file-card.is-directory:active {
  background: #fff3d9;
}

.card-checkbox {
  flex-shrink: 0;
}

.card-main {
  display: flex;
  align-items: center;
  gap: 12px;
  flex: 1;
  min-width: 0;
}

.file-card-icon {
  flex-shrink: 0;
}

.file-card-info {
  flex: 1;
  min-width: 0;
}

.file-card-name {
  font-size: 15px;
  font-weight: 500;
  color: #333;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  margin-bottom: 4px;
}

.file-card-meta {
  font-size: 12px;
  color: #909399;
  display: flex;
  align-items: center;
  gap: 4px;
}

.meta-divider {
  color: #dcdfe6;
}
</style>