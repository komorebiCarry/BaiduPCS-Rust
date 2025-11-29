<template>
  <tr
    class="file-item"
    :class="{
      'is-selected': selected,
      'is-disabled': disabled,
      'is-directory': entry.entryType === 'directory'
    }"
    @click="emit('click', entry)"
    @dblclick="emit('dblclick', entry)"
  >
    <td class="col-name">
      <div class="name-content">
        <el-icon class="file-icon" :class="iconClass">
          <component :is="iconComponent" />
        </el-icon>
        <span class="file-name" :title="entry.name">{{ entry.name }}</span>
      </div>
    </td>
    <td class="col-time">{{ formatTime(entry.updatedAt) }}</td>
    <td class="col-type">{{ typeText }}</td>
    <td class="col-size">{{ formatSize(entry.size) }}</td>
  </tr>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { FileEntry } from '@/api/filesystem'
import {
  Folder,
  Document,
  Picture,
  VideoPlay,
  Headset,
  Files,
  Monitor,
} from '@element-plus/icons-vue'

const props = defineProps<{
  entry: FileEntry
  selected: boolean
  disabled: boolean
}>()

const emit = defineEmits<{
  'click': [entry: FileEntry]
  'dblclick': [entry: FileEntry]
}>()

// 图标组件
const iconComponent = computed(() => {
  if (props.entry.entryType === 'directory') {
    // 检查是否是驱动器
    if (/^[A-Za-z]:$/.test(props.entry.name) || props.entry.name.includes(':')) {
      return Monitor
    }
    return Folder
  }

  const ext = props.entry.name.split('.').pop()?.toLowerCase()

  // 图片类型
  if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'svg', 'ico'].includes(ext || '')) {
    return Picture
  }

  // 视频类型
  if (['mp4', 'avi', 'mkv', 'mov', 'wmv', 'flv', 'webm'].includes(ext || '')) {
    return VideoPlay
  }

  // 音频类型
  if (['mp3', 'wav', 'flac', 'aac', 'ogg', 'm4a'].includes(ext || '')) {
    return Headset
  }

  // 压缩文件
  if (['zip', 'rar', '7z', 'tar', 'gz', 'bz2'].includes(ext || '')) {
    return Files
  }

  return Document
})

// 图标样式类
const iconClass = computed(() => {
  if (props.entry.entryType === 'directory') {
    return 'icon-folder'
  }
  return 'icon-file'
})

// 类型文本
const typeText = computed(() => {
  if (props.entry.entryType === 'directory') {
    // 检查是否是驱动器
    if (/^[A-Za-z]:$/.test(props.entry.name)) {
      return '本地磁盘'
    }
    return '文件夹'
  }

  const ext = props.entry.name.split('.').pop()?.toLowerCase()
  if (!ext) return '文件'

  const typeMap: Record<string, string> = {
    // 图片
    jpg: 'JPEG 图像', jpeg: 'JPEG 图像', png: 'PNG 图像', gif: 'GIF 图像',
    bmp: 'BMP 图像', webp: 'WebP 图像', svg: 'SVG 图像',
    // 视频
    mp4: 'MP4 视频', avi: 'AVI 视频', mkv: 'MKV 视频', mov: 'MOV 视频',
    wmv: 'WMV 视频', flv: 'FLV 视频',
    // 音频
    mp3: 'MP3 音频', wav: 'WAV 音频', flac: 'FLAC 音频',
    aac: 'AAC 音频', ogg: 'OGG 音频',
    // 文档
    pdf: 'PDF 文档', doc: 'Word 文档', docx: 'Word 文档',
    xls: 'Excel 表格', xlsx: 'Excel 表格',
    ppt: 'PPT 演示', pptx: 'PPT 演示',
    txt: '文本文件', md: 'Markdown',
    // 代码
    js: 'JavaScript', ts: 'TypeScript', json: 'JSON',
    html: 'HTML', css: 'CSS', vue: 'Vue',
    py: 'Python', java: 'Java', rs: 'Rust', go: 'Go',
    // 压缩
    zip: 'ZIP 压缩', rar: 'RAR 压缩', '7z': '7z 压缩',
    // 可执行
    exe: '可执行文件', msi: '安装程序',
  }

  return typeMap[ext] || `${ext.toUpperCase()} 文件`
})

// 格式化时间
function formatTime(isoString: string): string {
  if (!isoString) return '-'
  const date = new Date(isoString)
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

// 格式化大小
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
.file-item {
  cursor: pointer;
  transition: background-color 0.1s;
}

.file-item:hover {
  background: var(--el-fill-color-light);
}

.file-item.is-selected {
  background: var(--el-color-primary-light-9);
}

.file-item.is-selected:hover {
  background: var(--el-color-primary-light-8);
}

.file-item.is-disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.file-item td {
  padding: 8px 12px;
  font-size: 13px;
  color: var(--el-text-color-regular);
  border-bottom: 1px solid var(--el-border-color-lighter);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.col-name {
  width: 45%;
}

.name-content {
  display: flex;
  align-items: center;
  gap: 8px;
  overflow: hidden;
}

.file-icon {
  flex-shrink: 0;
  font-size: 18px;
}

.icon-folder {
  color: #f0ad4e;
}

.icon-file {
  color: #909399;
}

.file-name {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.col-time {
  width: 25%;
  color: var(--el-text-color-secondary);
}

.col-type {
  width: 15%;
  color: var(--el-text-color-secondary);
}

.col-size {
  width: 15%;
  text-align: right;
  color: var(--el-text-color-secondary);
}
</style>