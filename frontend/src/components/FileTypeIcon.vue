<template>
  <span class="file-type-icon" :style="{ width: `${size}px`, height: `${size}px` }" aria-hidden="true">
    <!-- 文件夹 -->
    <svg v-if="category === 'folder'" viewBox="0 0 24 24" fill="none">
      <path d="M2.5 6.2C2.5 5.0 3.4 4.1 4.6 4.1H9.3L11.2 6.2H19.4C20.6 6.2 21.5 7.1 21.5 8.3V9.5H2.5V6.2Z" fill="#f59e0b" />
      <rect x="2.5" y="7.5" width="19" height="12.4" rx="2.3" fill="#fbbf24" />
      <path d="M2.5 9.8C2.5 8.9 3.2 8.2 4.1 8.2H19.9C20.8 8.2 21.5 8.9 21.5 9.8V10.6H2.5V9.8Z" fill="#fcd34d" opacity="0.65" />
    </svg>

    <!-- 视频 -->
    <svg v-else-if="category === 'video'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#6366f1" />
      <path d="M10 8.5L16.5 12L10 15.5V8.5Z" fill="#ffffff" />
    </svg>

    <!-- 音频 -->
    <svg v-else-if="category === 'audio'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#10b981" />
      <path d="M14.5 7.5V14.3A2.2 2.2 0 1 1 13 12.2V9.3L9.8 10.1V15.3A2.2 2.2 0 1 1 8.3 13.2V9.1C8.3 8.6 8.6 8.2 9.1 8.1L13.7 6.9C14.1 6.8 14.5 7.1 14.5 7.5Z" fill="#ffffff" />
    </svg>

    <!-- 图片 -->
    <svg v-else-if="category === 'image'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#ec4899" />
      <circle cx="8.8" cy="9" r="1.7" fill="#ffffff" />
      <path d="M5.5 17L10 12.3L13 15.2L15.4 12.8L18.5 17H5.5Z" fill="#ffffff" />
    </svg>

    <!-- 压缩包 -->
    <svg v-else-if="category === 'archive'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#d97706" />
      <rect x="10.5" y="4.5" width="3" height="2" rx="0.4" fill="#fef3c7" />
      <rect x="10.5" y="7.5" width="3" height="2" rx="0.4" fill="#fef3c7" />
      <rect x="10.5" y="10.5" width="3" height="2" rx="0.4" fill="#fef3c7" />
      <rect x="9.6" y="13.5" width="4.8" height="4.8" rx="1" fill="#ffffff" />
      <rect x="11.1" y="15.1" width="1.8" height="1.6" rx="0.3" fill="#d97706" />
    </svg>

    <!-- 字幕 -->
    <svg v-else-if="category === 'subtitle'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#0ea5e9" />
      <rect x="6" y="12.5" width="12" height="2" rx="1" fill="#ffffff" />
      <rect x="7.5" y="15.8" width="9" height="1.8" rx="0.9" fill="#e0f2fe" />
      <path d="M10.4 7.5C9.2 7.5 8.3 8.3 8.3 9.4C8.3 10.5 9.2 11.3 10.4 11.3H11.2V10.1H10.4C9.8 10.1 9.5 9.8 9.5 9.4C9.5 9 9.8 8.7 10.4 8.7H11.2V7.5H10.4ZM14.6 7.5C13.4 7.5 12.5 8.3 12.5 9.4C12.5 10.5 13.4 11.3 14.6 11.3H15.4V10.1H14.6C14 10.1 13.7 9.8 13.7 9.4C13.7 9 14 8.7 14.6 8.7H15.4V7.5H14.6Z" fill="#ffffff" />
    </svg>

    <!-- PDF -->
    <svg v-else-if="category === 'pdf'" viewBox="0 0 24 24" fill="none">
      <path d="M5.5 3H14.5L19 7.5V19.5C19 20.3 18.3 21 17.5 21H5.5C4.7 21 4 20.3 4 19.5V4.5C4 3.7 4.7 3 5.5 3Z" fill="#ef4444" />
      <path d="M14.5 3V7.5H19L14.5 3Z" fill="#fca5a5" />
      <rect x="6.5" y="11" width="10" height="1.8" rx="0.9" fill="#ffffff" />
      <rect x="6.5" y="14.2" width="7.5" height="1.8" rx="0.9" fill="#fee2e2" />
    </svg>

    <!-- Word 文档 -->
    <svg v-else-if="category === 'word'" viewBox="0 0 24 24" fill="none">
      <path d="M5.5 3H14.5L19 7.5V19.5C19 20.3 18.3 21 17.5 21H5.5C4.7 21 4 20.3 4 19.5V4.5C4 3.7 4.7 3 5.5 3Z" fill="#2563eb" />
      <path d="M14.5 3V7.5H19L14.5 3Z" fill="#93c5fd" />
      <path d="M7.2 10.5L8.7 16.5H9.9L11.5 12.2L13.1 16.5H14.3L15.8 10.5H14.4L13.5 14.4L12.1 10.5H10.9L9.5 14.4L8.6 10.5H7.2Z" fill="#ffffff" />
    </svg>

    <!-- Excel 表格 -->
    <svg v-else-if="category === 'excel'" viewBox="0 0 24 24" fill="none">
      <path d="M5.5 3H14.5L19 7.5V19.5C19 20.3 18.3 21 17.5 21H5.5C4.7 21 4 20.3 4 19.5V4.5C4 3.7 4.7 3 5.5 3Z" fill="#16a34a" />
      <path d="M14.5 3V7.5H19L14.5 3Z" fill="#86efac" />
      <rect x="7" y="10.5" width="9" height="6.8" rx="0.8" stroke="#ffffff" stroke-width="1.3" />
      <path d="M7 13.8H16M11.5 10.5V17.3" stroke="#ffffff" stroke-width="1.2" />
    </svg>

    <!-- PPT 演示 -->
    <svg v-else-if="category === 'ppt'" viewBox="0 0 24 24" fill="none">
      <path d="M5.5 3H14.5L19 7.5V19.5C19 20.3 18.3 21 17.5 21H5.5C4.7 21 4 20.3 4 19.5V4.5C4 3.7 4.7 3 5.5 3Z" fill="#ea580c" />
      <path d="M14.5 3V7.5H19L14.5 3Z" fill="#fdba74" />
      <rect x="7" y="10.8" width="9" height="5.8" rx="1" fill="#ffffff" />
      <rect x="10.7" y="16.6" width="1.6" height="1.8" fill="#ffffff" />
    </svg>

    <!-- 纯文本 / Markdown -->
    <svg v-else-if="category === 'text'" viewBox="0 0 24 24" fill="none">
      <path d="M5.5 3H14.5L19 7.5V19.5C19 20.3 18.3 21 17.5 21H5.5C4.7 21 4 20.3 4 19.5V4.5C4 3.7 4.7 3 5.5 3Z" fill="#0d9488" />
      <path d="M14.5 3V7.5H19L14.5 3Z" fill="#5eead4" />
      <rect x="7" y="10.5" width="9" height="1.6" rx="0.8" fill="#ffffff" />
      <rect x="7" y="13.5" width="9" height="1.6" rx="0.8" fill="#ffffff" />
      <rect x="7" y="16.5" width="6" height="1.6" rx="0.8" fill="#ccfbf1" />
    </svg>

    <!-- 代码 / 配置 -->
    <svg v-else-if="category === 'code'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#475569" />
      <path d="M9.6 8.8L6.4 12L9.6 15.2M14.4 8.8L17.6 12L14.4 15.2M13 8L11 16" stroke="#ffffff" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" />
    </svg>

    <!-- 安装包 / 应用 -->
    <svg v-else-if="category === 'app'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#14b8a6" />
      <path d="M12 6.8L16.8 9.4V14.6L12 17.2L7.2 14.6V9.4L12 6.8Z" stroke="#ffffff" stroke-width="1.6" stroke-linejoin="round" />
      <path d="M7.2 9.4L12 12L16.8 9.4M12 12V17.2" stroke="#ffffff" stroke-width="1.5" stroke-linejoin="round" />
    </svg>

    <!-- 种子 -->
    <svg v-else-if="category === 'torrent'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#7c3aed" />
      <path d="M12 7V14.2M12 14.2L8.8 11M12 14.2L15.2 11M7.5 16.8H16.5" stroke="#ffffff" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" />
    </svg>

    <!-- 电子书 -->
    <svg v-else-if="category === 'ebook'" viewBox="0 0 24 24" fill="none">
      <rect x="3" y="3" width="18" height="18" rx="4" fill="#8b5cf6" />
      <path d="M7.5 8C9 7.4 10.6 7.7 12 8.7C13.4 7.7 15 7.4 16.5 8V16C15 15.4 13.4 15.7 12 16.7C10.6 15.7 9 15.4 7.5 16V8Z" fill="#ffffff" />
      <path d="M12 8.7V16.7" stroke="#8b5cf6" stroke-width="1.2" />
    </svg>

    <!-- 默认文件 -->
    <svg v-else viewBox="0 0 24 24" fill="none">
      <path d="M5.5 3H14.5L19 7.5V19.5C19 20.3 18.3 21 17.5 21H5.5C4.7 21 4 20.3 4 19.5V4.5C4 3.7 4.7 3 5.5 3Z" fill="#94a3b8" />
      <path d="M14.5 3V7.5H19L14.5 3Z" fill="#cbd5e1" />
      <rect x="7" y="11" width="9" height="1.5" rx="0.75" fill="#ffffff" />
      <rect x="7" y="14" width="9" height="1.5" rx="0.75" fill="#ffffff" />
      <rect x="7" y="17" width="6" height="1.5" rx="0.75" fill="#f1f5f9" />
    </svg>
  </span>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { getFileCategory } from '@/utils/fileIcon'

const props = withDefaults(
  defineProps<{
    name: string
    isDir?: boolean
    size?: number
  }>(),
  {
    isDir: false,
    size: 20,
  }
)

const category = computed(() => getFileCategory(props.name, props.isDir))
</script>

<style scoped>
.file-type-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  line-height: 1;
}

.file-type-icon svg {
  width: 100%;
  height: 100%;
  display: block;
}
</style>
