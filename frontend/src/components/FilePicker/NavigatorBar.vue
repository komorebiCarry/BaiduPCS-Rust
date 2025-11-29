<template>
  <div class="navigator-bar">
    <!-- 导航按钮组 -->
    <div class="nav-buttons">
      <el-button
        :icon="ArrowLeft"
        :disabled="!canGoBack"
        circle
        size="small"
        @click="emit('back')"
      />
      <el-button
        :icon="ArrowRight"
        :disabled="!canGoForward"
        circle
        size="small"
        @click="emit('forward')"
      />
      <el-button
        :icon="Top"
        :disabled="!canGoUp"
        circle
        size="small"
        @click="emit('up')"
      />
      <el-button
        :icon="Refresh"
        circle
        size="small"
        @click="emit('refresh')"
      />
    </div>

    <!-- 路径输入框 -->
    <div class="path-input-wrapper">
      <el-input
        v-model="inputPath"
        placeholder="输入路径并按回车跳转"
        clearable
        @keyup.enter="handleNavigate"
        @focus="isEditing = true"
        @blur="handleBlur"
      >
        <template #prefix>
          <el-icon><FolderOpened /></el-icon>
        </template>
      </el-input>
    </div>

    <!-- 面包屑（非编辑状态显示） -->
    <div v-if="!isEditing && breadcrumbs.length > 0" class="breadcrumb-overlay" @click="focusInput">
      <el-breadcrumb separator="/">
        <el-breadcrumb-item
          v-for="(crumb, index) in breadcrumbs"
          :key="index"
          @click.stop="handleCrumbClick(crumb.path)"
        >
          <span class="crumb-item" :class="{ 'is-current': index === breadcrumbs.length - 1 }">
            {{ crumb.name }}
          </span>
        </el-breadcrumb-item>
      </el-breadcrumb>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, nextTick } from 'vue'
import { ArrowLeft, ArrowRight, Top, Refresh, FolderOpened } from '@element-plus/icons-vue'

const props = defineProps<{
  currentPath: string
  canGoBack: boolean
  canGoForward: boolean
  canGoUp: boolean
}>()

const emit = defineEmits<{
  'navigate': [path: string]
  'back': []
  'forward': []
  'up': []
  'refresh': []
}>()

const inputPath = ref('')
const isEditing = ref(false)

// 面包屑数据
const breadcrumbs = computed(() => {
  if (!props.currentPath) {
    return [{ name: '计算机', path: '' }]
  }

  const parts: { name: string; path: string }[] = []

  // 处理 Windows 路径 (C:\xxx) 和 Unix 路径 (/xxx)
  const path = props.currentPath

  // 检测是否是 Windows 驱动器路径
  const isWindowsPath = /^[A-Za-z]:/.test(path)

  if (isWindowsPath) {
    // Windows 路径
    const driveLetter = path.substring(0, 2) // C:
    parts.push({ name: '计算机', path: '' })
    parts.push({ name: driveLetter, path: driveLetter })

    const restPath = path.substring(3) // 去掉 C:\
    if (restPath) {
      const segments = restPath.split(/[\\\/]/).filter(Boolean)
      let currentPath = driveLetter
      for (const segment of segments) {
        currentPath += '\\' + segment
        parts.push({ name: segment, path: currentPath })
      }
    }
  } else {
    // Unix 路径
    parts.push({ name: '根目录', path: '/' })

    const segments = path.split('/').filter(Boolean)
    let currentPath = ''
    for (const segment of segments) {
      currentPath += '/' + segment
      parts.push({ name: segment, path: currentPath })
    }
  }

  return parts
})

// 同步路径到输入框
watch(() => props.currentPath, (newPath) => {
  inputPath.value = newPath
}, { immediate: true })

// 处理导航
function handleNavigate() {
  const path = inputPath.value.trim()
  if (path !== props.currentPath) {
    emit('navigate', path)
  }
  isEditing.value = false
}

// 处理面包屑点击
function handleCrumbClick(path: string) {
  // 空路径（Windows 的"计算机"根目录）会触发 navigate 事件
  // 父组件会处理空路径，直接调用 navigateTo('') 而不是 jumpToPath('')
  if (path !== props.currentPath) {
    emit('navigate', path)
  }
}

// 聚焦输入框
function focusInput() {
  isEditing.value = true
  nextTick(() => {
    const input = document.querySelector('.path-input-wrapper input') as HTMLInputElement
    input?.focus()
    input?.select()
  })
}

// 处理失焦
function handleBlur() {
  // 延迟关闭编辑状态，以便点击事件能够触发
  setTimeout(() => {
    isEditing.value = false
    inputPath.value = props.currentPath
  }, 150)
}
</script>

<style scoped>
.navigator-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 0;
  position: relative;
}

.nav-buttons {
  display: flex;
  gap: 4px;
  flex-shrink: 0;
}

.path-input-wrapper {
  flex: 1;
  position: relative;
}

.breadcrumb-overlay {
  position: absolute;
  left: 108px;
  right: 0;
  top: 50%;
  transform: translateY(-50%);
  background: var(--el-fill-color-blank);
  padding: 0 12px 0 32px;
  height: 30px;
  display: flex;
  align-items: center;
  border-radius: 4px;
  cursor: text;
  overflow: hidden;
}

.breadcrumb-overlay :deep(.el-breadcrumb) {
  font-size: 13px;
  white-space: nowrap;
}

.crumb-item {
  cursor: pointer;
  color: var(--el-text-color-regular);
  transition: color 0.2s;
}

.crumb-item:hover {
  color: var(--el-color-primary);
}

.crumb-item.is-current {
  color: var(--el-text-color-primary);
  font-weight: 500;
}
</style>