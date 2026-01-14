<template>
  <el-dialog
    v-model="visible"
    title="个人信息"
    width="400px"
    :close-on-click-modal="true"
    @close="handleClose"
  >
    <div class="profile-content">
      <!-- 头像和基本信息 -->
      <div class="profile-header">
        <el-avatar :size="80" :src="user?.avatar_url">
          <el-icon :size="40"><User /></el-icon>
        </el-avatar>
        <div class="profile-name">
          <h3>{{ user?.nickname || user?.username || '未知用户' }}</h3>
          <el-tag v-if="vipLabel" :type="vipTagType" size="small">
            {{ vipLabel }}
          </el-tag>
        </div>
      </div>

      <!-- 详细信息 -->
      <el-descriptions :column="1" border class="profile-details">
        <el-descriptions-item label="用户名">
          {{ user?.username || '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="UID">
          {{ user?.uid || '-' }}
        </el-descriptions-item>
        <el-descriptions-item label="登录时间">
          {{ loginTimeFormatted }}
        </el-descriptions-item>
      </el-descriptions>

      <!-- 存储空间 -->
      <div v-if="user?.total_space" class="storage-section">
        <div class="storage-header">
          <span>存储空间</span>
          <span class="storage-text">{{ usedSpaceFormatted }} / {{ totalSpaceFormatted }}</span>
        </div>
        <el-progress
          :percentage="storagePercentage"
          :color="storageColor"
          :stroke-width="10"
        />
      </div>
    </div>

    <template #footer>
      <el-button @click="handleClose">关闭</el-button>
    </template>
  </el-dialog>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { User } from '@element-plus/icons-vue'
import type { UserAuth } from '@/api/auth'

const props = defineProps<{
  modelValue: boolean
  user: UserAuth | null
}>()

const emit = defineEmits<{
  'update:modelValue': [value: boolean]
}>()

const visible = computed({
  get: () => props.modelValue,
  set: (val) => emit('update:modelValue', val)
})

// VIP 标签
const vipLabel = computed(() => {
  switch (props.user?.vip_type) {
    case 2: return '超级会员'
    case 1: return '普通会员'
    default: return ''
  }
})

const vipTagType = computed(() => {
  switch (props.user?.vip_type) {
    case 2: return 'warning'
    case 1: return 'success'
    default: return 'info'
  }
})

// 登录时间格式化
const loginTimeFormatted = computed(() => {
  if (!props.user?.login_time) return '-'
  const date = new Date(props.user.login_time * 1000)
  return date.toLocaleString('zh-CN')
})

// 存储空间格式化
function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

const usedSpaceFormatted = computed(() => {
  return formatBytes(props.user?.used_space || 0)
})

const totalSpaceFormatted = computed(() => {
  return formatBytes(props.user?.total_space || 0)
})

const storagePercentage = computed(() => {
  if (!props.user?.total_space) return 0
  return Math.round((props.user.used_space || 0) / props.user.total_space * 100)
})

const storageColor = computed(() => {
  const p = storagePercentage.value
  if (p >= 90) return '#f56c6c'
  if (p >= 70) return '#e6a23c'
  return '#409eff'
})

function handleClose() {
  visible.value = false
}
</script>

<style scoped lang="scss">
.profile-content {
  padding: 0 10px;
}

.profile-header {
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-bottom: 24px;

  .profile-name {
    margin-top: 12px;
    text-align: center;

    h3 {
      margin: 0 0 8px 0;
      font-size: 18px;
      color: #303133;
    }
  }
}

.profile-details {
  margin-bottom: 20px;
}

.storage-section {
  background: #f5f7fa;
  padding: 16px;
  border-radius: 8px;

  .storage-header {
    display: flex;
    justify-content: space-between;
    margin-bottom: 10px;
    font-size: 14px;
    color: #606266;

    .storage-text {
      color: #909399;
    }
  }
}
</style>
