<template>
  <div class="settings-container">
    <el-container>
      <!-- 顶部标题 -->
      <el-header height="60px" class="header">
        <h2>系统设置</h2>
        <div class="header-actions">
          <el-button @click="handleReset" :loading="resetting">
            <el-icon><RefreshLeft /></el-icon>
            恢复推荐配置
          </el-button>
          <el-button type="primary" @click="handleSave" :loading="saving">
            <el-icon><Check /></el-icon>
            保存设置
          </el-button>
        </div>
      </el-header>

      <!-- 设置内容 -->
      <el-main>
        <el-skeleton :loading="loading" :rows="8" animated>
          <el-form
            v-if="formData"
            ref="formRef"
            :model="formData"
            :rules="rules"
            label-width="140px"
            label-position="left"
          >
            <!-- 服务器配置 -->
            <el-card class="setting-card" shadow="hover">
              <template #header>
                <div class="card-header">
                  <el-icon :size="20" color="#409eff">
                    <Monitor />
                  </el-icon>
                  <span>服务器配置</span>
                </div>
              </template>

              <el-form-item label="监听地址" prop="server.host">
                <el-input
                  v-model="formData.server.host"
                  placeholder="例如: 127.0.0.1"
                  clearable
                >
                  <template #prepend>
                    <el-icon><Connection /></el-icon>
                  </template>
                </el-input>
                <div class="form-tip">服务器监听的IP地址</div>
              </el-form-item>

              <el-form-item label="监听端口" prop="server.port">
                <el-input-number
                  v-model="formData.server.port"
                  :min="1"
                  :max="65535"
                  :step="1"
                  controls-position="right"
                  style="width: 100%"
                />
                <div class="form-tip">服务器监听的端口号，修改后需要重启服务器</div>
              </el-form-item>
            </el-card>

            <!-- 下载配置 -->
            <el-card class="setting-card" shadow="hover">
              <template #header>
                <div class="card-header">
                  <el-icon :size="20" color="#67c23a">
                    <Download />
                  </el-icon>
                  <span>下载配置</span>
                </div>
              </template>

              <!-- VIP 等级信息 -->
              <el-alert
                v-if="recommended"
                :title="`您的会员等级: ${recommended.vip_name}`"
                type="info"
                :closable="false"
                style="margin-bottom: 20px"
              >
                <template #default>
                  <div class="vip-info">
                    <div class="vip-item">
                      <el-icon><User /></el-icon>
                      <span>推荐线程数: {{ recommended.recommended.threads }} 个</span>
                    </div>
                    <div class="vip-item">
                      <el-icon><Files /></el-icon>
                      <span>推荐分片大小: {{ recommended.recommended.chunk_size }} MB</span>
                    </div>
                    <div class="vip-item">
                      <el-icon><Download /></el-icon>
                      <span>最大同时下载: {{ recommended.recommended.max_tasks }} 个文件</span>
                    </div>
                  </div>
                </template>
              </el-alert>

              <!-- 警告提示 -->
              <el-alert
                v-if="recommended && recommended.warnings && recommended.warnings.length > 0"
                type="warning"
                :closable="false"
                style="margin-bottom: 20px"
              >
                <template #default>
                  <div v-for="(warning, index) in recommended.warnings" :key="index">
                    <div style="white-space: pre-wrap">{{ warning }}</div>
                  </div>
                </template>
              </el-alert>

              <el-form-item label="下载目录" prop="download.download_dir">
                <el-input
                  v-model="formData.download.download_dir"
                  placeholder="例如: downloads"
                  clearable
                >
                  <template #prepend>
                    <el-icon><Folder /></el-icon>
                  </template>
                </el-input>
                <div class="form-tip">文件下载的保存目录，支持相对路径和绝对路径</div>
              </el-form-item>

              <el-form-item label="全局最大线程数" prop="download.max_global_threads">
                <el-slider
                  v-model="formData.download.max_global_threads"
                  :min="1"
                  :max="20"
                  :step="1"
                  :marks="threadMarks"
                  show-stops
                  style="width: calc(100% - 20px); margin-right: 20px"
                />
                <div class="value-display">
                  当前: {{ formData.download.max_global_threads }} 个
                  <span v-if="recommended" class="recommend-hint">
                    (推荐: {{ recommended.recommended.threads }} 个)
                  </span>
                </div>
                <div class="form-tip">
                  <el-icon><InfoFilled /></el-icon>
                  所有下载任务共享的线程池大小，单文件可使用全部线程进行分片下载
                </div>
                <div class="form-tip warning-tip" v-if="formData.download.max_global_threads > 10 && recommended && recommended.vip_type === 0">
                  ⚠️ 警告：普通用户建议保持1个线程，调大可能触发限速！
                </div>
              </el-form-item>

              <el-form-item label="最大同时下载数" prop="download.max_concurrent_tasks">
                <el-slider
                  v-model="formData.download.max_concurrent_tasks"
                  :min="1"
                  :max="10"
                  :step="1"
                  :marks="taskMarks"
                  show-stops
                  style="width: calc(100% - 20px); margin-right: 20px"
                />
                <div class="value-display">
                  当前: {{ formData.download.max_concurrent_tasks }} 个
                  <span v-if="recommended" class="recommend-hint">
                    (推荐: {{ recommended.recommended.max_tasks }} 个)
                  </span>
                </div>
                <div class="form-tip">
                  可以同时进行下载的文件数量上限
                </div>
              </el-form-item>

              <!-- 分片大小说明（自适应，不可配置） -->
              <el-alert
                title="智能分片大小"
                type="success"
                :closable="false"
                style="margin-bottom: 20px"
              >
                <template #default>
                  <div style="line-height: 1.8">
                    系统会根据文件大小和您的VIP等级自动选择最优分片大小：<br/>
                    • 小文件（<5MB）使用 256KB 分片<br/>
                    • 中等文件（5-10MB）使用 512KB 分片<br/>
                    • 中大型文件（10-500MB）使用 1MB-4MB 分片<br/>
                    • 大文件（≥500MB）使用 5MB 分片<br/>
                    • VIP限制：普通用户最高4MB，普通会员最高4MB，SVIP最高5MB<br/>
                    • 注意：百度网盘限制单个Range请求最大5MB，超过会返回403错误
                  </div>
                </template>
              </el-alert>

              <el-form-item label="最大重试次数" prop="download.max_retries">
                <el-input-number
                  v-model="formData.download.max_retries"
                  :min="0"
                  :max="10"
                  :step="1"
                  controls-position="right"
                  style="width: 100%"
                />
                <div class="form-tip">下载分片失败后的重试次数，0 表示不重试</div>
              </el-form-item>
            </el-card>

            <!-- 关于信息 -->
            <el-card class="setting-card" shadow="hover">
              <template #header>
                <div class="card-header">
                  <el-icon :size="20" color="#909399">
                    <InfoFilled />
                  </el-icon>
                  <span>关于</span>
                </div>
              </template>

              <div class="about-content">
                <div class="about-item">
                  <span class="label">项目名称:</span>
                  <span class="value">百度网盘 Rust 客户端</span>
                </div>
                <div class="about-item">
                  <span class="label">版本:</span>
                  <span class="value">v1.0.0</span>
                </div>
                <div class="about-item">
                  <span class="label">后端技术:</span>
                  <span class="value">Rust + Axum + Tokio</span>
                </div>
                <div class="about-item">
                  <span class="label">前端技术:</span>
                  <span class="value">Vue 3 + TypeScript + Element Plus</span>
                </div>
                <div class="about-item">
                  <span class="label">许可证:</span>
                  <span class="value">MIT License</span>
                </div>
              </div>
            </el-card>
          </el-form>
        </el-skeleton>
      </el-main>
    </el-container>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'
import { useConfigStore } from '@/stores/config'
import type { AppConfig } from '@/api/config'
import { getRecommendedConfig, resetToRecommended } from '@/api/config'
import {
  Check,
  RefreshLeft,
  Monitor,
  Connection,
  Download,
  Folder,
  InfoFilled,
  User,
  Files,
} from '@element-plus/icons-vue'

const configStore = useConfigStore()

// 状态
const loading = ref(false)
const saving = ref(false)
const resetting = ref(false)
const formRef = ref<FormInstance>()
const formData = ref<AppConfig | null>(null)
const recommended = ref<any>(null)

// 滑块标记
const threadMarks = reactive({
  1: '1',
  5: '5',
  10: '10',
  15: '15',
  20: '20',
})

const taskMarks = reactive({
  1: '1',
  3: '3',
  5: '5',
  7: '7',
  10: '10',
})

// 表单验证规则
const rules = reactive<FormRules<AppConfig>>({
  'server.host': [
    { required: true, message: '请输入监听地址', trigger: 'blur' },
  ],
  'server.port': [
    { required: true, message: '请输入监听端口', trigger: 'blur' },
    { type: 'number', min: 1, max: 65535, message: '端口范围: 1-65535', trigger: 'blur' },
  ],
  'download.download_dir': [
    { required: true, message: '请输入下载目录', trigger: 'blur' },
  ],
  'download.max_global_threads': [
    { required: true, message: '请选择全局最大线程数', trigger: 'change' },
    { type: 'number', min: 1, max: 20, message: '线程数范围: 1-20', trigger: 'change' },
  ],
  'download.max_concurrent_tasks': [
    { required: true, message: '请选择最大同时下载数', trigger: 'change' },
    { type: 'number', min: 1, max: 10, message: '同时下载数范围: 1-10', trigger: 'change' },
  ],
  'download.max_retries': [
    { required: true, message: '请输入最大重试次数', trigger: 'blur' },
    { type: 'number', min: 0, max: 10, message: '重试次数范围: 0-10', trigger: 'blur' },
  ],
})

// 加载配置
async function loadConfig() {
  loading.value = true
  try {
    const config = await configStore.fetchConfig()
    formData.value = JSON.parse(JSON.stringify(config)) // 深拷贝
    
    // 同时加载推荐配置
    try {
      recommended.value = await getRecommendedConfig()
    } catch (error) {
      console.warn('获取推荐配置失败:', error)
    }
  } catch (error: any) {
    ElMessage.error('加载配置失败: ' + (error.message || '未知错误'))
  } finally {
    loading.value = false
  }
}

// 恢复推荐配置
async function handleReset() {
  try {
    await ElMessageBox.confirm(
      '确定要恢复为推荐配置吗？这将根据您的VIP等级应用最佳配置。',
      '提示',
      {
        confirmButtonText: '确定',
        cancelButtonText: '取消',
        type: 'warning',
      }
    )
    
    resetting.value = true
    await resetToRecommended()
    ElMessage.success('已恢复为推荐配置')
    
    // 重新加载配置
    await loadConfig()
  } catch (error: any) {
    if (error !== 'cancel') {
      ElMessage.error('恢复配置失败: ' + (error.message || '未知错误'))
    }
  } finally {
    resetting.value = false
  }
}

// 保存配置
async function handleSave() {
  if (!formRef.value || !formData.value) return

  try {
    // 验证表单
    await formRef.value.validate()

    saving.value = true
    await configStore.saveConfig(formData.value)
    ElMessage.success('配置已保存')
    
    // 重新加载推荐配置以更新警告
    try {
      recommended.value = await getRecommendedConfig()
    } catch (error) {
      console.warn('更新推荐配置失败:', error)
    }
  } catch (error: any) {
    if (error instanceof Error) {
      ElMessage.error('保存配置失败: ' + error.message)
    }
  } finally {
    saving.value = false
  }
}

// 组件挂载
onMounted(() => {
  loadConfig()
})
</script>

<style scoped lang="scss">
.settings-container {
  width: 100%;
  height: 100vh;
  background: #f5f5f5;
}

.el-container {
  height: 100%;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: white;
  border-bottom: 1px solid #e0e0e0;
  padding: 0 20px;

  h2 {
    margin: 0;
    font-size: 20px;
    color: #333;
  }

  .header-actions {
    display: flex;
    gap: 10px;
  }
}

.el-main {
  padding: 20px;
  overflow: auto;
}

.setting-card {
  margin-bottom: 20px;

  .card-header {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 16px;
    font-weight: 600;
    color: #333;
  }
}

.form-tip {
  margin-top: 4px;
  font-size: 12px;
  color: #999;
  line-height: 1.5;
}

.value-display {
  margin-top: 8px;
  font-size: 14px;
  font-weight: 600;
  color: #409eff;
  text-align: right;

  .recommend-hint {
    font-size: 12px;
    font-weight: normal;
    color: #67c23a;
    margin-left: 8px;
  }
}

.warning-tip {
  color: #e6a23c !important;
  font-weight: 600;
  margin-top: 8px;
}

.vip-info {
  display: flex;
  gap: 20px;
  flex-wrap: wrap;
  margin-top: 10px;

  .vip-item {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 13px;
    color: #606266;

    .el-icon {
      color: #409eff;
    }
  }
}

.about-content {
  .about-item {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 0;
    border-bottom: 1px solid #f0f0f0;

    &:last-child {
      border-bottom: none;
    }

    .label {
      font-size: 14px;
      color: #666;
    }

    .value {
      font-size: 14px;
      font-weight: 500;
      color: #333;
    }
  }
}

:deep(.el-slider__marks-text) {
  font-size: 11px;
}
</style>
