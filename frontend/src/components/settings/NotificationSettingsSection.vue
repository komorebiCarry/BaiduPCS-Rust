<template>
  <el-card class="setting-card" shadow="hover">
    <template #header>
      <div class="card-header">
        <el-icon :size="20" color="#e6a23c">
          <Bell />
        </el-icon>
        <span>消息通知</span>
      </div>
    </template>

    <el-form-item label="启用通知">
      <el-switch v-model="model.enabled" />
      <div class="form-tip">
        任务进入终态（完成 / 失败）时推送到 Gotify 或任意 Webhook（如企业微信 / 飞书 / Bark / ntfy 等）。推送在后台异步进行，失败只记录日志，不影响任务本身。
      </div>
    </el-form-item>

    <!-- 触发事件 -->
    <el-divider content-position="left">触发事件</el-divider>
    <div class="event-grid">
      <div v-for="group in eventGroups" :key="group.label" class="event-group">
        <div class="event-group-title">{{ group.label }}</div>
        <el-checkbox
            v-for="item in group.items"
            :key="item.key"
            v-model="model.events[item.key]"
            :disabled="!model.enabled"
        >
          {{ item.label }}
        </el-checkbox>
      </div>
    </div>
    <div class="form-tip">
      分享同步仅在「发现新增 / 更新的文件」时通知，没有变化的轮询不打扰；分享同步与自动备份内部产生的转存 / 下载子任务不单独通知；没有任何文件变动的自动备份轮次不通知。
    </div>

    <!-- Gotify -->
    <el-divider content-position="left">Gotify</el-divider>
    <el-form-item label="启用 Gotify">
      <el-switch v-model="model.gotify.enabled" />
    </el-form-item>
    <template v-if="model.gotify.enabled">
      <el-form-item label="服务地址">
        <el-input v-model="model.gotify.server_url" placeholder="例如: https://gotify.example.com" clearable />
      </el-form-item>
      <el-form-item label="App Token">
        <el-input
            v-model="model.gotify.app_token"
            type="password"
            show-password
            placeholder="在 Gotify「Apps」中创建应用获得"
            autocomplete="new-password"
            clearable
        />
      </el-form-item>
      <el-form-item label="优先级">
        <el-input-number v-model="model.gotify.priority" :min="0" :max="10" :step="1" controls-position="right" />
        <div class="form-tip">0-10，数值越大越重要（Gotify 客户端据此决定是否弹出 / 响铃）</div>
      </el-form-item>
    </template>

    <!-- Webhook -->
    <el-divider content-position="left">通用 Webhook</el-divider>
    <el-form-item label="启用 Webhook">
      <el-switch v-model="model.webhook.enabled" />
    </el-form-item>
    <template v-if="model.webhook.enabled">
      <el-form-item label="请求地址">
        <el-input v-model="model.webhook.url" placeholder="https://example.com/hook" clearable>
          <template #prepend>
            <el-select v-model="model.webhook.method" style="width: 90px">
              <el-option value="POST" label="POST" />
              <el-option value="PUT" label="PUT" />
              <el-option value="GET" label="GET" />
            </el-select>
          </template>
        </el-input>
        <div class="form-tip">地址中也可使用占位符（会按 URL 编码替换），GET 请求不发送请求体</div>
      </el-form-item>

      <el-form-item label="请求头">
        <div class="header-list">
          <div v-for="(h, idx) in model.webhook.headers" :key="idx" class="header-row">
            <el-input v-model="h.name" placeholder="名称，如 Authorization" />
            <el-input v-model="h.value" placeholder="值" type="password" show-password autocomplete="new-password" />
            <el-button :icon="Delete" circle plain type="danger" @click="removeHeader(idx)" />
          </div>
          <el-button size="small" :icon="Plus" @click="addHeader">添加请求头</el-button>
        </div>
      </el-form-item>

      <template v-if="model.webhook.method !== 'GET'">
        <el-form-item label="Content-Type">
          <el-input v-model="model.webhook.content_type" placeholder="application/json" />
        </el-form-item>
        <el-form-item label="Body 模板">
          <el-input
              v-model="model.webhook.body_template"
              type="textarea"
              :autosize="{ minRows: 6, maxRows: 16 }"
              class="mono"
          />
          <div class="form-tip">
            可用占位符：<code v-for="p in placeholders" :key="p" class="ph">{{ wrap(p) }}</code>。
            Content-Type 含 json 时，占位符的值会按 JSON 字符串规则转义，请写在引号内；为 application/x-www-form-urlencoded 时按 URL 编码。请求头中的占位符会把换行折叠为空格。
            <el-link type="primary" :underline="false" @click="resetTemplate">恢复默认模板</el-link>
          </div>
        </el-form-item>
      </template>

      <el-form-item label="超时（秒）">
        <el-input-number v-model="model.webhook.timeout_secs" :min="1" :max="60" :step="1" controls-position="right" />
      </el-form-item>
    </template>

    <el-form-item label=" ">
      <el-button
          type="primary"
          plain
          :loading="testing"
          :disabled="!model.gotify.enabled && !model.webhook.enabled"
          @click="handleTest"
      >
        <el-icon v-if="!testing"><Promotion /></el-icon>
        发送测试通知
      </el-button>
      <div class="form-tip" style="width: 100%">
        使用当前表单中（可未保存）的渠道配置发送一条测试消息，不受「启用通知」与事件开关影响。修改后请点击页面右上角「保存」生效。
      </div>
    </el-form-item>

    <el-form-item v-if="testResults.length" label="测试结果">
      <div class="test-results">
        <div v-for="r in testResults" :key="r.channel">
          <el-tag :type="r.success ? 'success' : 'danger'" size="small" effect="light">
            {{ channelLabel(r.channel) }}：{{ r.success ? '发送成功' : '发送失败' }}
          </el-tag>
          <span v-if="!r.success && r.error" class="test-error">{{ r.error }}</span>
        </div>
      </div>
    </el-form-item>
  </el-card>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Bell, Delete, Plus, Promotion } from '@element-plus/icons-vue'
import {
  DEFAULT_WEBHOOK_BODY_TEMPLATE,
  testNotification,
  type ChannelSendResult,
  type NotificationConfig,
  type NotificationEventToggles,
} from '@/api/config'

const model = defineModel<NotificationConfig>({ required: true })

const testing = ref(false)
const testResults = ref<ChannelSendResult[]>([])

const eventGroups: { label: string; items: { key: keyof NotificationEventToggles; label: string }[] }[] = [
  {
    label: '分享同步',
    items: [
      { key: 'share_sync_updated', label: '发现更新并完成转存 / 下载' },
      { key: 'share_sync_failed', label: '运行失败 / 部分失败' },
      { key: 'share_sync_link_invalid', label: '分享链接失效自动暂停' },
    ],
  },
  {
    label: '转存',
    items: [
      { key: 'transfer_completed', label: '转存完成' },
      { key: 'transfer_failed', label: '转存失败' },
    ],
  },
  {
    label: '下载',
    items: [
      { key: 'download_completed', label: '下载完成' },
      { key: 'download_failed', label: '下载失败' },
    ],
  },
  {
    label: '自动备份',
    items: [
      { key: 'backup_completed', label: '备份完成' },
      { key: 'backup_failed', label: '备份失败' },
    ],
  },
]

const placeholders = [
  'event', 'event_label', 'title', 'message', 'level', 'name', 'task_id', 'account_uid', 'time', 'timestamp',
]

function wrap(p: string) {
  return `{{${p}}}`
}

function channelLabel(channel: string) {
  return channel === 'gotify' ? 'Gotify' : channel === 'webhook' ? 'Webhook' : channel
}

function addHeader() {
  model.value.webhook.headers.push({ name: '', value: '' })
}

function removeHeader(idx: number) {
  model.value.webhook.headers.splice(idx, 1)
}

function resetTemplate() {
  model.value.webhook.body_template = DEFAULT_WEBHOOK_BODY_TEMPLATE
}

async function handleTest() {
  testing.value = true
  testResults.value = []
  try {
    // 去掉空名称的请求头，与保存时一致
    const draft: NotificationConfig = JSON.parse(JSON.stringify(model.value))
    draft.webhook.headers = draft.webhook.headers.filter((h) => h.name.trim() !== '')
    const resp = await testNotification(draft)
    testResults.value = resp.results
    if (resp.success) {
      ElMessage.success('测试通知已发送')
    } else {
      ElMessage.warning('部分渠道发送失败，请查看测试结果')
    }
  } catch {
    // 错误提示由 API 拦截器统一弹出
  } finally {
    testing.value = false
  }
}
</script>

<style scoped lang="scss">
.event-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
  gap: 12px 24px;
  margin-bottom: 8px;
}

.event-group {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
}

.event-group-title {
  font-weight: 600;
  font-size: 13px;
  margin-bottom: 4px;
  color: var(--el-text-color-primary);
}

.header-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
  width: 100%;
}

.header-row {
  display: flex;
  gap: 8px;
  align-items: center;
}

.mono :deep(textarea) {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
}

.ph {
  margin: 0 2px;
  padding: 0 4px;
  border-radius: 3px;
  background: var(--el-fill-color-light);
}

.test-results {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.test-error {
  margin-left: 8px;
  font-size: 12px;
  color: var(--el-color-danger);
  word-break: break-all;
}

.form-tip {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  line-height: 1.5;
  margin-top: 4px;
}
</style>
