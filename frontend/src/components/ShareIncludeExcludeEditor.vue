<template>
  <div class="share-include-exclude-editor">
    <!-- 同步路径（include_paths） -->
    <div class="field-block">
      <div class="field-label">同步路径</div>
      <div class="path-editor">
        <div v-if="includePaths.length === 0" class="path-empty">
          不填则同步整个分享；填写后只同步勾选的子路径（前缀匹配）。
        </div>
        <div v-else class="path-tags">
          <el-tag
            v-for="(p, i) in includePaths"
            :key="p + i"
            closable
            @close="removeInclude(i)"
            style="margin: 2px 4px 2px 0"
          >{{ p }}</el-tag>
        </div>
        <div class="path-actions">
          <el-tooltip
            :disabled="ownerLoggedIn !== false"
            content="订阅所属账号未登录，请先登录该账号再预览目录树"
            placement="top"
          >
            <span>
              <el-button
                size="small"
                :icon="FolderOpened"
                :loading="previewing"
                :disabled="!shareUrl || ownerLoggedIn === false"
                @click="openPicker"
              >从分享浏览</el-button>
            </span>
          </el-tooltip>
        </div>
        <div v-if="ownerLoggedIn === false" class="owner-offline-hint">
          订阅所属账号未登录，目录树预览不可用，请先登录该账号
        </div>
      </div>
    </div>

    <!-- 排除规则（exclude_patterns） -->
    <div class="field-block">
      <div class="field-label">排除规则</div>
      <div class="path-editor">
        <div class="path-tags">
          <el-tag
            v-for="(p, i) in excludePatterns"
            :key="p + i"
            closable
            type="info"
            @close="removeExclude(i)"
            style="margin: 2px 4px 2px 0"
          >{{ p }}</el-tag>
          <span v-if="excludePatterns.length === 0" class="path-empty-inline">
            支持 glob：*.tmp、sample.*、*广告* 等
          </span>
        </div>
        <el-input
          v-model="excludeInput"
          size="small"
          placeholder="添加排除规则，按回车确认"
          style="width: 280px; margin-top: 4px"
          @keyup.enter="addExclude"
        />
      </div>
    </div>

    <!-- 从分享选择子路径：复用转存同款 ShareFileSelector -->
    <el-dialog
      v-model="pickerVisible"
      title="从分享中选择要同步的子路径"
      width="680px"
      :close-on-click-modal="false"
      append-to-body
    >
      <ShareFileSelector
        :files="previewFiles"
        :loading="previewing"
        :share-info="previewShareInfo"
        :share-url="shareUrl"
        :share-password="password || undefined"
        @update:selected-files="onSelectedFiles"
      />
      <template #footer>
        <span class="picker-hint">已选 {{ pendingSelected.length }} 项</span>
        <el-button @click="pickerVisible = false">取消</el-button>
        <el-button type="primary" @click="confirmPicker">确定</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { ElMessage } from 'element-plus'
import type { AxiosError } from 'axios'
import { FolderOpened } from '@element-plus/icons-vue'
import ShareFileSelector from './ShareFileSelector.vue'
import {
  previewShareFiles,
  type SharedFileInfo,
  type PreviewShareInfo,
} from '@/api/transfer'

const props = defineProps<{
  shareUrl: string
  password?: string | null
  includePaths: string[]
  excludePatterns: string[]
  // 订阅所属账号：编辑订阅时传订阅自己的 owner_uid，预览目录树按该账号路由 client
  // （不传则后端回退当前 active 账号——转存对话框创建场景 owner=active）
  ownerUid?: number | null
  // 订阅所属账号是否已登录：显式 false 时禁用目录树预览并提示登录该账号。
  // undefined（创建场景）= 默认按已登录处理。
  ownerLoggedIn?: boolean
}>()

const emit = defineEmits<{
  'update:includePaths': [value: string[]]
  'update:excludePatterns': [value: string[]]
}>()

const excludeInput = ref('')

// 分享文件选择器（与转存对话框同款体验）
const pickerVisible = ref(false)
const previewing = ref(false)
const previewFiles = ref<SharedFileInfo[]>([])
const previewShareInfo = ref<PreviewShareInfo | null>(null)
const pendingSelected = ref<SharedFileInfo[]>([])

function normalizePath(v: string): string {
  const s = v.trim().replace(/\/+/g, '/')
  if (!s) return ''
  const prefixed = s.startsWith('/') ? s : `/${s}`
  if (prefixed.length === 1) return '/'
  return prefixed.endsWith('/') ? prefixed.slice(0, -1) : prefixed
}

function removeInclude(i: number) {
  const next = [...props.includePaths]
  next.splice(i, 1)
  emit('update:includePaths', next)
}

function addExclude() {
  const v = excludeInput.value.trim()
  if (!v) return
  if (!props.excludePatterns.includes(v)) {
    emit('update:excludePatterns', [...props.excludePatterns, v])
  }
  excludeInput.value = ''
}

function removeExclude(i: number) {
  const next = [...props.excludePatterns]
  next.splice(i, 1)
  emit('update:excludePatterns', next)
}

async function openPicker() {
  if (!props.shareUrl || !props.shareUrl.trim()) {
    ElMessage.warning('请先填写分享链接')
    return
  }
  if (props.ownerLoggedIn === false) {
    ElMessage.warning('订阅所属账号未登录，请先登录该账号再预览目录树')
    return
  }
  previewing.value = true
  try {
    const resp = await previewShareFiles({
      share_url: props.shareUrl.trim(),
      password: props.password || undefined,
    })
    previewFiles.value = resp.files || []
    previewShareInfo.value = resp.share_info ?? null
    pendingSelected.value = []
    pickerVisible.value = true
  } catch (e) {
    const ax = e as AxiosError<{ message?: string; error?: string }>
    ElMessage.error(
      ax?.response?.data?.message ||
        ax?.response?.data?.error ||
        (e as Error)?.message ||
        '加载分享文件失败',
    )
  } finally {
    previewing.value = false
  }
}

function onSelectedFiles(files: SharedFileInfo[]) {
  pendingSelected.value = files
}

function confirmPicker() {
  const selectedPaths = pendingSelected.value
    .map((f) => normalizePath(f.path))
    .filter((p) => p.length > 0)
  // 与已有路径合并去重；勾选目录即代表整棵子树，裁剪掉被祖先覆盖的冗余后代
  const merged: string[] = []
  const seen = new Set<string>()
  for (const p of [...props.includePaths, ...selectedPaths]) {
    if (seen.has(p)) continue
    seen.add(p)
    merged.push(p)
  }
  const trimmed = merged.filter(
    (p) => !merged.some((a) => a !== p && p.startsWith(`${a}/`)),
  )
  emit('update:includePaths', trimmed)
  pickerVisible.value = false
}
</script>

<style scoped lang="scss">
.field-block {
  margin-bottom: 12px;
}

.field-label {
  font-size: 13px;
  color: var(--el-text-color-regular);
  margin-bottom: 6px;
}

.path-editor {
  width: 100%;
}

.path-empty {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-bottom: 6px;
}

.owner-offline-hint {
  font-size: 12px;
  color: var(--el-color-warning);
  margin-top: 6px;
}

.path-empty-inline {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.path-tags {
  margin-bottom: 6px;
}

.path-actions {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 4px;
}

.picker-hint {
  margin-right: auto;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}
</style>
