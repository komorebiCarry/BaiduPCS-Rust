<template>
  <div class="shares-container" :class="{ 'is-mobile': isMobile }">
    <!-- 页面标题和操作栏 -->
    <div class="page-header">
      <div class="header-left">
        <h2 v-if="!isMobile">我的分享</h2>
        <el-tag :type="activeCountType" size="large">
          {{ headerCountText }}
        </el-tag>
      </div>
      <div class="header-right">
        <!-- PC端按钮 -->
        <template v-if="!isMobile">
          <el-button @click="refreshList" :loading="loading">
            <el-icon><Refresh /></el-icon>
            刷新
          </el-button>
          <el-dropdown
              v-if="selectedIds.length > 0"
              class="batch-expire-dropdown"
              trigger="click"
              @command="handleBatchExpire"
          >
            <el-button :loading="batchExpireUpdating">
              <el-icon><Timer /></el-icon>
              批量修改有效期 ({{ selectedIds.length }})
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item
                    v-for="opt in SHARE_EXPIRE_OPTIONS"
                    :key="opt.days"
                    :command="opt.days"
                >
                  {{ opt.label }}
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
          <el-button
              v-if="selectedIds.length > 0"
              type="danger"
              @click="handleBatchCancel"
          >
            <el-icon><Delete /></el-icon>
            批量取消 ({{ selectedIds.length }})
          </el-button>
        </template>
        <!-- 移动端按钮 -->
        <template v-else>
          <el-button circle aria-label="刷新" title="刷新" @click="refreshList" :loading="loading">
            <el-icon><Refresh /></el-icon>
          </el-button>
          <el-dropdown
              v-if="selectedIds.length > 0"
              class="batch-expire-dropdown"
              trigger="click"
              @command="handleBatchExpire"
          >
            <el-button circle :loading="batchExpireUpdating" aria-label="批量修改有效期" title="批量修改有效期">
              <el-icon><Timer /></el-icon>
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item
                    v-for="opt in SHARE_EXPIRE_OPTIONS"
                    :key="opt.days"
                    :command="opt.days"
                >
                  {{ opt.label }}
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
          <el-button
              v-if="selectedIds.length > 0"
              circle
              type="danger"
              aria-label="批量取消分享"
              title="批量取消分享"
              @click="handleBatchCancel"
          >
            <el-icon><Delete /></el-icon>
          </el-button>
        </template>
      </div>
    </div>

    <!-- PC端表格视图 -->
    <el-table
        v-if="!isMobile"
        :data="shareList"
        v-loading="loading"
        @selection-change="handleSelectionChange"
        class="share-table"
        :row-class-name="shareRowClass"
    >
      <el-table-column type="selection" width="50" />
      <el-table-column label="文件" prop="typicalPath" min-width="200">
        <template #default="{ row }">
          <span class="file-name">{{ getFileName(row.typicalPath) }}</span>
        </template>
      </el-table-column>
      <el-table-column label="链接" prop="shortlink" min-width="180">
        <template #default="{ row }">
          <el-link type="primary" :href="row.shortlink" target="_blank">
            {{ row.shortlink }}
          </el-link>
        </template>
      </el-table-column>
      <el-table-column label="浏览" prop="viewCount" width="80" align="center" />
      <el-table-column v-if="showDownloadCount" label="下载" width="80" align="center">
        <template #default="{ row }">{{ row.downloadCount ?? '-' }}</template>
      </el-table-column>
      <el-table-column v-if="showTransferCount" label="保存" width="80" align="center">
        <template #default="{ row }">{{ row.transferCount ?? '-' }}</template>
      </el-table-column>
      <el-table-column label="状态" width="100" align="center">
        <template #default="{ row }">
          <el-tag :type="getStatusType(row)" size="small" class="share-status-tag">
            {{ getStatusText(row) }}
          </el-tag>
        </template>
      </el-table-column>
      <el-table-column label="过期时间" width="160">
        <template #default="{ row }">
          {{ formatExpireTime(row) }}
        </template>
      </el-table-column>
      <el-table-column label="操作" width="360" fixed="right">
        <template #default="{ row }">
          <el-button
              size="small"
              @click="handleGetDetail(row)"
              :icon="Key"
              :disabled="isDeadActionLocked(row)"
              :title="deadActionTitle(row)"
          >
            提取码
          </el-button>
          <el-button
              size="small"
              @click="handleCopyLink(row)"
              :icon="Link"
              :disabled="isDeadActionLocked(row)"
              :title="deadActionTitle(row)"
          >
            复制
          </el-button>
          <el-dropdown
              class="row-expire-dropdown"
              trigger="click"
              :disabled="isExpireLocked(row)"
              @command="(days: ShareExpireDays) => handleModifyExpire(row, days)"
          >
            <el-button
                size="small"
                :icon="Timer"
                :disabled="isExpireLocked(row)"
                :loading="expireUpdatingIds.includes(row.shareId)"
                :title="expireButtonTitle(row)"
            >
              有效期
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item
                    v-for="opt in SHARE_EXPIRE_OPTIONS"
                    :key="opt.days"
                    :command="opt.days"
                >
                  {{ opt.label }}
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
          <el-button size="small" type="danger" @click="handleCancel(row)" :icon="Delete">
            取消
          </el-button>
        </template>
      </el-table-column>
    </el-table>

    <!-- 移动端卡片列表 -->
    <div v-else class="share-card-list">
      <div
          v-for="item in shareList"
          :key="item.shareId"
          class="share-card"
          :class="[{ selected: selectedIds.includes(item.shareId) }, deadShareClass(item)]"
      >
        <div class="card-header" @click="toggleSelect(item.shareId)">
          <el-checkbox
              :model-value="selectedIds.includes(item.shareId)"
              @click.stop
              @change="() => toggleSelect(item.shareId)"
          />
          <span class="file-name">{{ getFileName(item.typicalPath) }}</span>
          <el-tag size="small" :type="getStatusType(item)" class="share-status-tag">
            {{ getStatusText(item) }}
          </el-tag>
        </div>

        <div class="card-body">
          <div class="info-row">
            <span class="label">链接:</span>
            <span class="value link-text">{{ item.shortlink }}</span>
          </div>
          <!-- 浏览 / 下载 / 保存合并一行（接口没返回的计数不显示） -->
          <div class="info-row stats-row">
            <span class="label">统计:</span>
            <span class="value stats-value">
              <span class="stat">浏览 {{ item.viewCount }}</span>
              <span v-if="item.downloadCount != null" class="stat">下载 {{ item.downloadCount }}</span>
              <span v-if="item.transferCount != null" class="stat">保存 {{ item.transferCount }}</span>
            </span>
          </div>
          <div class="info-row expire-row">
            <span class="label">过期:</span>
            <span class="value">{{ formatExpireTime(item) }}</span>
          </div>
        </div>

        <div class="card-actions" @click.stop>
          <el-button
              size="small"
              :icon="Key"
              :disabled="isDeadActionLocked(item)"
              @click="handleGetDetail(item)"
          >提取码</el-button>
          <el-button
              size="small"
              :icon="Link"
              :disabled="isDeadActionLocked(item)"
              @click="handleCopyLink(item)"
          >复制</el-button>
          <el-dropdown
              class="row-expire-dropdown"
              trigger="click"
              :disabled="isExpireLocked(item)"
              @command="(days: ShareExpireDays) => handleModifyExpire(item, days)"
          >
            <el-button
                size="small"
                :icon="Timer"
                :disabled="isExpireLocked(item)"
                :loading="expireUpdatingIds.includes(item.shareId)"
            >
              有效期
            </el-button>
            <template #dropdown>
              <el-dropdown-menu>
                <el-dropdown-item
                    v-for="opt in SHARE_EXPIRE_OPTIONS"
                    :key="opt.days"
                    :command="opt.days"
                >
                  {{ opt.label }}
                </el-dropdown-item>
              </el-dropdown-menu>
            </template>
          </el-dropdown>
          <el-button size="small" type="danger" :icon="Delete" @click="handleCancel(item)">取消</el-button>
        </div>
      </div>

      <!-- 空状态 -->
      <el-empty v-if="shareList.length === 0 && !loading" description="暂无分享记录" />
    </div>

    <!-- 分页：百度 /share/record 不返回总数，用 nextpage（has_more）翻页；
         万一接口给了 total 才用完整分页条 -->
    <div class="pagination-wrapper" v-if="total > 0">
      <el-pagination
          v-model:current-page="currentPage"
          :page-size="pageSize"
          :total="total"
          :layout="isMobile ? 'prev, pager, next' : 'total, prev, pager, next'"
          @current-change="handlePageChange"
      />
    </div>
    <div class="pagination-wrapper simple-pager" v-else-if="currentPage > 1 || hasMore">
      <el-button
          size="small"
          class="pager-prev"
          :disabled="currentPage <= 1 || loading"
          @click="handlePageChange(currentPage - 1)"
      >
        上一页
      </el-button>
      <span class="pager-text">第 {{ currentPage }} 页</span>
      <el-button
          size="small"
          class="pager-next"
          :disabled="!hasMore || loading"
          @click="handlePageChange(currentPage + 1)"
      >
        下一页
      </el-button>
    </div>

    <!-- 分享详情弹窗 - 移动端底部抽屉 -->
    <el-drawer
        v-if="isMobile"
        v-model="detailVisible"
        title="分享详情"
        direction="btt"
        size="auto"
        class="share-detail-drawer"
    >
      <div class="share-detail-mobile">
        <div class="detail-item">
          <span class="label">链接</span>
          <span class="value">{{ currentShareRecord?.shortlink }}</span>
        </div>
        <div class="detail-item">
          <span class="label">提取码</span>
          <span class="value pwd-highlight">{{ currentDetail?.pwd || '无' }}</span>
        </div>
        <div class="action-buttons">
          <el-button type="primary" @click="copyDetailLink" class="block-btn">复制链接</el-button>
          <el-button type="success" @click="copyDetailAll" class="block-btn">复制链接和提取码</el-button>
        </div>
      </div>
    </el-drawer>

    <!-- PC端详情对话框 -->
    <el-dialog v-else v-model="detailVisible" title="分享详情" width="400px">
      <div class="share-detail-pc">
        <div class="detail-row">
          <span class="label">链接:</span>
          <span class="value">{{ currentShareRecord?.shortlink }}</span>
        </div>
        <div class="detail-row">
          <span class="label">提取码:</span>
          <span class="value pwd-highlight">{{ currentDetail?.pwd || '无' }}</span>
        </div>
      </div>
      <template #footer>
        <el-button @click="copyDetailLink" :icon="Link">复制链接</el-button>
        <el-button type="primary" @click="copyDetailAll" :icon="DocumentCopy">复制链接和提取码</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import {
  Refresh,
  Delete,
  Key,
  Link,
  DocumentCopy,
  Timer,
} from '@element-plus/icons-vue'
import { useIsMobile } from '@/utils/responsive'
import { useUiPrefsStore } from '@/stores/uiPrefs'
import {
  deadShareKind,
  getShareList,
  getShareDetail,
  cancelShare,
  modifyShareExpire,
  SHARE_EXPIRE_OPTIONS,
  type ShareExpireDays,
  type ShareExpireItemResult,
  type ShareRecord,
  type ShareDetailData,
} from '@/api/share'

// 响应式检测
const isMobile = useIsMobile()
const uiPrefs = useUiPrefsStore()

// 状态
const loading = ref(false)
const shareList = ref<ShareRecord[]>([])
// 下载 / 保存次数：百度接口返回了才显示对应列（不返回时不臆造为 0）
const showDownloadCount = computed(() => shareList.value.some((r) => r.downloadCount != null))
const showTransferCount = computed(() => shareList.value.some((r) => r.transferCount != null))
// 百度 /share/record 不返回 total（恒为 0），本页条数看 count，翻页看 has_more（nextpage）
const total = ref(0)
const pageCount = ref(0)
const hasMore = ref(false)

// 请求版本号 — 切账号 / 切页 时丢弃旧请求回包
//
// 防御场景：A 账号 getShareList 在路上 → 用户切到 B 账号 → B 的 list 先回 →
// A 的 list 后回会覆盖 B 视图。后续操作按 B active 路由，可能 404 / 误操作 B 资源。
let shareListRequestVersion = 0

// 详情请求独立版本号 — 同上语义，挡 getShareDetail 旧回包
let shareDetailRequestVersion = 0
const currentPage = ref(1)
const pageSize = 20
const selectedIds = ref<number[]>([])
// 正在修改有效期的分享（按钮 loading / 防重复提交）
const expireUpdatingIds = ref<number[]>([])
const batchExpireUpdating = ref(false)
// 账号代次：切账号后丢弃旧账号的有效期修改回包，避免写到新账号列表上
let accountEpoch = 0

// 详情弹窗状态
const detailVisible = ref(false)
const currentDetail = ref<ShareDetailData | null>(null)
const currentShareRecord = ref<ShareRecord | null>(null)

// 计算属性
const activeCountType = computed(() => {
  if (total.value === 0 && shareList.value.length === 0) return 'info'
  return 'success'
})

// 顶部计数：有 total 用 total；否则用本页条数（count / 列表长度），不再显示「0 条」。
// 有多页时注明「本页」，避免把本页条数误读成总数
const headerCountText = computed(() => {
  if (total.value > 0) return `${total.value} 条分享记录`
  const n = pageCount.value || shareList.value.length
  if (currentPage.value > 1 || hasMore.value) return `第 ${currentPage.value} 页 · 本页 ${n} 条`
  return `${n} 条分享记录`
})

// ---------- 失效分享（文件已删除 / 已过期） ----------

// 表格行 / 卡片的高亮 class（界面偏好可关闭）
function deadShareClass(record: ShareRecord): string {
  if (!uiPrefs.highlightDeadShares) return ''
  const kind = deadShareKind(record)
  return kind ? `dead-share dead-share--${kind}` : ''
}

function shareRowClass({ row }: { row: ShareRecord }): string {
  return deadShareClass(row)
}

// 界面偏好开启时，失效分享只保留「取消分享」
function isDeadActionLocked(record: ShareRecord): boolean {
  return uiPrefs.disableDeadShareActions && deadShareKind(record) !== null
}

function deadActionTitle(record: ShareRecord): string | undefined {
  if (!isDeadActionLocked(record)) return undefined
  return deadShareKind(record) === 'deleted' ? '分享的文件已被删除，只能取消分享' : '分享已过期，只能取消分享'
}

function expireButtonTitle(record: ShareRecord): string {
  if (isExpired(record)) return '已过期的分享不能修改有效期'
  if (isDeadActionLocked(record)) return '分享的文件已被删除，只能取消分享'
  return '修改有效期'
}

// 获取文件名
function getFileName(path: string): string {
  if (!path) return '未知文件'
  const parts = path.replace(/\\/g, '/').split('/')
  return parts[parts.length - 1] || path
}

// 获取状态类型（失效分享优先：百度对它们的 status 是 0 / 10 等，不能直接映射）
function getStatusType(record: ShareRecord): 'success' | 'danger' | 'warning' | 'info' {
  const kind = deadShareKind(record)
  if (kind === 'deleted') return 'danger'
  if (kind === 'expired') return 'warning'
  switch (record.status) {
    case 0:
      return 'success'
    case 1:
      return 'danger'
    default:
      return 'warning'
  }
}

// 获取状态文本
function getStatusText(record: ShareRecord): string {
  const kind = deadShareKind(record)
  if (kind === 'deleted') return '已删除'
  if (kind === 'expired') return '已过期'
  switch (record.status) {
    case 0:
      return '正常'
    case 1:
      return '已失效'
    case 3:
      return '审核中'
    default:
      return '未知'
  }
}

// 格式化过期时间
// 后端已把百度的 expiredTime（剩余秒数）换算成过期时间点（优先 lastExpireTime），
// 这里的 expiredTime 是 Unix 秒级时间戳；0 = 永久（或已过期）
function formatExpireTime(record: ShareRecord): string {
  if (deadShareKind(record) === 'expired') {
    return '已过期'
  }

  const timestamp = record.expiredTime
  if (!timestamp || timestamp === 0) {
    return '永久有效'
  }

  // 判断时间戳是秒还是毫秒
  // 如果时间戳大于 10000000000（约 2286 年的秒级时间戳），则认为是毫秒
  const isMilliseconds = timestamp > 10000000000
  const date = new Date(isMilliseconds ? timestamp : timestamp * 1000)
  const now = new Date()

  if (date < now) {
    return '已过期'
  }
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  })
}

// 是否已过期（与 formatExpireTime 判定一致）
function isExpired(record: ShareRecord): boolean {
  if (deadShareKind(record) === 'expired') return true
  const ts = record.expiredTime
  if (!ts) return false
  const ms = ts > 10000000000 ? ts : ts * 1000
  return ms < Date.now()
}

// 网页端同样不允许修改已过期分享的有效期；修改中也锁住
// 界面偏好「失效分享只保留取消」开启时，文件已删除的分享同样锁住
function isExpireLocked(record: ShareRecord): boolean {
  return (
    isExpired(record) ||
    isDeadActionLocked(record) ||
    expireUpdatingIds.value.includes(record.shareId) ||
    batchExpireUpdating.value
  )
}

function expireLabel(days: ShareExpireDays): string {
  return SHARE_EXPIRE_OPTIONS.find((o) => o.days === days)?.label ?? `${days}天有效`
}

// 用接口返回值就地更新行，不整表刷新
function applyExpireResults(results: ShareExpireItemResult[]) {
  for (const r of results) {
    if (!r.success || r.expiredType == null || r.expiredTime == null) continue
    const row = shareList.value.find((item) => item.shareId === r.shareId)
    if (row) {
      row.expiredType = r.expiredType
      row.expiredTime = r.expiredTime
    }
  }
}

async function runModifyExpire(shareIds: number[], days: ShareExpireDays) {
  const epoch = accountEpoch
  const data = await modifyShareExpire(shareIds, days)
  if (epoch !== accountEpoch) return null
  applyExpireResults(data.results)
  return data
}

// 修改单个分享有效期
async function handleModifyExpire(record: ShareRecord, days: ShareExpireDays) {
  if (isExpireLocked(record)) return
  const id = record.shareId
  const epoch = accountEpoch
  expireUpdatingIds.value.push(id)
  try {
    const data = await runModifyExpire([id], days)
    if (!data) return
    const r = data.results.find((x) => x.shareId === id)
    if (r?.success) {
      ElMessage.success(`有效期已改为「${expireLabel(days)}」`)
    } else {
      ElMessage.error(`修改有效期失败：${r?.error || '未知错误'}`)
    }
  } catch (error: any) {
    console.error('修改有效期失败:', error)
    ElMessage.error(error.message || '修改有效期失败')
    // 请求失败（超时 / 网络）时结果未知，重新拉列表以显示真实有效期
    if (epoch === accountEpoch) refreshList()
  } finally {
    if (epoch === accountEpoch) {
      expireUpdatingIds.value = expireUpdatingIds.value.filter((x) => x !== id)
    }
  }
}

// 批量修改有效期（跳过已过期的分享，与网页端一致）
async function handleBatchExpire(days: ShareExpireDays) {
  if (selectedIds.value.length === 0 || batchExpireUpdating.value) return
  // 只跳过当前列表里能确认已过期（或按界面偏好锁定的失效分享）的；
  // 不在当前页的选中项交给后端（失败会逐条返回原因）
  let skippedLocked = 0
  const targets = selectedIds.value.filter((id) => {
    const row = shareList.value.find((item) => item.shareId === id)
    if (!row || isExpired(row)) return !row
    if (isDeadActionLocked(row)) {
      skippedLocked++
      return false
    }
    return true
  })
  const skipped = selectedIds.value.length - targets.length
  const skippedWhat = skippedLocked > 0 ? '已过期或文件已删除' : '已过期'
  if (targets.length === 0) {
    ElMessage.warning(`选中的分享都${skippedWhat}，无法修改有效期`)
    return
  }
  try {
    await ElMessageBox.confirm(
        `确定把选中的 ${targets.length} 个分享的有效期改为「${expireLabel(days)}」吗？` +
        (skipped > 0 ? `（已跳过 ${skipped} 个${skippedWhat}的分享）` : ''),
        '批量修改有效期',
        { confirmButtonText: '确定', cancelButtonText: '取消', type: 'info' },
    )
  } catch {
    return
  }
  const epoch = accountEpoch
  batchExpireUpdating.value = true
  try {
    const data = await runModifyExpire(targets, days)
    if (!data) return
    if (data.failedCount === 0) {
      ElMessage.success(`已将 ${data.successCount} 个分享的有效期改为「${expireLabel(days)}」`)
    } else {
      const firstError = data.results.find((r) => !r.success)?.error || '未知错误'
      const msg = `成功 ${data.successCount} 个，失败 ${data.failedCount} 个：${firstError}`
      if (data.successCount > 0) {
        ElMessage.warning(msg)
      } else {
        ElMessage.error(msg)
      }
    }
  } catch (error: any) {
    console.error('批量修改有效期失败:', error)
    ElMessage.error(error.message || '批量修改有效期失败')
    if (epoch === accountEpoch) refreshList()
  } finally {
    if (epoch === accountEpoch) {
      batchExpireUpdating.value = false
    }
  }
}

// 刷新列表
async function refreshList() {
  // 请求版本号防御
  const version = ++shareListRequestVersion
  loading.value = true
  try {
    const data = await getShareList(currentPage.value)
    if (version !== shareListRequestVersion) {
      // 已被新请求覆盖（切账号 / 切页等）→ 丢弃本次回包
      return
    }
    shareList.value = data.list
    total.value = data.total || 0
    pageCount.value = data.count ?? data.list.length
    hasMore.value = data.has_more ?? false
  } catch (error: any) {
    if (version !== shareListRequestVersion) return
    console.error('获取分享列表失败:', error)
    ElMessage.error(error.message || '获取分享列表失败')
  } finally {
    if (version === shareListRequestVersion) {
      loading.value = false
    }
  }
}

// 页码变化
function handlePageChange(page: number) {
  currentPage.value = page
  refreshList()
}

// 表格选择变化
function handleSelectionChange(selection: ShareRecord[]) {
  selectedIds.value = selection.map(item => item.shareId)
}

// 移动端切换选择
function toggleSelect(shareId: number) {
  const index = selectedIds.value.indexOf(shareId)
  if (index === -1) {
    selectedIds.value.push(shareId)
  } else {
    selectedIds.value.splice(index, 1)
  }
}

// 获取分享详情
async function handleGetDetail(record: ShareRecord) {
  if (isDeadActionLocked(record)) return
  // 详情请求版本号防御
  const version = ++shareDetailRequestVersion
  try {
    currentShareRecord.value = record
    const detail = await getShareDetail(record.shareId)
    if (version !== shareDetailRequestVersion) return
    currentDetail.value = detail
    detailVisible.value = true
  } catch (error: any) {
    if (version !== shareDetailRequestVersion) return
    console.error('获取分享详情失败:', error)
    ElMessage.error(error.message || '获取分享详情失败')
  }
}

// 复制链接
async function handleCopyLink(record: ShareRecord) {
  if (isDeadActionLocked(record)) return
  try {
    await navigator.clipboard.writeText(record.shortlink)
    ElMessage.success('链接已复制到剪贴板')
  } catch {
    ElMessage.error('复制失败，请手动复制')
  }
}

// 复制详情链接
async function copyDetailLink() {
  if (!currentShareRecord.value) return
  try {
    await navigator.clipboard.writeText(currentShareRecord.value.shortlink)
    ElMessage.success('链接已复制到剪贴板')
  } catch {
    ElMessage.error('复制失败，请手动复制')
  }
}

// 复制详情链接和提取码
async function copyDetailAll() {
  if (!currentShareRecord.value || !currentDetail.value) return
  const pwd = currentDetail.value.pwd || ''
  // 使用列表中的完整链接，提取码从详情接口获取
  const link = currentShareRecord.value.shortlink
  const linkWithPwd = pwd ? `${link}?pwd=${pwd}` : link
  const text = pwd
      ? `链接: ${linkWithPwd}\n提取码: ${pwd}`
      : `链接: ${linkWithPwd}`
  try {
    await navigator.clipboard.writeText(text)
    ElMessage.success('链接和提取码已复制到剪贴板')
  } catch {
    ElMessage.error('复制失败，请手动复制')
  }
}

// 取消单个分享
async function handleCancel(record: ShareRecord) {
  try {
    await ElMessageBox.confirm(
        `确定要取消分享 "${getFileName(record.typicalPath)}" 吗？`,
        '取消分享',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )
    await cancelShare([record.shareId])
    ElMessage.success('分享已取消')
    refreshList()
  } catch (error: any) {
    if (error !== 'cancel') {
      console.error('取消分享失败:', error)
      ElMessage.error(error.message || '取消分享失败')
    }
  }
}

// 批量取消分享
async function handleBatchCancel() {
  if (selectedIds.value.length === 0) return

  try {
    await ElMessageBox.confirm(
        `确定要取消选中的 ${selectedIds.value.length} 个分享吗？`,
        '批量取消分享',
        {
          confirmButtonText: '确定',
          cancelButtonText: '取消',
          type: 'warning',
        }
    )
    await cancelShare(selectedIds.value)
    ElMessage.success(`已取消 ${selectedIds.value.length} 个分享`)
    selectedIds.value = []
    refreshList()
  } catch (error: any) {
    if (error !== 'cancel') {
      console.error('批量取消分享失败:', error)
      ElMessage.error(error.message || '批量取消分享失败')
    }
  }
}

// 多账号切换处理器——与 DownloadsView/UploadsView/FilesView/...
// 5 个视图一致：收到 active 变更时重拉当前账号的分享列表。
//
// 同时关闭详情弹窗 + 清空 currentShareRecord/currentDetail，
// 否则切到新账号后弹窗仍可能展示上一账号的链接/提取码（信息混淆 — 本身不会
// 造成跨账号删除，因为 handleGetDetail 已按当前 active 账号路由）。
//
// 递增 shareListRequestVersion 并清空 shareList，
// 让旧账号 in-flight `getShareList` 回包被 refreshList 内部 version check 丢弃，
// 避免覆盖新账号视图。
function handleActiveChanged() {
  accountEpoch++ // 丢弃旧账号有效期修改回包
  expireUpdatingIds.value = []
  batchExpireUpdating.value = false
  shareListRequestVersion++
  shareDetailRequestVersion++ // 丢弃旧账号详情请求回包
  shareList.value = []
  total.value = 0
  pageCount.value = 0
  hasMore.value = false
  currentPage.value = 1
  detailVisible.value = false
  currentShareRecord.value = null
  currentDetail.value = null
  selectedIds.value = []
  refreshList()
}

// 组件挂载时加载数据
onMounted(() => {
  refreshList()
  window.addEventListener('multi-account:active-changed', handleActiveChanged)
})

onBeforeUnmount(() => {
  window.removeEventListener('multi-account:active-changed', handleActiveChanged)
})
</script>


<style scoped lang="scss">
@use '@/styles/mobile-toolbar' as mt;
/* =====================
   容器样式
   ===================== */
.shares-container {
  padding: 20px;
  height: 100%;
  display: flex;
  flex-direction: column;

  &.is-mobile {
    padding: 12px;
  }
}

/* =====================
   页面头部
   ===================== */
.page-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
  flex-wrap: wrap;
  gap: 12px;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 12px;

  h2 {
    margin: 0;
    font-size: 18px;
    font-weight: 600;
  }
}

.header-right {
  display: flex;
  gap: 8px;
}

/* =====================
   PC端表格样式
   ===================== */
/* 表格行内「有效期」下拉：与相邻按钮保持 Element Plus 默认 12px 间距 */
.share-table .row-expire-dropdown {
  margin: 0 12px;
  vertical-align: middle;
}

.share-table {
  flex: 1;

  .file-name {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

/* =====================
   移动端卡片列表
   ===================== */
.share-card-list {
  flex: 1;
  overflow-y: auto;
}

.share-card {
  background: var(--el-bg-color);
  border-radius: 8px;
  padding: 12px;
  margin-bottom: 12px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  transition: all 0.2s;

  &.selected {
    border: 2px solid var(--el-color-primary);
  }
}

.card-header {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 8px;

  .file-name {
    flex: 1;
    font-weight: 500;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
}

.card-body {
  .info-row {
    display: flex;
    font-size: 13px;
    margin-bottom: 4px;
  }

  .label {
    color: var(--el-text-color-secondary);
    width: 50px;
    flex-shrink: 0;
  }

  .value {
    flex: 1;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .link-text {
    color: var(--el-color-primary);
  }

  .stats-value {
    display: flex;
    flex-wrap: wrap;
    column-gap: 12px;
  }
}

/* =====================
   失效分享高亮（界面偏好「高亮失效分享」）
   用 Element Plus 颜色变量：浅色 / 暗色主题下都是低饱和底色
   ===================== */
.share-table :deep(.el-table__row.dead-share--deleted) > td.el-table__cell {
  background-color: var(--el-color-danger-light-9);
}

.share-table :deep(.el-table__row.dead-share--expired) > td.el-table__cell {
  background-color: var(--el-color-warning-light-9);
}

.share-table :deep(.el-table__row.dead-share--deleted .file-name) {
  color: var(--el-color-danger);
}

.share-table :deep(.el-table__row.dead-share--expired .file-name) {
  color: var(--el-color-warning-dark-2);
}

.share-card.dead-share--deleted {
  background: var(--el-color-danger-light-9);
  border-left: 3px solid var(--el-color-danger-light-5);

  .card-header .file-name {
    color: var(--el-color-danger);
  }
}

.share-card.dead-share--expired {
  background: var(--el-color-warning-light-9);
  border-left: 3px solid var(--el-color-warning-light-5);

  .card-header .file-name {
    color: var(--el-color-warning-dark-2);
  }
}

/* 四个操作等宽网格、统一 8px 间距，不再叠加 Element Plus 相邻按钮的 12px 外边距，
   窄屏（< 360px）改两列，按钮始终不超出卡片 */
.card-actions {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 8px;
  margin-top: 12px;
  padding-top: 12px;
  border-top: 1px solid var(--el-border-color-lighter);

  .el-button {
    width: 100%;
    min-width: 0;
    min-height: 44px; /* 触摸友好的最小高度 */
    margin-left: 0;
    padding-left: 6px;
    padding-right: 6px;
  }

  .row-expire-dropdown {
    display: flex;
    min-width: 0;

    .el-button {
      width: 100%;
    }
  }
}

@media (max-width: 359px) {
  .card-actions {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

/* =====================
   分页样式
   ===================== */
.pagination-wrapper {
  padding: 16px 0;
  display: flex;
  justify-content: center;
}

.simple-pager {
  align-items: center;
  gap: 12px;

  .pager-text {
    font-size: 13px;
    color: var(--el-text-color-regular);
  }
}

/* =====================
   移动端分享详情
   ===================== */
.share-detail-mobile {
  padding: 16px;
}

.share-detail-mobile .detail-item {
  margin-bottom: 16px;
}

.share-detail-mobile .label {
  display: block;
  color: var(--el-text-color-secondary);
  font-size: 12px;
  margin-bottom: 4px;
}

.share-detail-mobile .value {
  font-size: 16px;
  word-break: break-all;
}

.share-detail-mobile .pwd-highlight {
  color: var(--el-color-primary);
  font-weight: bold;
  font-size: 20px;
}

.share-detail-mobile .action-buttons {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-top: 24px;

  :deep(.el-button + .el-button) {
    margin-left: 0;
  }
}

.block-btn {
  width: 100%;
  min-height: 44px;
}

/* =====================
   PC端分享详情
   ===================== */
.share-detail-pc {
  .detail-row {
    margin-bottom: 16px;
    display: flex;
    align-items: flex-start;

    &:last-child {
      margin-bottom: 0;
    }
  }

  .label {
    color: var(--el-text-color-secondary);
    width: 70px;
    flex-shrink: 0;
  }

  .value {
    word-break: break-all;
  }

  .pwd-highlight {
    color: var(--el-color-primary);
    font-weight: bold;
    font-size: 18px;
  }
}

/* =====================
   移动端响应式
   ===================== */
// 手机端工具栏：与下载 / 转存等页同一套规则（styles/_mobile-toolbar.scss）；
// 页面容器已有 12px 内边距，工具栏自身不再加内边距
.shares-container.is-mobile {
  @include mt.mobile-toolbar($inset: 0, $padding-y: 0, $margin-bottom: 12px);
}

/* =====================
   抽屉样式覆盖
   ===================== */
:global(.share-detail-drawer) {
  .el-drawer__header {
    margin-bottom: 0;
    padding: 16px;
    border-bottom: 1px solid var(--el-border-color-lighter);
  }

  .el-drawer__body {
    padding: 0;
  }
}
</style>
