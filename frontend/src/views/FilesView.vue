<template>
  <div class="files-container" :class="{ 'is-mobile': isMobile }">
    <!-- 面包屑导航 -->
    <div class="breadcrumb-bar">
      <el-breadcrumb separator="/">
        <el-breadcrumb-item :class="{ 'is-last': pathParts.length === 0 }">
          <span class="crumb-link" @click="navigateToDir('/')">
            <el-icon>
              <HomeFilled/>
            </el-icon>
            <span v-if="!isMobile">根目录</span>
          </span>
        </el-breadcrumb-item>
        <el-breadcrumb-item
            v-for="(part, index) in pathParts"
            :key="getPathUpTo(index)"
            :class="{ 'is-last': index === pathParts.length - 1 }"
        >
          <span class="crumb-link" @click="navigateToDir(getPathUpTo(index))">{{ part }}</span>
        </el-breadcrumb-item>
      </el-breadcrumb>

      <!-- PC端工具栏 -->
      <div v-if="!isMobile" class="toolbar-buttons">
        <div class="search-wrapper persistent" :class="{ active: isSearchMode }">
          <div class="search-shell persistent">
            <button
                type="button"
                class="search-trigger persistent"
                :class="{ 'is-active': isSearchMode }"
                :disabled="searchLoading"
                @click="handleSearchTrigger"
            >
              <el-icon v-if="!searchLoading"><Search /></el-icon>
              <el-icon v-else class="is-loading"><Loading /></el-icon>
            </button>
            <div class="search-input-area">
              <el-input
                  ref="searchInputRef"
                  v-model="searchKeyword"
                  placeholder="搜索文件..."
                  clearable
                  @keyup.enter="handleSearch"
                  @keyup.esc="handleSearchEscape"
                  @clear="handleSearchClear"
              />
            </div>
          </div>
        </div>
        <!-- 批量操作下拉：明确的"批量操作"文本按钮，所有动作在下拉内一目了然 -->
        <el-dropdown
            v-if="selectedFiles.length > 0"
            trigger="click"
        >
          <el-button
              type="warning"
              :loading="batchDownloading || batchDeleting || batchCopying || batchMoving"
          >
            <el-icon><Operation /></el-icon>
            批量操作 ({{ selectedFiles.length }})
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item @click="handleBatchDownload">
                <el-icon><Download /></el-icon>
                批量下载
              </el-dropdown-item>
              <el-dropdown-item v-if="authStore.hasMultipleAccounts" @click="handleBatchCrossAccount('download')">
                <el-icon><Download /></el-icon>
                用其他账号下载...
              </el-dropdown-item>
              <el-dropdown-item @click="handleBatchCopy">
                <el-icon><CopyDocument /></el-icon>
                复制到...
              </el-dropdown-item>
              <el-dropdown-item @click="handleBatchMove">
                <el-icon><Rank /></el-icon>
                移动到...
              </el-dropdown-item>
              <el-dropdown-item v-if="authStore.hasMultipleAccounts" @click="handleBatchCrossAccount('copy')">
                <el-icon><Switch /></el-icon>
                复制/移动到其他账号...
              </el-dropdown-item>
              <el-dropdown-item @click="handleBatchShare">
                <el-icon><Link /></el-icon>
                分享
              </el-dropdown-item>
              <el-dropdown-item divided @click="handleBatchDelete">
                <el-icon><Delete /></el-icon>
                删除
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-button type="primary" @click="showCreateFolderDialog">
          <el-icon><FolderAdd /></el-icon>
          新建文件夹
        </el-button>
        <el-button type="success" @click="showFilePicker = true" >
          <el-icon><Upload /></el-icon>
          上传
        </el-button>
        <el-button type="warning" @click="showTransferDialog = true">
          <el-icon><Share /></el-icon>
          转存
        </el-button>
        <el-button type="danger" @click="showShareDirectDownloadDialog = true">
          <el-icon><Download /></el-icon>
          分享直下
        </el-button>
        <el-button type="primary" @click="refreshFileList">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </div>

      <!-- 移动端工具栏（图标按钮） -->
      <div
          v-else
          class="toolbar-buttons-mobile"
          :class="{ 'search-open': searchExpanded || isSearchMode }"
      >
        <div
            ref="searchWrapperRef"
            class="search-wrapper"
            :class="{ expanded: searchExpanded, active: isSearchMode }"
        >
          <div class="search-shell" :class="{ 'search-flyout-panel': searchExpanded }">
            <div class="search-input-area">
              <el-input
                  ref="searchInputRef"
                  v-model="searchKeyword"
                  placeholder="搜索文件..."
                  clearable
                  @keyup.enter="handleSearch"
                  @keyup.esc="handleSearchEscape"
                  @clear="handleSearchClear"
              >
                <template #prefix>
                  <el-icon><Search /></el-icon>
                </template>
              </el-input>
            </div>
            <el-button
                circle
                class="search-trigger search-flyout-toggle"
                :class="{ 'is-active': searchExpanded || isSearchMode }"
                :loading="searchLoading"
                @click="handleSearchTrigger"
            >
              <el-icon><Search /></el-icon>
            </el-button>
          </div>
        </div>
        <el-button type="primary" circle @click="showCreateFolderDialog">
          <el-icon><FolderAdd /></el-icon>
        </el-button>
        <el-button type="success" circle @click="showFilePicker = true">
          <el-icon><Upload /></el-icon>
        </el-button>
        <el-button type="warning" circle @click="showTransferDialog = true">
          <el-icon><Share /></el-icon>
        </el-button>
        <el-button type="danger" circle @click="showShareDirectDownloadDialog = true" title="分享直下">
          <el-icon><Download /></el-icon>
        </el-button>
        <el-button type="primary" circle @click="refreshFileList">
          <el-icon><Refresh /></el-icon>
        </el-button>
      </div>
    </div>

    <!-- FilePicker 文件选择器弹窗 -->
    <FilePickerModal
        v-model="showFilePicker"
        :select-type="'both'"
        :title="'选择上传文件'"
        :confirm-text="'上传'"
        :multiple="true"
        :initial-path="uploadConfig?.recent_directory"
        :show-encryption="hasEncryptionKey"
        :show-conflict-strategy="true"
        :default-upload-conflict-strategy="uploadConflictStrategy"
        :show-skip-rapid="true"
        :default-skip-rapid-upload="uploadConfig?.skip_rapid_upload ?? false"
        @select="handleFilePickerSelect"
        @select-multiple="handleFilePickerMultiSelect"
    />

    <!-- 文件列表 -->
    <div
        class="file-list"
        :class="{ 'has-mobile-batch-bar': isMobile && selectedFiles.length > 0 }"
        ref="fileListRef"
        @scroll="handleScroll"
        @contextmenu="handleBlankContextMenu"
    >
      <!-- PC端表格视图 -->
      <el-table
          v-if="!isMobile"
          ref="fileTableRef"
          v-loading="loading"
          :data="fileList"
          style="width: 100%"
          @row-click="handleRowClick"
          @row-contextmenu="handleRowContextMenu"
          @selection-change="handleSelectionChange"
          @sort-change="handleSortChange"
          :default-sort="{ prop: 'server_filename', order: 'ascending' }"
          :row-class-name="getRowClassName"
      >
        <el-table-column type="selection" width="55" />
        <el-table-column label="文件名" min-width="400" prop="server_filename" sortable="custom">
          <template #default="{ row }">
            <div class="file-name" :title="(row.is_encrypted || row.is_encrypted_folder) ? `加密${row.isdir === 1 ? '文件夹' : '文件'}: ${row.server_filename}` : ''">
              <el-icon :size="20" class="file-icon">
                <Folder v-if="row.isdir === 1"/>
                <Document v-else/>
              </el-icon>
              <span>{{ getDisplayName(row) }}</span>
              <el-tag v-if="row.is_encrypted || row.is_encrypted_folder" type="warning" size="small" class="encrypted-tag">
                加密
              </el-tag>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="大小" width="120" prop="size" sortable="custom">
          <template #default="{ row }">
            <span v-if="row.isdir === 0">{{ formatFileSize(row.size) }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>

        <el-table-column label="修改时间" width="180" prop="server_mtime" sortable="custom">
          <template #default="{ row }">
            {{ formatTime(row.server_mtime) }}
          </template>
        </el-table-column>

        <el-table-column label="操作" width="280" fixed="right">
          <template #default="{ row }">
            <!-- 分享按钮 -->
            <el-button
                type="info"
                size="small"
                @click.stop="handleSingleShare(row)"
            >
              分享
            </el-button>
            <!-- 文件下载按钮 -->
            <el-button
                v-if="row.isdir === 0"
                type="primary"
                size="small"
                @click.stop="handleDownload(row)"
            >
              下载
            </el-button>
            <!-- 文件夹下载按钮 -->
            <el-button
                v-if="row.isdir === 1"
                type="success"
                size="small"
                :loading="downloadingFolders.has(row.path)"
                @click.stop="handleDownloadFolder(row)"
            >
              下载
            </el-button>
            <!-- 重命名按钮 -->
            <el-button
                type="warning"
                size="small"
                @click.stop="handleSingleRename(row)"
            >
              重命名
            </el-button>
          </template>
        </el-table-column>
      </el-table>

      <!-- 移动端卡片视图 -->
      <div v-else class="mobile-file-list" v-loading="loading">
        <div
            v-for="item in fileList"
            :key="item.fs_id"
            class="mobile-file-card"
            :class="{
              'is-folder': item.isdir === 1,
              'row-located': item.path === locatedPath,
              'is-selected': isFileSelected(item),
            }"
            @click="handleRowClick(item)"
            @contextmenu.prevent="openContextMenu($event.clientX, $event.clientY, [item])"
        >
          <el-checkbox
              class="file-card-checkbox"
              :model-value="isFileSelected(item)"
              @click.stop
              @change="() => toggleMobileSelect(item)"
          />
          <div class="file-card-main">
            <el-icon :size="36" class="file-card-icon" :color="item.isdir === 1 ? '#e6a23c' : '#409eff'">
              <Folder v-if="item.isdir === 1"/>
              <Document v-else/>
            </el-icon>
            <div class="file-card-info">
              <div class="file-card-name" :title="(item.is_encrypted || item.is_encrypted_folder) ? `加密${item.isdir === 1 ? '文件夹' : '文件'}: ${item.server_filename}` : ''">
                {{ getDisplayName(item) }}
                <el-tag v-if="item.is_encrypted || item.is_encrypted_folder" type="warning" size="small" class="encrypted-tag-mobile">
                  加密
                </el-tag>
              </div>
              <div class="file-card-meta">
                <span v-if="item.isdir === 0">{{ formatFileSize(item.size) }}</span>
                <span v-else>文件夹</span>
                <span class="meta-divider">·</span>
                <span>{{ formatTime(item.server_mtime) }}</span>
              </div>
            </div>
          </div>
          <div class="file-card-action">
            <el-button
                type="info"
                size="small"
                circle
                @click.stop="handleSingleShare(item)"
            >
              <el-icon><Link /></el-icon>
            </el-button>
            <el-button
                type="primary"
                size="small"
                circle
                :loading="item.isdir === 1 && downloadingFolders.has(item.path)"
                @click.stop="item.isdir === 1 ? handleDownloadFolder(item) : handleDownload(item)"
            >
              <el-icon><Download /></el-icon>
            </el-button>
            <el-button
                size="small"
                circle
                title="更多操作"
                @click.stop="handleCardMoreClick($event, item)"
            >
              <el-icon><MoreFilled /></el-icon>
            </el-button>
          </div>
        </div>
      </div>

      <!-- 加载更多提示 -->
      <div v-if="loadingMore" class="loading-more">
        <el-icon class="is-loading"><Loading /></el-icon>
        <span>加载中...</span>
      </div>
      <div v-else-if="!(isSearchMode ? searchHasMore : hasMore) && fileList.length > 0" class="no-more">
        没有更多了
      </div>

      <!-- 空状态 -->
      <el-empty v-if="!loading && !searchLoading && fileList.length === 0" :description="isSearchMode ? '未找到匹配的文件' : '当前目录为空'"/>
    </div>

    <!-- 创建文件夹对话框 -->
    <el-dialog
        v-model="createFolderDialogVisible"
        title="新建文件夹"
        width="500px"
        @close="handleDialogClose"
    >
      <!--
        表单里只有一个输入框时，浏览器会把回车当作"隐式提交"，整页刷新（页面状态丢失 → 回到根目录），
        所以必须 @submit.prevent；回车创建也走表单 submit（而不是 keyup.enter，后者在刷新开始后才触发）
      -->
      <el-form :model="createFolderForm" label-width="80px" @submit.prevent="handleCreateFolder">
        <el-form-item label="文件夹名">
          <el-input
              v-model="createFolderForm.folderName"
              placeholder="请输入文件夹名称"
              autofocus
          />
        </el-form-item>
        <el-form-item label="当前路径">
          <el-text>{{ currentDir }}</el-text>
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="createFolderDialogVisible = false">取消</el-button>
          <el-button
              type="primary"
              :loading="creatingFolder"
              @click="handleCreateFolder"
          >
            创建
          </el-button>
        </span>
      </template>
    </el-dialog>

    <!-- 下载目录选择弹窗 -->
    <FilePickerModal
        v-model="showDownloadPicker"
        mode="download"
        select-type="directory"
        title="选择下载目录"
        :initial-path="downloadConfig?.recent_directory || downloadConfig?.default_directory || downloadConfig?.download_dir"
        :default-download-dir="downloadConfig?.default_directory || downloadConfig?.download_dir"
        :show-conflict-strategy="true"
        :default-conflict-strategy="downloadConflictStrategy"
        @confirm-download="handleConfirmDownload"
        @use-default="handleUseDefaultDownload"
    />

    <!-- 转存对话框 -->
    <TransferDialog
        v-model="showTransferDialog"
        :current-path="currentDir"
        @success="handleTransferSuccess"
    />

    <!-- 分享对话框 -->
    <ShareDialog
        v-model="showShareDialog"
        :files="shareFiles"
        @success="handleShareSuccess"
    />

    <!-- 分享直下对话框 -->
    <ShareDirectDownloadDialog
        v-model="showShareDirectDownloadDialog"
        @success="handleShareDirectDownloadSuccess"
    />

    <!-- 网盘文件夹选择器（用于复制 / 移动） -->
    <NetdiskFolderPickerModal
        v-model="showFolderPicker"
        :title="folderPickerTitle"
        :initial-path="folderPickerInitialPath"
        :blocked-paths="folderPickerBlockedPaths"
        :blocked-exact-paths="folderPickerBlockedExactPaths"
        @confirm="handleFolderPicked"
    />

    <!-- 复制 / 移动同名冲突处理 -->
    <FileConflictDialog
        v-model="conflictDialogVisible"
        :operation="conflictOperation"
        :conflicts="conflictEntries"
        :done-count="conflictDoneCount"
        @resolve="handleConflictResolve"
    />

    <!-- 跨账号复制 / 移动 / 下载 -->
    <CrossAccountTransferDialog
        v-model="crossAccountVisible"
        :files="crossAccountFiles"
        :initial-mode="crossAccountMode"
        @finished="handleCrossAccountFinished"
    />

    <!-- 文件右键菜单 -->
    <FileContextMenu
        v-model="contextMenuVisible"
        :x="contextMenuX"
        :y="contextMenuY"
        :items="contextMenuItems"
        @select="handleContextMenuSelect"
    />

    <!-- 重命名对话框 -->
    <el-dialog
        v-model="renameDialogVisible"
        title="重命名"
        width="500px"
        @close="resetRenameDialog"
    >
      <!-- 同新建文件夹：单输入框表单回车会整页刷新，必须 @submit.prevent，并由 submit 触发重命名 -->
      <el-form label-width="80px" @submit.prevent="handleRenameConfirm">
        <el-form-item label="原名称">
          <el-text>{{ renameTarget?.server_filename }}</el-text>
        </el-form-item>
        <el-form-item label="新名称" :error="renameNameError ?? undefined">
          <el-input
              v-model="renameNewName"
              placeholder="输入新名称"
              autofocus
          />
        </el-form-item>
        <el-form-item label="所在目录">
          <el-text>{{ renameTarget ? dirname(renameTarget.path) : '-' }}</el-text>
        </el-form-item>
      </el-form>
      <template #footer>
        <span class="dialog-footer">
          <el-button @click="renameDialogVisible = false">取消</el-button>
          <el-button
              type="primary"
              :loading="renameSubmitting"
              :disabled="!!renameNameError"
              @click="handleRenameConfirm"
          >确定</el-button>
        </span>
      </template>
    </el-dialog>

    <!-- 移动端批量操作栏：吸底于底部导航上方，不占用本已拥挤的顶部工具栏 -->
    <div
        v-if="isMobile && selectedFiles.length > 0"
        class="mobile-batch-bar"
    >
      <div class="mobile-batch-bar__meta">
        <span class="mobile-batch-bar__count">已选 {{ selectedFiles.length }} 项</span>
        <el-button class="mobile-batch-bar__select-all" text type="primary" size="small" @click="selectAllLoaded">全选</el-button>
        <el-button class="mobile-batch-bar__clear" text size="small" @click="clearMobileSelection">取消</el-button>
      </div>
      <div class="mobile-batch-bar__actions">
        <el-button
            class="mobile-batch-bar__download"
            size="small"
            :loading="batchDownloading"
            @click="handleBatchDownload"
        >下载</el-button>
        <el-button
            class="mobile-batch-bar__copy"
            size="small"
            :loading="batchCopying"
            @click="handleBatchCopy"
        >复制</el-button>
        <el-button
            class="mobile-batch-bar__move"
            size="small"
            :loading="batchMoving"
            @click="handleBatchMove"
        >移动</el-button>
        <el-button
            class="mobile-batch-bar__share"
            size="small"
            @click="handleBatchShare"
        >分享</el-button>
        <el-button
            class="mobile-batch-bar__delete"
            size="small"
            type="danger"
            :loading="batchDeleting"
            @click="handleBatchDelete"
        >删除</el-button>
        <el-dropdown
            v-if="authStore.hasMultipleAccounts"
            class="mobile-batch-bar__more"
            trigger="click"
        >
          <el-button size="small">
            更多
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item @click="handleBatchCrossAccount('download')">
                <el-icon><Download /></el-icon>
                用其他账号下载...
              </el-dropdown-item>
              <el-dropdown-item @click="handleBatchCrossAccount('copy')">
                <el-icon><Switch /></el-icon>
                复制/移动到其他账号...
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import {ref, onMounted, onBeforeUnmount, computed, nextTick, watch} from 'vue'
import {useRoute, useRouter} from 'vue-router'
import {ElMessage, ElMessageBox} from 'element-plus'
import type {InputInstance, TableInstance} from 'element-plus'
import {
  getFileList,
  searchFiles,
  formatFileSize,
  formatTime,
  createFolder,
  deleteFiles,
  copyFiles,
  moveFiles,
  renameFile,
  validateFilename,
  joinPath,
  basename,
  dirname,
  isNotFoundError,
  LOCATE_MAX_PAGES,
  FILE_EXISTS_ERROR_CODE,
  type FileItem,
  type FileOperationItem,
  type FileOperationOutcomeDto,
  type FileSortOrder,
} from '@/api/file'
import NetdiskFolderPickerModal from '@/components/NetdiskFolderPickerModal.vue'
import FileContextMenu, {type ContextMenuItem} from '@/components/FileContextMenu.vue'
import FileConflictDialog, {type ConflictChoice, type ConflictEntry} from '@/components/FileConflictDialog.vue'
import CrossAccountTransferDialog from '@/components/CrossAccountTransferDialog.vue'
import type {CrossAccountJob, CrossAccountMode} from '@/api/crossAccount'
import {useAuthStore} from '@/stores/auth'
import {useUiPrefsStore} from '@/stores/uiPrefs'
import {
  CopyDocument as CopyDocumentIcon,
  Delete as DeleteIcon,
  Download as DownloadIcon,
  Edit as EditIcon,
  Folder as FolderIcon,
  FolderAdd as FolderAddIcon,
  FolderOpened as FolderOpenedIcon,
  Link as LinkIcon,
  Rank as RankIcon,
  Switch as SwitchIcon,
  Refresh as RefreshIcon,
  Upload as UploadIcon,
} from '@element-plus/icons-vue'
import {useIsMobile} from '@/utils/responsive'
import {createDownload, createFolderDownload, createBatchDownload, type BatchDownloadItem, type DownloadConflictStrategy} from '@/api/download'
import {createUpload, createFolderUpload, type UploadConflictStrategy} from '@/api/upload'
import {getConfig, updateRecentDirDebounced, setDefaultDownloadDir, type DownloadConfig, type UploadConfig} from '@/api/config'
import {getEncryptionStatus} from '@/api/autobackup'
import {FilePickerModal} from '@/components/FilePicker'
import TransferDialog from '@/components/TransferDialog.vue'
import ShareDialog from '@/components/ShareDialog.vue'
import ShareDirectDownloadDialog from '@/components/ShareDirectDownloadDialog.vue'
import type {FileEntry} from '@/api/filesystem'

// 响应式检测
const isMobile = useIsMobile()

const route = useRoute()
const router = useRouter()
const uiPrefsStore = useUiPrefsStore()

// 下载配置状态
const downloadConfig = ref<DownloadConfig | null>(null)

// 上传配置状态
const uploadConfig = ref<UploadConfig | null>(null)

// 冲突策略状态
const uploadConflictStrategy = ref<UploadConflictStrategy>('smart_dedup')
const downloadConflictStrategy = ref<DownloadConflictStrategy>('overwrite')

// 加密密钥状态
const hasEncryptionKey = ref(false)

// 状态
const loading = ref(false)
const loadingMore = ref(false)
const fileList = ref<FileItem[]>([])
const currentDir = ref('/')
// 正在加载（非追加）中的目标目录，用于目录点击防连点（同一目标加载中忽略重复点击）
const pendingDir = ref<string | null>(null)
const currentPage = ref(1)
const hasMore = ref(true)
// 排序状态（透传给百度接口：name/time/size + 升降序）
const sortOrder = ref<FileSortOrder>('name')
const sortDesc = ref(false)
const fileListRef = ref<HTMLElement | null>(null)
const downloadingFolders = ref<Set<string>>(new Set())
const createFolderDialogVisible = ref(false)
const creatingFolder = ref(false)
const createFolderForm = ref({
  folderName: ''
})

// FilePicker 状态
const showFilePicker = ref(false)

// 批量选择状态
const selectedFiles = ref<FileItem[]>([])
const showDownloadPicker = ref(false)
const batchDownloading = ref(false)
const batchDeleting = ref(false)

// 批量复制 / 批量移动 / 重命名 状态
const batchCopying = ref(false)
const batchMoving = ref(false)
const showFolderPicker = ref(false)
const folderPickerOperation = ref<'copy' | 'move'>('copy')
const folderPickerTitle = computed(() =>
    folderPickerOperation.value === 'copy' ? '选择复制目标文件夹' : '选择移动目标文件夹'
)
const folderPickerBlockedPaths = ref<string[]>([])      // 子树禁选（含子目录）
const folderPickerBlockedExactPaths = ref<string[]>([]) // 精确禁选（仅自身）
const folderPickerInitialPath = ref<string>('/')
// 本次复制 / 移动的源文件（来自批量选择或右键菜单）
const operationFiles = ref<FileItem[]>([])
// 复制 / 移动同名冲突对话框
const conflictDialogVisible = ref(false)
const conflictOperation = ref<'copy' | 'move'>('copy')
const conflictEntries = ref<ConflictEntry[]>([])
const conflictDoneCount = ref(0)
let conflictResolver: ((choice: ConflictChoice) => void) | null = null

// 跨账号复制 / 移动 / 下载
const authStore = useAuthStore()
const crossAccountVisible = ref(false)
const crossAccountFiles = ref<FileItem[]>([])
const crossAccountMode = ref<CrossAccountMode>('copy')

// 右键菜单
const fileTableRef = ref<TableInstance>()
const contextMenuVisible = ref(false)
const contextMenuX = ref(0)
const contextMenuY = ref(0)
// 右键作用对象；为空表示在空白处右键
const contextMenuTargets = ref<FileItem[]>([])

// 重命名对话框
const renameDialogVisible = ref(false)
const renameTarget = ref<FileItem | null>(null)
const renameNewName = ref('')
const renameSubmitting = ref(false)
const renameNameError = computed(() => validateFilename(renameNewName.value))

// 单文件下载（支持 ask_each_time）
const pendingDownloadFile = ref<FileItem | null>(null)
// 批量下载（支持 ask_each_time）：选择目录期间暂存待下载项
const pendingBatchDownloadFiles = ref<FileItem[]>([])

// 转存对话框状态
const showTransferDialog = ref(false)

// 分享对话框状态
const showShareDialog = ref(false)
const shareFiles = ref<FileItem[]>([])

// 分享直下对话框状态
const showShareDirectDownloadDialog = ref(false)

// 搜索状态
const searchExpanded = ref(false)
const searchKeyword = ref('')
const searchLoading = ref(false)
const isSearchMode = ref(false)
const searchPage = ref(1)
const searchHasMore = ref(false)
const searchInputRef = ref<InputInstance>()
const searchWrapperRef = ref<HTMLElement | null>(null)

// 请求版本号，用于取消过期的请求回调
let fileRequestVersion = 0

// 「定位到网盘」高亮的文件路径（从上传管理跳转过来时设置，切换目录后清除）
const locatedPath = ref<string | null>(null)

// 路径分割
const pathParts = computed(() => {
  if (currentDir.value === '/') return []
  return currentDir.value.split('/').filter(p => p)
})

// 获取指定深度的路径
function getPathUpTo(index: number): string {
  const parts = pathParts.value.slice(0, index + 1)
  return '/' + parts.join('/')
}

// 归一化目录路径用于比较：去掉末尾多余斜杠（根目录 "/" 除外），
// 避免未来后端返回带尾斜杠等格式差异导致的字符串相等判断误差
function normalizeDirPath(p: string): string {
  if (!p) return '/'
  const trimmed = p.replace(/\/+$/, '')
  return trimmed === '' ? '/' : trimmed
}

// 目录列表加载结果：not_found 为百度 errno -9（目录不存在）；被更新请求取代或无需加载均视为 ok
type LoadResult = 'ok' | 'not_found' | 'error'

// 加载文件列表；silent 时失败不弹错误提示，由调用方处理
async function loadFiles(
    dir: string,
    append: boolean = false,
    options: { silent?: boolean } = {},
): Promise<LoadResult> {
  if (append && (loadingMore.value || !hasMore.value)) return 'ok'
  const version = ++fileRequestVersion
  // 请求发起时的账号：记忆目录按账号区分
  const ownerUid = authStore.activeUid

  if (append) {
    loadingMore.value = true
  } else {
    loading.value = true
    pendingDir.value = dir
    locatedPath.value = null
    // 切换目录/刷新时，清掉可能残留的分页 loading，避免被上一目录的在途分页请求
    // 卡住（其回调因版本过期而不再复位 loadingMore）
    loadingMore.value = false
    currentPage.value = 1
    hasMore.value = true
  }

  // 追加时请求下一页；页码只在成功后前进，避免失败时无限累加
  const requestPage = append ? currentPage.value + 1 : 1

  try {
    const data = await getFileList(dir, requestPage, 50, sortOrder.value, sortDesc.value)
    if (version !== fileRequestVersion) return 'ok'

    if (append) {
      // 防御：追加结果必须仍属于当前目录，避免旧目录的分页请求覆盖/污染已跳转的目录
      // 使用归一化比较，兼容潜在的尾斜杠等路径格式差异
      if (normalizeDirPath(dir) !== normalizeDirPath(currentDir.value)) return 'ok'
      fileList.value = [...fileList.value, ...data.list]
    } else {
      fileList.value = data.list
      currentDir.value = dir
      uiPrefsStore.setLastNetdiskDir(ownerUid, dir)
      // 与桌面端 el-table 换数据后清空勾选一致（含切目录 / 刷新 / 排序）
      selectedFiles.value = []
    }

    hasMore.value = data.has_more
    currentPage.value = data.page
    return 'ok'
  } catch (error: any) {
    if (version !== fileRequestVersion) return 'ok'
    // 失败时停止继续加载，避免滚动加载在出错后不断 page+1 重发造成死循环
    hasMore.value = false
    if (!options.silent) {
      ElMessage.error(error.message || '加载文件列表失败')
    }
    console.error('加载文件列表失败:', error)
    return isNotFoundError(error) ? 'not_found' : 'error'
  } finally {
    if (version === fileRequestVersion) {
      loading.value = false
      loadingMore.value = false
      if (!append) pendingDir.value = null
    }
  }
}

// 表头排序变化（服务端排序，重新从第一页加载）
const SORT_PROP_MAP: Record<string, FileSortOrder> = {
  server_filename: 'name',
  size: 'size',
  server_mtime: 'time',
}
function handleSortChange({ prop, order }: { prop: string; order: 'ascending' | 'descending' | null }) {
  // 搜索模式下不改变排序（搜索结果由搜索接口返回）
  if (isSearchMode.value) return
  // order 为 null（取消排序）时回退到默认：文件名升序
  sortOrder.value = order ? (SORT_PROP_MAP[prop] ?? 'name') : 'name'
  sortDesc.value = order === 'descending'
  loadFiles(currentDir.value)
}

// 加载下一页
async function loadNextPage() {
  // loading 为 true 表示正在切换目录/刷新（首屏加载中），此时 currentDir 仍是旧值，
  // 不能触发分页，否则会用旧目录发出 page+1 请求并覆盖掉目录跳转结果
  if (loading.value || loadingMore.value || !hasMore.value) return
  await loadFiles(currentDir.value, true)
}

// 滚动事件处理
function handleScroll(event: Event) {
  const target = event.target as HTMLElement
  const { scrollTop, scrollHeight, clientHeight } = target

  // 当滚动到距离底部 100px 时加载更多
  if (scrollHeight - scrollTop - clientHeight < 100) {
    if (isSearchMode.value) {
      loadMoreSearchResults()
    } else {
      loadNextPage()
    }
  }
}

// 导航到目录
function navigateToDir(dir: string) {
  // 防连点：同一目标目录正在加载中时忽略重复点击；
  // 切换到不同目录仍允许（正确性由请求版本号保证，取最新结果）
  if (
      loading.value &&
      pendingDir.value !== null &&
      normalizeDirPath(dir) === normalizeDirPath(pendingDir.value)
  ) {
    return
  }
  if (isSearchMode.value) {
    resetSearchState()
  }
  loadFiles(dir)
}

// 刷新文件列表
function refreshFileList() {
  if (isSearchMode.value) {
    resetSearchState()
  }
  loadFiles(currentDir.value)
}

// 行点击事件
function handleRowClick(row: FileItem) {
  if (row.isdir === 1) {
    // 进入目录
    navigateToDir(row.path)
  }
}

// 行样式
function getRowClassName({row}: { row: FileItem }) {
  const classes: string[] = []
  if (row.isdir === 1) classes.push('directory-row')
  if (locatedPath.value !== null && row.path === locatedPath.value) classes.push('row-located')
  return classes.join(' ')
}

// 获取文件显示名称（加密文件/文件夹显示原始名称）
function getDisplayName(file: FileItem): string {
  if ((file.is_encrypted || file.is_encrypted_folder) && file.original_name) {
    return file.original_name
  }
  return file.server_filename
}

// 下载文件
async function handleDownload(file: FileItem) {
  // 确保配置已加载
  if (!downloadConfig.value) {
    await loadDownloadConfig()
  }

  // 检查是否需要询问下载目录
  if (downloadConfig.value?.ask_each_time) {
    pendingDownloadFile.value = file
    showDownloadPicker.value = true
  } else {
    // 使用默认目录直接下载
    try {
      ElMessage.info('正在创建:' + file.server_filename + ' 下载任务...')

      // 创建下载任务
      await createDownload({
        fs_id: file.fs_id,
        remote_path: file.path,
        filename: file.server_filename,
        total_size: file.size,
        conflict_strategy: downloadConflictStrategy.value,
      })

      ElMessage.success('下载任务已创建')

    } catch (error: any) {
      ElMessage.error(error.message || '创建下载任务失败')
      console.error('创建下载任务失败:', error)
    }
  }
}

// 下载文件夹
async function handleDownloadFolder(folder: FileItem) {
  // 防止重复点击
  if (downloadingFolders.value.has(folder.path)) {
    return
  }

  // 确保配置已加载
  if (!downloadConfig.value) {
    await loadDownloadConfig()
  }

  // 检查是否需要询问下载目录
  if (downloadConfig.value?.ask_each_time) {
    pendingDownloadFile.value = folder
    showDownloadPicker.value = true
  } else {
    downloadingFolders.value.add(folder.path)

    try {
      // 获取显示名称（如果是加密文件夹，使用原始名称）
      const displayName = getDisplayName(folder)
      ElMessage.info('正在创建文件夹:' + displayName + ' 下载任务...')

      // 创建文件夹下载任务（如果是加密文件夹，传递原始名称）
      const originalName = folder.is_encrypted_folder ? folder.original_name : undefined
      await createFolderDownload(folder.path, originalName, downloadConflictStrategy.value)

      ElMessage.success('文件夹下载任务已创建，正在扫描文件...')

    } catch (error: any) {
      ElMessage.error(error.message || '创建文件夹下载任务失败')
      console.error('创建文件夹下载任务失败:', error)
    } finally {
      downloadingFolders.value.delete(folder.path)
    }
  }
}

// 处理 FilePicker 选择结果
async function handleFilePickerSelect(entry: FileEntry, encrypt: boolean = false, conflictStrategy?: string, skipRapidUpload?: boolean) {
  // 如果用户选择了冲突策略，更新当前策略
  if (conflictStrategy) {
    uploadConflictStrategy.value = conflictStrategy as any
  }

  try {
    if (entry.entryType === 'file') {
      // 单文件上传
      const remotePath = currentDir.value === '/'
          ? `/${entry.name}`
          : `${currentDir.value}/${entry.name}`

      await createUpload({
        local_path: entry.path,
        remote_path: remotePath,
        encrypt,
        conflict_strategy: uploadConflictStrategy.value,
        skip_rapid_upload: skipRapidUpload,
      })

      ElMessage.success(encrypt ? '已添加加密上传任务' : '已添加上传任务')
    } else {
      // 文件夹上传
      const remoteFolderPath = currentDir.value === '/'
          ? `/${entry.name}`
          : `${currentDir.value}/${entry.name}`

      await createFolderUpload({
        local_folder: entry.path,
        remote_folder: remoteFolderPath,
        encrypt,
        conflict_strategy: uploadConflictStrategy.value,
        skip_rapid_upload: skipRapidUpload,
      })

      ElMessage.success(encrypt ? '已添加加密文件夹上传任务' : '已添加文件夹上传任务')
    }

    // 更新上传最近目录（使用文件/文件夹的父目录）
    const parentDir = getParentDirectory(entry.path)
    if (parentDir) {
      updateRecentDirDebounced({ dir_type: 'upload', path: parentDir })
      if (uploadConfig.value) {
        uploadConfig.value.recent_directory = parentDir
      }
    }

  } catch (error: any) {
    ElMessage.error(error.message || '创建上传任务失败')
    console.error('创建上传任务失败:', error)
  }
}

// 处理 FilePicker 多选结果
async function handleFilePickerMultiSelect(entries: FileEntry[], encrypt: boolean = false, conflictStrategy?: string, skipRapidUpload?: boolean) {
  if (entries.length === 0) return

  // 如果用户选择了冲突策略，更新当前策略
  if (conflictStrategy) {
    uploadConflictStrategy.value = conflictStrategy as any
  }

  let successCount = 0
  let failedCount = 0

  ElMessage.info(`正在添加 ${entries.length} 个${encrypt ? '加密' : ''}上传任务...`)

  for (const entry of entries) {
    try {
      if (entry.entryType === 'file') {
        // 单文件上传
        const remotePath = currentDir.value === '/'
            ? `/${entry.name}`
            : `${currentDir.value}/${entry.name}`

        await createUpload({
          local_path: entry.path,
          remote_path: remotePath,
          encrypt,
          conflict_strategy: uploadConflictStrategy.value,
          skip_rapid_upload: skipRapidUpload,
        })
        successCount++
      } else {
        // 文件夹上传
        const remoteFolderPath = currentDir.value === '/'
            ? `/${entry.name}`
            : `${currentDir.value}/${entry.name}`

        await createFolderUpload({
          local_folder: entry.path,
          remote_folder: remoteFolderPath,
          encrypt,
          conflict_strategy: uploadConflictStrategy.value,
          skip_rapid_upload: skipRapidUpload,
        })
        successCount++
      }
    } catch (error: any) {
      failedCount++
      console.error(`上传任务创建失败: ${entry.name}`, error)
    }
  }

  // 显示结果
  if (failedCount === 0) {
    ElMessage.success(`成功添加 ${successCount} 个${encrypt ? '加密' : ''}上传任务`)
  } else if (successCount > 0) {
    ElMessage.warning(`成功 ${successCount} 个，失败 ${failedCount} 个`)
  } else {
    ElMessage.error(`全部 ${failedCount} 个任务创建失败`)
  }

  // 更新上传最近目录（使用第一个文件/文件夹的父目录）
  if (successCount > 0 && entries.length > 0) {
    const parentDir = getParentDirectory(entries[0].path)
    if (parentDir) {
      updateRecentDirDebounced({ dir_type: 'upload', path: parentDir })
      if (uploadConfig.value) {
        uploadConfig.value.recent_directory = parentDir
      }
    }
  }
}

// 获取文件/文件夹的父目录
function getParentDirectory(filePath: string): string | null {
  // 处理 Windows 和 Unix 风格路径
  const normalizedPath = filePath.replace(/\\/g, '/')
  const lastSlashIndex = normalizedPath.lastIndexOf('/')
  if (lastSlashIndex > 0) {
    return normalizedPath.substring(0, lastSlashIndex)
  } else if (lastSlashIndex === 0) {
    return '/'
  }
  return null
}

// 显示创建文件夹对话框
function showCreateFolderDialog() {
  createFolderDialogVisible.value = true
  createFolderForm.value.folderName = ''
}

// 对话框关闭时重置表单
function handleDialogClose() {
  createFolderForm.value.folderName = ''
  creatingFolder.value = false
}

// 创建文件夹
async function handleCreateFolder() {
  // 回车与按钮可能连续触发，创建中不重复提交
  if (creatingFolder.value) return
  const folderName = createFolderForm.value.folderName.trim()

  // 验证文件夹名
  if (!folderName) {
    ElMessage.warning('请输入文件夹名称')
    return
  }

  // 验证文件夹名不能包含特殊字符
  if (/[<>:"/\\|?*]/.test(folderName)) {
    ElMessage.warning('文件夹名称不能包含特殊字符: < > : " / \\ | ? *')
    return
  }

  creatingFolder.value = true

  try {
    // 构建完整路径
    const fullPath = currentDir.value === '/'
        ? `/${folderName}`
        : `${currentDir.value}/${folderName}`

    // 调用创建文件夹 API
    await createFolder(fullPath)

    ElMessage.success('文件夹创建成功')

    // 关闭对话框
    createFolderDialogVisible.value = false

    // 刷新文件列表
    await loadFiles(currentDir.value)

  } catch (error: any) {
    ElMessage.error(error.message || '创建文件夹失败')
    console.error('创建文件夹失败:', error)
  } finally {
    creatingFolder.value = false
  }
}

// ============================================
// 批量选择与下载相关函数
// ============================================

// 加载下载配置
async function loadDownloadConfig() {
  try {
    const config = await getConfig()
    downloadConfig.value = config.download
    uploadConfig.value = config.upload

    // 加载默认冲突策略
    if (config.conflict_strategy) {
      uploadConflictStrategy.value = config.conflict_strategy.default_upload_strategy || 'smart_dedup'
      downloadConflictStrategy.value = config.conflict_strategy.default_download_strategy || 'overwrite'
    }
  } catch (error: any) {
    console.error('加载配置失败:', error)
  }

  // 加载加密状态
  try {
    const encryptionStatus = await getEncryptionStatus()
    hasEncryptionKey.value = encryptionStatus.has_key
  } catch (error: any) {
    console.error('加载加密状态失败:', error)
    hasEncryptionKey.value = false
  }
}

// 处理表格选择变化
function handleSelectionChange(selection: FileItem[]) {
  selectedFiles.value = selection
}

function isFileSelected(item: FileItem): boolean {
  return selectedFiles.value.some((f) => f.fs_id === item.fs_id)
}

function toggleMobileSelect(item: FileItem) {
  const index = selectedFiles.value.findIndex((f) => f.fs_id === item.fs_id)
  if (index === -1) {
    selectedFiles.value = [...selectedFiles.value, item]
  } else {
    selectedFiles.value = selectedFiles.value.filter((_, i) => i !== index)
  }
}

function selectAllLoaded() {
  selectedFiles.value = [...fileList.value]
}

function clearMobileSelection() {
  selectedFiles.value = []
}

// 批量下载入口（工具栏）
async function handleBatchDownload() {
  if (selectedFiles.value.length === 0) {
    ElMessage.warning('请先选择要下载的文件或文件夹')
    return
  }
  await startBatchDownload([...selectedFiles.value])
}

async function startBatchDownload(files: FileItem[]) {
  // 确保配置已加载
  if (!downloadConfig.value) {
    await loadDownloadConfig()
  }

  // 检查是否需要询问下载目录
  if (downloadConfig.value?.ask_each_time) {
    // 清掉之前取消选择目录时残留的单文件下载，避免确认目录后下错对象
    pendingDownloadFile.value = null
    pendingBatchDownloadFiles.value = files
    showDownloadPicker.value = true
  } else {
    // 使用默认目录直接下载
    const targetDir = downloadConfig.value?.default_directory || downloadConfig.value?.download_dir || 'downloads'
    await executeBatchDownload(targetDir, files)
  }
}

// 下载任意文件/文件夹组合：单项走单文件/文件夹下载，多项走批量下载
async function downloadFiles(files: FileItem[]) {
  if (files.length === 0) return
  if (files.length === 1) {
    const [file] = files
    await (file.isdir === 1 ? handleDownloadFolder(file) : handleDownload(file))
    return
  }
  await startBatchDownload(files)
}

// 处理下载目录确认
async function handleConfirmDownload(payload: { path: string; setAsDefault: boolean; conflictStrategy?: string }) {
  const { path, setAsDefault, conflictStrategy } = payload
  showDownloadPicker.value = false

  // 如果用户选择了冲突策略，更新当前策略
  if (conflictStrategy) {
    downloadConflictStrategy.value = conflictStrategy as any
  }

  // 如果设置为默认目录
  if (setAsDefault) {
    try {
      await setDefaultDownloadDir({ path })
      if (downloadConfig.value) {
        downloadConfig.value.default_directory = path
      }
    } catch (error: any) {
      console.error('设置默认下载目录失败:', error)
    }
  }

  // 更新最近目录（使用防抖版本，避免频繁 IO）
  updateRecentDirDebounced({ dir_type: 'download', path })
  if (downloadConfig.value) {
    downloadConfig.value.recent_directory = path
  }

  // 执行下载
  if (pendingDownloadFile.value) {
    // 单文件下载
    await executeSingleDownload(pendingDownloadFile.value, path)
    pendingDownloadFile.value = null
  } else {
    // 批量下载
    const files = pendingBatchDownloadFiles.value
    pendingBatchDownloadFiles.value = []
    await executeBatchDownload(path, files)
  }
}

// 处理使用默认目录下载
async function handleUseDefaultDownload(conflictStrategy?: string) {
  showDownloadPicker.value = false

  // 如果用户选择了冲突策略，更新当前策略
  if (conflictStrategy) {
    downloadConflictStrategy.value = conflictStrategy as any
  }

  const targetDir = downloadConfig.value?.default_directory || downloadConfig.value?.download_dir || 'downloads'

  if (pendingDownloadFile.value) {
    // 单文件下载
    await executeSingleDownload(pendingDownloadFile.value, targetDir)
    pendingDownloadFile.value = null
  } else {
    // 批量下载
    const files = pendingBatchDownloadFiles.value
    pendingBatchDownloadFiles.value = []
    await executeBatchDownload(targetDir, files)
  }
}

// 分批处理常量
const BATCH_SIZE = 10 // 每批处理 10 个下载项

// 执行批量下载（支持分批处理）
async function executeBatchDownload(targetDir: string, files: FileItem[]) {
  if (files.length === 0) return

  batchDownloading.value = true

  try {
    // 构建批量下载请求项
    const allItems: BatchDownloadItem[] = files.map(file => ({
      fs_id: file.fs_id,
      path: file.path,
      name: file.server_filename,
      is_dir: file.isdir === 1,
      size: file.isdir === 0 ? file.size : undefined,
      // 🔥 修复：传递 original_name 以支持加密文件夹名称还原
      original_name: (file.is_encrypted || file.is_encrypted_folder) ? file.original_name : undefined
    }))

    const totalCount = allItems.length
    const batchCount = Math.ceil(totalCount / BATCH_SIZE)

    // 统计结果
    let totalTaskIds: string[] = []
    let totalFolderTaskIds: string[] = []
    let totalFailed: { path: string; reason: string }[] = []

    ElMessage.info(`正在创建 ${totalCount} 个下载任务（共 ${batchCount} 批）...`)

    // 分批处理
    for (let i = 0; i < batchCount; i++) {
      const start = i * BATCH_SIZE
      const end = Math.min(start + BATCH_SIZE, totalCount)
      const batchItems = allItems.slice(start, end)

      try {
        const response = await createBatchDownload({
          items: batchItems,
          target_dir: targetDir,
          conflict_strategy: downloadConflictStrategy.value,
        })

        // 累计结果
        totalTaskIds = totalTaskIds.concat(response.task_ids)
        totalFolderTaskIds = totalFolderTaskIds.concat(response.folder_task_ids)
        totalFailed = totalFailed.concat(response.failed)

        // 显示进度（仅在多批时显示）
        if (batchCount > 1) {
          console.log(`批次 ${i + 1}/${batchCount} 完成: ${response.task_ids.length + response.folder_task_ids.length} 成功, ${response.failed.length} 失败`)
        }

      } catch (batchError: any) {
        console.error(`批次 ${i + 1}/${batchCount} 失败:`, batchError)
        // 将整批标记为失败
        batchItems.forEach(item => {
          totalFailed.push({
            path: item.path,
            reason: batchError.message || '批次请求失败'
          })
        })
      }
    }

    // 显示最终结果统计
    const successCount = totalTaskIds.length + totalFolderTaskIds.length
    const failedCount = totalFailed.length

    if (failedCount === 0) {
      ElMessage.success(`成功创建 ${successCount} 个下载任务`)
    } else if (successCount > 0) {
      ElMessage.warning(`成功 ${successCount} 个，失败 ${failedCount} 个`)
      console.warn('部分下载任务创建失败:', totalFailed)
    } else {
      ElMessage.error(`全部 ${failedCount} 个任务创建失败`)
      console.error('批量下载创建失败:', totalFailed)
    }

    // 清空选择
    selectedFiles.value = []

  } catch (error: any) {
    ElMessage.error(error.message || '批量下载失败')
    console.error('批量下载失败:', error)
  } finally {
    batchDownloading.value = false
  }
}

// 执行单文件下载（带目录选择）
async function executeSingleDownload(file: FileItem, targetDir: string) {
  try {
    const displayName = getDisplayName(file)
    ElMessage.info('正在创建:' + displayName + ' 下载任务...')

    // 获取原始名称（如果是加密文件/文件夹）
    const originalName = (file.is_encrypted || file.is_encrypted_folder) ? file.original_name : undefined

    // 使用批量下载 API 以支持自定义目录
    const response = await createBatchDownload({
      items: [{
        fs_id: file.fs_id,
        path: file.path,
        name: file.server_filename,
        is_dir: file.isdir === 1,
        size: file.isdir === 0 ? file.size : undefined,
        original_name: originalName
      }],
      target_dir: targetDir,
      conflict_strategy: downloadConflictStrategy.value,
    })

    if (response.failed.length === 0) {
      ElMessage.success('下载任务已创建')
    } else {
      ElMessage.error(response.failed[0].reason || '创建下载任务失败')
    }

  } catch (error: any) {
    ElMessage.error(error.message || '创建下载任务失败')
    console.error('创建下载任务失败:', error)
  }
}

// ============================================
// 搜索相关函数
// ============================================

// 执行搜索
async function handleSearch() {
  const keyword = searchKeyword.value.trim()
  if (!keyword) {
    if (isSearchMode.value) {
      await exitSearch({ keepExpanded: !isMobile.value })
    }
    return
  }

  searchLoading.value = true
  isSearchMode.value = true
  searchPage.value = 1
  const version = ++fileRequestVersion

  try {
    const data = await searchFiles(keyword, 1, 100)
    if (version !== fileRequestVersion) return
    fileList.value = data.list as FileItem[]
    searchHasMore.value = data.has_more
    selectedFiles.value = []
  } catch (error: any) {
    ElMessage.error(error.message || '搜索失败')
    console.error('搜索失败:', error)
  } finally {
    searchLoading.value = false
    loading.value = false
  }
}

// 加载更多搜索结果
async function loadMoreSearchResults() {
  if (searchLoading.value || !searchHasMore.value) return

  searchLoading.value = true
  loadingMore.value = true
  const version = ++fileRequestVersion
  // 页码只在成功后前进，避免失败时无限累加
  const requestPage = searchPage.value + 1

  try {
    const data = await searchFiles(searchKeyword.value.trim(), requestPage, 100)
    if (version !== fileRequestVersion) return
    fileList.value = [...fileList.value, ...(data.list as FileItem[])]
    searchHasMore.value = data.has_more
    searchPage.value = requestPage
  } catch (error: any) {
    if (version !== fileRequestVersion) return
    // 失败时停止继续加载，避免滚动加载在出错后不断翻页死循环
    searchHasMore.value = false
    ElMessage.error(error.message || '加载更多搜索结果失败')
  } finally {
    if (version === fileRequestVersion) {
      searchLoading.value = false
      loadingMore.value = false
    }
  }
}

function resetSearchState(options: { keepExpanded?: boolean } = {}) {
  fileRequestVersion++
  isSearchMode.value = false
  searchKeyword.value = ''
  searchExpanded.value = options.keepExpanded ?? false
  searchPage.value = 1
  searchHasMore.value = false
}

async function openSearch() {
  if (!searchExpanded.value) {
    searchExpanded.value = true
    await nextTick()
  }
  searchInputRef.value?.focus()
}

async function handleSearchTrigger() {
  if (!isMobile.value && !searchKeyword.value.trim()) {
    if (isSearchMode.value) {
      await exitSearch({ keepExpanded: false })
      return
    }

    searchInputRef.value?.focus()
    return
  }

  if (!searchExpanded.value) {
    await openSearch()
    return
  }

  if (!searchKeyword.value.trim()) {
    if (isSearchMode.value) {
      await exitSearch()
    } else {
      resetSearchState()
    }
    return
  }

  await handleSearch()
}

async function handleSearchClear() {
  if (isSearchMode.value) {
    await exitSearch({ keepExpanded: isMobile.value })
    return
  }

  await nextTick()
  searchInputRef.value?.focus()
}

async function handleSearchEscape() {
  if (isSearchMode.value) {
    await exitSearch({ keepExpanded: isMobile.value })
    return
  }

  if (isMobile.value) {
    resetSearchState()
  } else {
    searchKeyword.value = ''
    await nextTick()
    searchInputRef.value?.focus()
  }
}

function handleSearchOutsidePointerDown(event: MouseEvent) {
  const target = event.target as Node | null
  if (!searchExpanded.value) return
  if (target && searchWrapperRef.value?.contains(target)) return
  if (searchKeyword.value.trim()) return

  if (isSearchMode.value) {
    void exitSearch()
    return
  }

  resetSearchState()
}

// 退出搜索模式
async function exitSearch(options: { keepExpanded?: boolean } = {}) {
  resetSearchState(options)
  await loadFiles(currentDir.value)

  await nextTick()
  if (!isMobile.value || searchExpanded.value) {
    searchInputRef.value?.focus()
  }
}

// 进入页面 / 切换账号时的起始目录：开启「保留网盘目录」时回到该账号上次浏览的目录，否则根目录
function getInitialDir(): string {
  if (!uiPrefsStore.rememberNetdiskPath) return '/'
  return uiPrefsStore.getLastNetdiskDir(authStore.activeUid) ?? '/'
}

// 目录不存在时逐级向上，打开最近一个仍存在的上级目录；返回打开的目录，全部失败返回 null
async function openNearestExistingDir(dir: string): Promise<string | null> {
  let cur = normalizeDirPath(dir)
  while (cur !== '/') {
    cur = dirname(cur)
    // 根目录失败时按常规弹出错误（此时多半是网络 / 登录问题）
    const result = await loadFiles(cur, false, { silent: cur !== '/' })
    if (result === 'ok') return cur
    if (result === 'error') break
  }
  if (cur !== '/' && (await loadFiles('/')) === 'ok') return '/'
  return null
}

function describeDir(dir: string): string {
  return dir === '/' ? '根目录' : `「${dir}」`
}

// 加载起始目录；记忆的目录已被删除 / 移走时回退到最近的上级目录
async function loadInitialDir() {
  const dir = getInitialDir()
  const result = await loadFiles(dir, false, { silent: dir !== '/' })
  if (result === 'ok' || dir === '/') return
  const opened = await openNearestExistingDir(dir)
  if (opened !== null) {
    ElMessage.info(
        result === 'not_found'
            ? `上次浏览的目录「${dir}」已不存在，已打开${describeDir(opened)}`
            : `无法打开上次浏览的目录「${dir}」，已打开${describeDir(opened)}`,
    )
  }
}

// 定位到网盘中的文件：打开所在目录，翻页找到目标并高亮滚动到可见区域
async function locateInNetdisk(dir: string, targetPath: string | null) {
  if (isSearchMode.value) {
    resetSearchState()
  }
  const result = await loadFiles(dir, false, { silent: dir !== '/' })
  if (result !== 'ok') {
    if (dir === '/') return
    const opened = await openNearestExistingDir(dir)
    if (opened !== null) {
      ElMessage.warning(
          result === 'not_found'
              ? `上传目录「${dir}」在网盘中不存在（可能已被移动、重命名或删除，或任务尚未开始上传），已打开${describeDir(opened)}`
              : `无法打开上传目录「${dir}」，已打开${describeDir(opened)}`,
      )
    }
    return
  }
  // 加载期间用户已跳到别的目录 → 放弃定位
  const stillHere = () => normalizeDirPath(currentDir.value) === normalizeDirPath(dir)
  if (!targetPath || !stillHere()) return

  const findTarget = () => fileList.value.find(f => f.path === targetPath)
  for (let i = 0; i < LOCATE_MAX_PAGES && !findTarget() && hasMore.value; i++) {
    const pageResult = await loadFiles(dir, true)
    if (pageResult !== 'ok' || !stillHere()) return
  }

  if (!findTarget()) {
    ElMessage.info('已打开上传目录，但未找到该文件（可能已被重命名、移动或删除）')
    return
  }
  locatedPath.value = targetPath
  await nextTick()
  const el = fileListRef.value?.querySelector('.row-located') as HTMLElement | null
  el?.scrollIntoView?.({ block: 'center' })
}

// 处理 /files?path=<目录>&highlight=<文件路径> 定位请求（来自上传管理），处理后清掉 query
function consumeLocateQuery(): boolean {
  // 离场过渡期间路由已指向其他页面，不处理
  if (route.path !== '/files') return false
  const dir = typeof route.query.path === 'string' ? route.query.path : ''
  if (!dir) return false
  const target = typeof route.query.highlight === 'string' ? route.query.highlight : null
  void router.replace({ path: route.path })
  void locateInNetdisk(normalizeDirPath(dir), target)
  return true
}

// 多账号切换处理器：切到新账号上次浏览的目录（未开启记忆 / 无记录则根目录）
// （新账号的目录树通常与旧账号不同，沿用旧 currentDir 会 404 或显示空）
function handleActiveChanged() {
  if (isSearchMode.value) {
    resetSearchState()
  }
  selectedFiles.value = []
  void loadInitialDir()
  loadDownloadConfig()
}

// 已在网盘管理页时再次收到定位请求
watch(() => route.query.path, () => {
  consumeLocateQuery()
})

// 组件挂载时加载起始目录和配置
onMounted(() => {
  document.addEventListener('mousedown', handleSearchOutsidePointerDown)
  if (!consumeLocateQuery()) {
    void loadInitialDir()
  }
  loadDownloadConfig()
  // 多账号 active 切换 → 重置到根目录并重拉（与 DownloadsView 等 5 视图一致）
  window.addEventListener('multi-account:active-changed', handleActiveChanged)
})

onBeforeUnmount(() => {
  document.removeEventListener('mousedown', handleSearchOutsidePointerDown)
  window.removeEventListener('multi-account:active-changed', handleActiveChanged)
})

// ============================================
// 转存相关函数
// ============================================

// 转存成功处理
function handleTransferSuccess(taskId: string) {
  console.log('转存任务创建成功:', taskId)
  // 刷新文件列表以显示转存后的文件
  refreshFileList()
}

// ============================================
// 分享相关函数
// ============================================

// 单个文件分享
function handleSingleShare(file: FileItem) {
  openShareDialog([file])
}

function openShareDialog(files: FileItem[]) {
  shareFiles.value = files
  showShareDialog.value = true
}

// 批量分享（工具栏按钮）
function handleBatchShare() {
  if (selectedFiles.value.length === 0) {
    ElMessage.warning('请先选择要分享的文件或文件夹')
    return
  }
  openShareDialog([...selectedFiles.value])
}

async function handleBatchDelete() {
  if (selectedFiles.value.length === 0) return
  await confirmDeleteFiles([...selectedFiles.value])
}

async function confirmDeleteFiles(files: FileItem[]) {
  if (files.length === 0) return
  const count = files.length
  const paths = files.map(f => f.path)
  const message = count === 1
      ? `确定要删除「${getDisplayName(files[0])}」吗？删除后可在回收站找回。`
      : `确定要删除选中的 ${count} 个文件/文件夹吗？删除后可在回收站找回。`
  try {
    await ElMessageBox.confirm(
        message,
        '确认删除',
        {
          confirmButtonText: '删除',
          cancelButtonText: '取消',
          type: 'warning',
          beforeClose: async (action, instance, done) => {
            if (action !== 'confirm') { done(); return }
            instance.confirmButtonLoading = true
            instance.confirmButtonText = '删除中...'
            try {
              const result = await deleteFiles(paths)
              done()
              if (result.failed_paths.length > 0) {
                ElMessage.warning(`成功删除 ${result.deleted_count} 个，失败 ${result.failed_paths.length} 个`)
              } else {
                ElMessage.success(`成功删除 ${result.deleted_count} 个文件/文件夹`)
              }
              selectedFiles.value = []
              await refreshFileList()
            } catch (error: any) {
              done()
              // errno=132：百度二次安全验证（多见于删除文件过多的大目录）
              const isVerify = error?.code === 132 ||
                  (typeof error?.message === 'string' && error.message.includes('安全验证'))
              if (isVerify) {
                ElMessageBox.alert(
                    `百度风控拦截：${error.message}。删除大目录（内含大量文件）会触发百度二次安全验证，请前往 pan.baidu.com 在浏览器中完成验证后再试。`,
                    '删除被风控拦截',
                    { type: 'warning' }
                )
              } else {
                ElMessage.error(error.message || '删除失败')
              }
            } finally {
              instance.confirmButtonLoading = false
            }
          }
        }
    )
  } catch {
    // 用户取消
  }
}

// 分享成功处理
function handleShareSuccess() {
  // 清空选择
  selectedFiles.value = []
}

// ============================================
// 分享直下相关函数
// ============================================

// 分享直下成功处理
function handleShareDirectDownloadSuccess(taskId: string) {
  console.log('分享直下任务创建成功:', taskId)
  ElMessage.success('分享直下任务已创建')
}

// =====================================================
// 文件管理操作（filemanager: copy / move / rename）
// =====================================================

/** 文件中是否含加密文件/文件夹（这种禁止 copy/move/rename） */
function findEncrypted(files: FileItem[]): FileItem | null {
  return files.find((f) => f.is_encrypted || f.is_encrypted_folder) ?? null
}

/** 触发批量复制：弹出文件夹选择器 */
function handleBatchCopy() {
  if (selectedFiles.value.length === 0) {
    ElMessage.warning('请先选择要复制的文件或文件夹')
    return
  }
  openCopyPicker([...selectedFiles.value])
}

function openCopyPicker(files: FileItem[]) {
  const enc = findEncrypted(files)
  if (enc) {
    // 提示使用用户可见的原始名称（加密文件页面显示的是 original_name 而非 UUID.dat）
    ElMessage.error(`加密文件/文件夹禁止复制：${getDisplayName(enc)}`)
    return
  }
  operationFiles.value = files
  folderPickerOperation.value = 'copy'
  folderPickerInitialPath.value = currentDir.value
  // 复制允许进入源文件夹自身子目录（百度支持），但禁选"目标 == 源父目录"：
  // 目录选择器无法改 newname，默认用源 basename，若选回源父目录相当于同名复制，
  // 百度会返回 errno=-8 这类"文件已存在"错误，提前在 UI 禁选避免误点。
  const exactBlocked = new Set<string>()
  for (const f of files) {
    exactBlocked.add(dirname(f.path))
  }
  folderPickerBlockedPaths.value = []
  folderPickerBlockedExactPaths.value = Array.from(exactBlocked)
  showFolderPicker.value = true
}

/** 触发批量移动：根据后端校验规则同步禁选 */
function handleBatchMove() {
  if (selectedFiles.value.length === 0) {
    ElMessage.warning('请先选择要移动的文件或文件夹')
    return
  }
  openMovePicker([...selectedFiles.value])
}

function openMovePicker(files: FileItem[]) {
  const enc = findEncrypted(files)
  if (enc) {
    ElMessage.error(`加密文件/文件夹禁止移动：${getDisplayName(enc)}`)
    return
  }
  operationFiles.value = files
  folderPickerOperation.value = 'move'
  folderPickerInitialPath.value = currentDir.value
  // 与后端 move-only 校验对齐，分两类禁选：
  //   1) 子树禁选（subtree）：选中的文件夹 + 其所有子目录
  //      → 对应 ensure_not_move_into_self
  //   2) 精确禁选（exact）：选中文件 / 文件夹的父目录（仅自身，不含其它兄弟目录）
  //      → 对应 ensure_move_not_same_parent
  const subtreeBlocked = new Set<string>()
  const exactBlocked = new Set<string>()
  for (const f of files) {
    if (f.isdir === 1) {
      subtreeBlocked.add(f.path)
    }
    exactBlocked.add(dirname(f.path))
  }
  folderPickerBlockedPaths.value = Array.from(subtreeBlocked)
  folderPickerBlockedExactPaths.value = Array.from(exactBlocked)
  showFolderPicker.value = true
}

/** 触发批量跨账号复制 / 移动 / 下载 */
function handleBatchCrossAccount(mode: CrossAccountMode = 'copy') {
  if (selectedFiles.value.length === 0) {
    ElMessage.warning('请先选择文件或文件夹')
    return
  }
  openCrossAccountDialog([...selectedFiles.value], mode)
}

function openCrossAccountDialog(files: FileItem[], mode: CrossAccountMode = 'copy') {
  const action = mode === 'download' ? '用其他账号下载' : '跨账号复制/移动'
  // 与跨账号复制/移动一致，加密文件/文件夹不支持
  const enc = findEncrypted(files)
  if (enc) {
    ElMessage.error(`加密文件/文件夹不支持${action}：${getDisplayName(enc)}`)
    return
  }
  // 与后端一致：只支持同一目录下的选中项（临时分享只能分享同一目录下的文件；
  // 也避免父子重叠、跨目录同名）
  if (new Set(files.map((f) => dirname(f.path))).size > 1) {
    ElMessage.warning(`${action}只支持同一目录下的文件，跨目录的搜索结果请分批操作`)
    return
  }
  crossAccountFiles.value = files
  crossAccountMode.value = mode
  crossAccountVisible.value = true
}

/** 跨账号移动删掉了源文件：刷新当前列表 */
function handleCrossAccountFinished(job: CrossAccountJob) {
  if (job.source_deleted) {
    refreshFileList()
  }
}

/** 文件夹选择器确认 */
async function handleFolderPicked(destPath: string) {
  await executeFileOperation(folderPickerOperation.value, destPath)
}

/** 处理 filemanager 操作的统一结果，返回是否成功 */
function reportOutcome(action: string, outcome: FileOperationOutcomeDto): boolean {
  if (outcome.kind === 'success') {
    ElMessage.success(`${action}成功：共 ${outcome.total} 项`)
    return true
  }
  // failed
  if (outcome.still_running) {
    ElMessage.warning(`${action}任务仍在后台处理，请稍后刷新查看`)
    return false
  }
  // 风控判断：errno / task_errno 命中 132，或后端透传了 authwidget（含 saferand/safesign/safetpl）
  // 任一条件命中均视为百度风控介入。
  const hitVerifyCode = outcome.errno === 132 || outcome.task_errno === 132
  const hasAuthWidget = !!outcome.authwidget && Object.keys(outcome.authwidget).length > 0
  if (hitVerifyCode || hasAuthWidget) {
    ElMessageBox.alert(
        `百度风控拦截：${outcome.message}。请前往 pan.baidu.com 在浏览器中完成验证后再试。`,
        `${action}被风控拦截`,
        { type: 'warning' }
    )
    return false
  }
  // 逐项失败：未列出的项已成功，提示失败数量而不是笼统的 task_errno
  const failedCount = outcome.failed_items?.length ?? 0
  if (failedCount > 0) {
    ElMessage.error(`${failedCount} 项${action}失败：${outcome.message}`)
    return false
  }
  ElMessage.error(`${action}失败：${outcome.message}`)
  return false
}

/**
 * 执行复制 / 移动，并处理同名冲突
 *
 * 百度按项独立处理：不冲突的照常完成，同名项保持不动并以 error_code=-8 逐项返回。
 * 因此先按默认提交，再只针对冲突项询问用户：保留两者 / 覆盖 → 仅重提冲突项，
 * 跳过 → 不再处理。不会重复处理已完成的项。
 */
async function executeFileOperation(kind: 'copy' | 'move', dest: string) {
  const action = kind === 'copy' ? '复制' : '移动'
  const submit = kind === 'copy' ? copyFiles : moveFiles
  const busy = kind === 'copy' ? batchCopying : batchMoving
  const files = operationFiles.value
  const items: FileOperationItem[] = files.map((f) => ({
    path: f.path,
    dest,
    newname: f.server_filename,
  }))
  busy.value = true
  try {
    const outcome = await submit(items)
    const failedItems = outcome.kind === 'failed' && !outcome.still_running ? outcome.failed_items ?? [] : []
    const conflictsByPath = new Map(
        failedItems.filter((i) => i.error_code === FILE_EXISTS_ERROR_CODE).map((i) => [i.from, i])
    )
    const conflictItems = items.filter((i) => conflictsByPath.has(i.path))

    if (conflictItems.length === 0) {
      if (reportOutcome(action, outcome)) {
        selectedFiles.value = []
        await refreshFileList()
      } else if (failedItems.length > 0 && failedItems.length < items.length) {
        // 部分项已完成，刷新以反映真实状态
        await refreshFileList()
      }
      return
    }

    // 不冲突的项已完成，先刷新列表
    const doneCount = items.length - failedItems.length
    const otherFailedCount = failedItems.length - conflictItems.length
    selectedFiles.value = []
    if (doneCount > 0) await refreshFileList()

    const choice = await askConflictChoice(
        kind,
        conflictItems.map((item) => {
          const file = files.find((f) => f.path === item.path)
          return {
            path: item.path,
            name: file ? getDisplayName(file) : item.newname,
            // 是否文件夹决定能否覆盖，以百度返回为准
            isdir: !!conflictsByPath.get(item.path)?.isdir || file?.isdir === 1,
          }
        }),
        doneCount
    )
    if (choice === 'skip') {
      ElMessage.info(`已跳过 ${conflictItems.length} 个同名项`)
    } else {
      const retry = await submit(conflictItems, choice)
      if (reportOutcome(action, retry)) await refreshFileList()
    }
    if (otherFailedCount > 0) {
      ElMessage.warning(`另有 ${otherFailedCount} 项${action}失败`)
    }
  } catch (e: unknown) {
    ElMessage.error(e instanceof Error ? e.message : `${action}请求失败`)
  } finally {
    busy.value = false
  }
}

/** 弹出同名冲突对话框，等待用户选择 */
function askConflictChoice(
    operation: 'copy' | 'move',
    entries: ConflictEntry[],
    doneCount: number
): Promise<ConflictChoice> {
  conflictOperation.value = operation
  conflictEntries.value = entries
  conflictDoneCount.value = doneCount
  conflictDialogVisible.value = true
  return new Promise((resolve) => {
    conflictResolver = resolve
  })
}

function handleConflictResolve(choice: ConflictChoice) {
  const resolve = conflictResolver
  conflictResolver = null
  resolve?.(choice)
}

/** 单个文件/文件夹重命名入口 */
function handleSingleRename(row: FileItem) {
  if (row.is_encrypted || row.is_encrypted_folder) {
    ElMessage.error(`加密${row.isdir === 1 ? '文件夹' : '文件'}禁止重命名`)
    return
  }
  renameTarget.value = row
  renameNewName.value = row.server_filename
  renameDialogVisible.value = true
}

function resetRenameDialog() {
  renameTarget.value = null
  renameNewName.value = ''
  renameSubmitting.value = false
}

async function handleRenameConfirm() {
  if (!renameTarget.value || renameSubmitting.value) return
  if (renameNameError.value) {
    ElMessage.error(renameNameError.value)
    return
  }
  if (renameNewName.value === renameTarget.value.server_filename) {
    ElMessage.info('名称未变化')
    renameDialogVisible.value = false
    return
  }
  renameSubmitting.value = true
  try {
    const outcome = await renameFile({
      path: renameTarget.value.path,
      newname: renameNewName.value,
      id: renameTarget.value.fs_id,
    })
    if (reportOutcome('重命名', outcome)) {
      renameDialogVisible.value = false
      await refreshFileList()
    }
  } catch (e: unknown) {
    ElMessage.error(e instanceof Error ? e.message : '重命名请求失败')
  } finally {
    renameSubmitting.value = false
  }
}

// =====================================================
// 右键菜单（行 / 移动端卡片 / 空白处）
// =====================================================

const contextMenuItems = computed<ContextMenuItem[]>(() => {
  const targets = contextMenuTargets.value

  // 空白处：当前目录级操作
  if (targets.length === 0) {
    return [
      { key: 'new-folder', label: '新建文件夹', icon: FolderAddIcon },
      { key: 'upload', label: '上传', icon: UploadIcon },
      { key: 'refresh', label: '刷新', icon: RefreshIcon, divided: true },
    ]
  }

  const single = targets.length === 1 ? targets[0] : null
  const encryptedReason = findEncrypted(targets) ? '加密文件/文件夹不支持此操作' : undefined
  const items: ContextMenuItem[] = []
  if (single?.isdir === 1) {
    items.push({ key: 'open', label: '打开', icon: FolderOpenedIcon })
  }
  if (single && isSearchMode.value) {
    items.push({ key: 'open-parent', label: '打开所在目录', icon: FolderIcon })
  }
  items.push({ key: 'download', label: '下载', icon: DownloadIcon, divided: items.length > 0 })
  if (authStore.hasMultipleAccounts) {
    items.push({
      key: 'cross-account-download',
      label: '用其他账号下载...',
      icon: DownloadIcon,
      disabled: !!encryptedReason,
      disabledReason: encryptedReason,
    })
  }
  items.push(
      { key: 'share', label: '分享', icon: LinkIcon },
      {
        key: 'copy',
        label: '复制到...',
        icon: CopyDocumentIcon,
        disabled: !!encryptedReason || batchCopying.value,
        disabledReason: encryptedReason ?? '正在复制，请稍候',
      },
      {
        key: 'move',
        label: '移动到...',
        icon: RankIcon,
        disabled: !!encryptedReason || batchMoving.value,
        disabledReason: encryptedReason ?? '正在移动，请稍候',
      },
  )
  if (authStore.hasMultipleAccounts) {
    items.push({
      key: 'cross-account',
      label: '复制/移动到其他账号...',
      icon: SwitchIcon,
      disabled: !!encryptedReason,
      disabledReason: encryptedReason,
    })
  }
  if (single) {
    items.push({
      key: 'rename',
      label: '重命名',
      icon: EditIcon,
      disabled: !!encryptedReason,
      disabledReason: encryptedReason,
    })
  }
  items.push({ key: 'delete', label: '删除', icon: DeleteIcon, divided: true, danger: true })
  return items
})

function openContextMenu(x: number, y: number, targets: FileItem[]) {
  contextMenuTargets.value = targets
  contextMenuX.value = x
  contextMenuY.value = y
  contextMenuVisible.value = true
}

/** PC 表格行右键：与百度网盘网页版一致，右键已选中的行作用于整个选区，否则改为只选中该行 */
function handleRowContextMenu(row: FileItem, _column: unknown, event: MouseEvent) {
  event.preventDefault()
  const inSelection = selectedFiles.value.some((f) => f.fs_id === row.fs_id)
  if (!inSelection) {
    fileTableRef.value?.clearSelection()
    fileTableRef.value?.toggleRowSelection(row, true)
  }
  openContextMenu(event.clientX, event.clientY, inSelection ? [...selectedFiles.value] : [row])
}

/**
 * 列表空白处右键；行 / 卡片上的右键已被其处理器 preventDefault，这里跳过。
 * 搜索结果不属于某个目录，不提供目录级菜单，保留浏览器原生菜单。
 */
function handleBlankContextMenu(event: MouseEvent) {
  if (event.defaultPrevented || isSearchMode.value) return
  event.preventDefault()
  openContextMenu(event.clientX, event.clientY, [])
}

/** 移动端卡片"更多"按钮：菜单右上角对齐按钮下沿 */
function handleCardMoreClick(event: MouseEvent, item: FileItem) {
  const target = event.target as HTMLElement
  const rect = (target.closest('button') ?? target).getBoundingClientRect()
  openContextMenu(rect.right, rect.bottom + 4, [item])
}

function handleContextMenuSelect(key: string) {
  const targets = contextMenuTargets.value
  const [first] = targets
  switch (key) {
    case 'open':
      navigateToDir(first.path)
      break
    case 'open-parent':
      navigateToDir(dirname(first.path))
      break
    case 'download':
      void downloadFiles(targets)
      break
    case 'share':
      openShareDialog(targets)
      break
    case 'copy':
      openCopyPicker(targets)
      break
    case 'move':
      openMovePicker(targets)
      break
    case 'cross-account':
      openCrossAccountDialog(targets)
      break
    case 'cross-account-download':
      openCrossAccountDialog(targets, 'download')
      break
    case 'rename':
      handleSingleRename(first)
      break
    case 'delete':
      void confirmDeleteFiles(targets)
      break
    case 'new-folder':
      showCreateFolderDialog()
      break
    case 'upload':
      showFilePicker.value = true
      break
    case 'refresh':
      refreshFileList()
      break
  }
}

// 防止 import 被 tree-shake 误判 unused（basename / joinPath 在模板中未直接使用）
void basename
void joinPath
</script>

<script lang="ts">
// 图标导入
export {Folder, Document, Refresh, HomeFilled, Upload, ArrowDown, FolderAdd, Download, Share, Loading, Link, Delete, Search, Close, CopyDocument, Rank, Switch, Edit, MoreFilled, Operation} from '@element-plus/icons-vue'
</script>

<style scoped lang="scss">
.files-container {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  background: white;
}

.breadcrumb-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 16px 20px;
  border-bottom: 1px solid #e0e0e0;
  background: white;
  gap: 12px;
  flex-wrap: wrap; // 路径与工具栏可分两行，路径再长也不会被按钮挤压

  // 让路径区域占据剩余空间并可收缩（含省略号）
  .el-breadcrumb {
    flex: 1 1 auto;
    min-width: 0;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
  }

  // Element Plus 用 :last-child 结构伪类控制分隔符与当前层级样式，而路径栏的层级
  // 是原地增删的；部分浏览器不会重算该伪类，旧的"最后一级"会残留 display:none，
  // 表现为中间的 "/" 连同外边距一起消失（issue #149）。这里改用显式 is-last 类判断。
  :deep(.el-breadcrumb__item:not(.is-last) .el-breadcrumb__separator) {
    display: inline-block;
  }

  :deep(.el-breadcrumb__item.is-last .el-breadcrumb__separator) {
    display: none;
  }

  // 点击热区放在层级文字上，分隔符不参与点击
  .crumb-link {
    display: inline-flex;
    align-items: center;
  }

  // 非当前层级的路径可点击跳转，给出手型指针与悬停高亮
  :deep(.el-breadcrumb__item:not(.is-last)) .crumb-link {
    cursor: pointer;

    &:hover {
      color: var(--el-color-primary);
    }
  }

  .toolbar-buttons {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-shrink: 0;
    flex-wrap: wrap;     // 按钮过多时换行到下一排，而不是横向溢出
    justify-content: flex-end;
  }

  .toolbar-buttons-mobile {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-shrink: 0;
  }
}

.search-wrapper {
  --toolbar-control-size: 32px;
  --search-shell-width: var(--toolbar-control-size);
  --search-expand-width: clamp(180px, 20vw, 232px);
  position: relative;
  width: var(--toolbar-control-size);
  height: var(--toolbar-control-size);
  flex-shrink: 0;
  z-index: 3;

  .search-shell {
    position: absolute;
    top: 50%;
    right: 0;
    transform: translateY(-50%);
    width: var(--search-shell-width);
    height: var(--toolbar-control-size);
    display: flex;
    align-items: center;
    justify-content: flex-end;
    padding: 0;
    border-radius: 999px;
    border: none;
    background: transparent;
    box-shadow: none;
    transition:
        width 0.28s cubic-bezier(0.22, 1, 0.36, 1),
        box-shadow 0.28s ease,
        background 0.28s ease;
    overflow: hidden;
  }

  .search-input-area {
    flex: 1;
    min-width: 0;
    opacity: 0;
    padding-left: 14px;
    transform: translateX(16px);
    pointer-events: none;
    transition:
        opacity 0.2s ease,
        transform 0.28s cubic-bezier(0.22, 1, 0.36, 1);

    :deep(.el-input) {
      width: 100%;
    }

    :deep(.el-input__wrapper) {
      box-shadow: none;
      background: transparent;
      padding-left: 0;
      padding-right: 10px;
    }

    :deep(.el-input__inner) {
      font-size: 13px;
    }
  }

  .search-trigger {
    flex-shrink: 0;
    margin: 0;
    border: none;
    box-shadow: none;
    background: transparent;
    color: #606266;
    width: var(--toolbar-control-size);
    height: var(--toolbar-control-size);
    min-height: var(--toolbar-control-size);
    padding: 0;
    transition:
        transform 0.22s ease,
        color 0.22s ease,
        background-color 0.22s ease,
        opacity 0.22s ease;

    &:hover {
      transform: scale(1.02);
      color: #409eff;
      background: rgba(64, 158, 255, 0.06);
    }

    &.is-active {
      color: #409eff;
      background: transparent;
    }
  }

  &.expanded,
  &.active {
    .search-shell {
      width: var(--search-expand-width);
      background: rgba(255, 255, 255, 0.98);
      box-shadow: 0 12px 28px rgba(15, 23, 42, 0.08);
    }

    .search-input-area {
      opacity: 1;
      transform: translateX(0);
      pointer-events: auto;
    }
  }
}

.search-wrapper.persistent {
  width: clamp(190px, 20vw, 240px);
  height: 36px;

  .search-shell.persistent {
    position: relative;
    top: auto;
    right: auto;
    transform: none;
    width: 100%;
    height: 36px;
    padding: 2px 8px 2px 4px;
    gap: 4px;
    border-radius: 999px;
    background: #f7f8fb;
    box-shadow: inset 0 0 0 1px rgba(148, 163, 184, 0.14);
    overflow: visible;
  }

  .search-input-area {
    opacity: 1;
    transform: none;
    pointer-events: auto;
    padding-left: 0;

    :deep(.el-input__wrapper) {
      padding-left: 0;
      padding-right: 0;
    }

    :deep(.el-input__inner) {
      color: #0f172a;
      font-weight: 500;
    }
  }

  .search-trigger.persistent {
    width: 30px;
    height: 30px;
    min-height: 30px;
    color: #94a3b8;

    &:hover {
      color: #409eff;
      background: rgba(64, 158, 255, 0.08);
    }

    &.is-active {
      color: #409eff;
      background: rgba(64, 158, 255, 0.08);
    }
  }
}

.file-list {
  flex: 1;
  padding: 20px;
  overflow: auto;
}

.loading-more {
  display: flex;
  justify-content: center;
  align-items: center;
  gap: 8px;
  padding: 16px;
  color: #909399;
  font-size: 14px;
}

.no-more {
  text-align: center;
  padding: 16px;
  color: #c0c4cc;
  font-size: 14px;
}

.file-name {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;

  .file-icon {
    flex-shrink: 0;
  }

  &:hover {
    color: #409eff;
  }

  .encrypted-tag {
    margin-left: 4px;
    flex-shrink: 0;
  }
}

:deep(.directory-row) {
  cursor: pointer;

  &:hover {
    background-color: #f5f7fa;
  }
}

// 「定位到网盘」目标行：常亮浅底色 + 进入时闪烁两次
@keyframes row-located-flash {
  0%, 50%, 100% { background-color: #ecf5ff; }
  25%, 75% { background-color: #c6e2ff; }
}

:deep(.el-table__row.row-located) > td.el-table__cell,
.mobile-file-card.row-located {
  background-color: #ecf5ff;
  animation: row-located-flash 1.6s ease-in-out;
}

:deep(.el-table__row) {
  &:hover .file-name {
    color: #409eff;
  }
}

// =====================
// 移动端样式
// =====================
.is-mobile {
  .breadcrumb-bar {
    padding: 12px 16px;
    flex-wrap: wrap;
  }

  // 只有搜索框展开/处于搜索结果时才让工具栏独占一整行（搜索框在下方铺满）；
  // 收起时保持原样，与面包屑同一行。
  .toolbar-buttons-mobile.search-open {
    flex-wrap: wrap;
    width: 100%;
    justify-content: flex-end;
  }

  .search-wrapper {
    --search-expand-width: 100%;

    &.expanded,
    &.active {
      flex: 1 0 100%;
      width: 100%;
      order: 1;
      height: var(--toolbar-control-size);

      .search-shell {
        position: relative;
        top: auto;
        right: auto;
        left: auto;
        transform: none;
        width: 100%;
      }
    }
  }

  .file-list {
    padding: 12px;

    &.has-mobile-batch-bar {
      // 两行操作栏约 108px，另留一点间距，避免最后一张卡片被挡住
      padding-bottom: 128px;
    }
  }
}

.mobile-batch-bar {
  position: fixed;
  left: 0;
  right: 0;
  // 底部导航栏占 56px，再叠加安全区，保证操作栏在 tabbar 之上
  bottom: calc(56px + env(safe-area-inset-bottom, 0px));
  z-index: 1001;
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 10px 12px;
  background: var(--el-bg-color);
  border-top: 1px solid var(--el-border-color-lighter);
  box-shadow: 0 -6px 18px rgba(15, 23, 42, 0.08);
  box-sizing: border-box;

  &__meta {
    display: flex;
    align-items: center;
    min-width: 0;
    gap: 4px;
  }

  &__count {
    font-size: 13px;
    font-weight: 600;
    color: var(--el-text-color-primary);
    margin-right: 4px;
    flex-shrink: 0;
  }

  &__actions {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 6px;

    :deep(.el-button) {
      margin: 0;
    }

    :deep(.el-dropdown) {
      flex: 0 0 auto;
    }
  }
}

// 移动端卡片列表
.mobile-file-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.mobile-file-card {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 12px 12px 10px;
  background: #f9f9f9;
  border-radius: 12px;
  cursor: pointer;
  transition: all 0.2s;
  // 长按唤起右键菜单时不触发文字选择 / iOS 系统长按菜单
  user-select: none;
  -webkit-touch-callout: none;

  // 触摸反馈
  &:active {
    background: #f0f0f0;
    transform: scale(0.98);
  }

  &.is-folder {
    background: #fffbf0;

    &:active {
      background: #fff3d9;
    }
  }

  &.is-selected {
    background: var(--el-color-primary-light-9);

    &:active {
      background: var(--el-color-primary-light-8);
    }

    &.is-folder {
      background: var(--el-color-primary-light-9);

      &:active {
        background: var(--el-color-primary-light-8);
      }
    }
  }

  .file-card-checkbox {
    flex-shrink: 0;
    margin-right: 8px;
    height: 22px;
  }

  .file-card-main {
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
    display: flex;
    align-items: center;
    gap: 4px;

    .encrypted-tag-mobile {
      flex-shrink: 0;
    }
  }

  .file-card-meta {
    font-size: 12px;
    color: #909399;
    display: flex;
    align-items: center;
    gap: 4px;
    min-width: 0;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;

    .meta-divider {
      color: #dcdfe6;
    }
  }

  .file-card-action {
    flex-shrink: 0;
    margin-left: 12px;
    display: flex;
    gap: 8px;
  }
}

// 移动端对话框适配
@media (max-width: 767px) {
  :deep(.el-dialog) {
    width: 92% !important;
    margin: 5vh auto !important;
  }
}
</style>
