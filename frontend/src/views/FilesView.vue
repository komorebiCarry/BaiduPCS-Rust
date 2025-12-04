<template>
  <div class="files-container">
    <!-- 面包屑导航 -->
    <div class="breadcrumb-bar">
      <el-breadcrumb separator="/">
        <el-breadcrumb-item @click="navigateToDir('/')">
          <el-icon>
            <HomeFilled/>
          </el-icon>
          根目录
        </el-breadcrumb-item>
        <el-breadcrumb-item
            v-for="(part, index) in pathParts"
            :key="index"
            @click="navigateToDir(getPathUpTo(index))"
        >
          {{ part }}
        </el-breadcrumb-item>
      </el-breadcrumb>
      <div class="toolbar-buttons">
        <!-- 批量下载按钮 -->
        <el-button
            v-if="selectedFiles.length > 0"
            type="warning"
            :loading="batchDownloading"
            @click="handleBatchDownload"
        >
          <el-icon><Download /></el-icon>
          批量下载 ({{ selectedFiles.length }})
        </el-button>
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
        <el-button type="primary" @click="refreshFileList">
          <el-icon><Refresh /></el-icon>
          刷新
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
        @select="handleFilePickerSelect"
        @select-multiple="handleFilePickerMultiSelect"
    />

    <!-- 文件列表 -->
    <div class="file-list" ref="fileListRef" @scroll="handleScroll">
      <el-table
          v-loading="loading"
          :data="fileList"
          style="width: 100%"
          @row-click="handleRowClick"
          @selection-change="handleSelectionChange"
          :row-class-name="getRowClassName"
      >
        <el-table-column type="selection" width="55" />
        <el-table-column label="文件名" min-width="400">
          <template #default="{ row }">
            <div class="file-name">
              <el-icon :size="20" class="file-icon">
                <Folder v-if="row.isdir === 1"/>
                <Document v-else/>
              </el-icon>
              <span>{{ row.server_filename }}</span>
            </div>
          </template>
        </el-table-column>

        <el-table-column label="大小" width="120">
          <template #default="{ row }">
            <span v-if="row.isdir === 0">{{ formatFileSize(row.size) }}</span>
            <span v-else>-</span>
          </template>
        </el-table-column>

        <el-table-column label="修改时间" width="180">
          <template #default="{ row }">
            {{ formatTime(row.server_mtime) }}
          </template>
        </el-table-column>

        <el-table-column label="操作" width="150" fixed="right">
          <template #default="{ row }">
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
          </template>
        </el-table-column>
      </el-table>

      <!-- 加载更多提示 -->
      <div v-if="loadingMore" class="loading-more">
        <el-icon class="is-loading"><Loading /></el-icon>
        <span>加载中...</span>
      </div>
      <div v-else-if="!hasMore && fileList.length > 0" class="no-more">
        没有更多了
      </div>

      <!-- 空状态 -->
      <el-empty v-if="!loading && fileList.length === 0" description="当前目录为空"/>
    </div>

    <!-- 创建文件夹对话框 -->
    <el-dialog
        v-model="createFolderDialogVisible"
        title="新建文件夹"
        width="500px"
        @close="handleDialogClose"
    >
      <el-form :model="createFolderForm" label-width="80px">
        <el-form-item label="文件夹名">
          <el-input
              v-model="createFolderForm.folderName"
              placeholder="请输入文件夹名称"
              @keyup.enter="handleCreateFolder"
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
        @confirm-download="handleConfirmDownload"
        @use-default="handleUseDefaultDownload"
    />

    <!-- 转存对话框 -->
    <TransferDialog
        v-model="showTransferDialog"
        :current-path="currentDir"
        @success="handleTransferSuccess"
    />
  </div>
</template>

<script setup lang="ts">
import {ref, onMounted, computed} from 'vue'
import {ElMessage} from 'element-plus'
import {getFileList, formatFileSize, formatTime, createFolder, type FileItem} from '@/api/file'
import {createDownload, createFolderDownload, createBatchDownload, type BatchDownloadItem} from '@/api/download'
import {createUpload, createFolderUpload} from '@/api/upload'
import {getConfig, updateRecentDirDebounced, setDefaultDownloadDir, type DownloadConfig, type UploadConfig} from '@/api/config'
import {FilePickerModal} from '@/components/FilePicker'
import TransferDialog from '@/components/TransferDialog.vue'
import type {FileEntry} from '@/api/filesystem'

// 下载配置状态
const downloadConfig = ref<DownloadConfig | null>(null)

// 上传配置状态
const uploadConfig = ref<UploadConfig | null>(null)

// 状态
const loading = ref(false)
const loadingMore = ref(false)
const fileList = ref<FileItem[]>([])
const currentDir = ref('/')
const currentPage = ref(1)
const hasMore = ref(true)
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

// 单文件下载（支持 ask_each_time）
const pendingDownloadFile = ref<FileItem | null>(null)

// 转存对话框状态
const showTransferDialog = ref(false)

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

// 加载文件列表
async function loadFiles(dir: string, append: boolean = false) {
  if (append) {
    loadingMore.value = true
  } else {
    loading.value = true
    currentPage.value = 1
    hasMore.value = true
  }

  try {
    const page = append ? currentPage.value : 1
    const data = await getFileList(dir, page, 50)

    if (append) {
      fileList.value = [...fileList.value, ...data.list]
    } else {
      fileList.value = data.list
      currentDir.value = dir
    }

    hasMore.value = data.has_more
    currentPage.value = data.page
  } catch (error: any) {
    ElMessage.error(error.message || '加载文件列表失败')
    console.error('加载文件列表失败:', error)
  } finally {
    loading.value = false
    loadingMore.value = false
  }
}

// 加载下一页
async function loadNextPage() {
  if (loadingMore.value || !hasMore.value) return

  currentPage.value++
  await loadFiles(currentDir.value, true)
}

// 滚动事件处理
function handleScroll(event: Event) {
  const target = event.target as HTMLElement
  const { scrollTop, scrollHeight, clientHeight } = target

  // 当滚动到距离底部 100px 时加载更多
  if (scrollHeight - scrollTop - clientHeight < 100) {
    loadNextPage()
  }
}

// 导航到目录
function navigateToDir(dir: string) {
  loadFiles(dir)
}

// 刷新文件列表
function refreshFileList() {
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
  return row.isdir === 1 ? 'directory-row' : ''
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
      ElMessage.info('正在创建文件夹:' + folder.server_filename + ' 下载任务...')

      // 创建文件夹下载任务
      await createFolderDownload(folder.path)

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
async function handleFilePickerSelect(entry: FileEntry) {
  try {
    if (entry.entryType === 'file') {
      // 单文件上传
      const remotePath = currentDir.value === '/'
          ? `/${entry.name}`
          : `${currentDir.value}/${entry.name}`

      await createUpload({
        local_path: entry.path,
        remote_path: remotePath,
      })

      ElMessage.success('已添加上传任务')
    } else {
      // 文件夹上传
      const remoteFolderPath = currentDir.value === '/'
          ? `/${entry.name}`
          : `${currentDir.value}/${entry.name}`

      await createFolderUpload({
        local_folder: entry.path,
        remote_folder: remoteFolderPath,
      })

      ElMessage.success('已添加文件夹上传任务')
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
async function handleFilePickerMultiSelect(entries: FileEntry[]) {
  if (entries.length === 0) return

  let successCount = 0
  let failedCount = 0

  ElMessage.info(`正在添加 ${entries.length} 个上传任务...`)

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
    ElMessage.success(`成功添加 ${successCount} 个上传任务`)
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
  } catch (error: any) {
    console.error('加载配置失败:', error)
  }
}

// 处理表格选择变化
function handleSelectionChange(selection: FileItem[]) {
  selectedFiles.value = selection
}

// 批量下载入口
async function handleBatchDownload() {
  if (selectedFiles.value.length === 0) {
    ElMessage.warning('请先选择要下载的文件或文件夹')
    return
  }

  // 确保配置已加载
  if (!downloadConfig.value) {
    await loadDownloadConfig()
  }

  // 检查是否需要询问下载目录
  if (downloadConfig.value?.ask_each_time) {
    showDownloadPicker.value = true
  } else {
    // 使用默认目录直接下载
    const targetDir = downloadConfig.value?.default_directory || downloadConfig.value?.download_dir || 'downloads'
    await executeBatchDownload(targetDir)
  }
}

// 处理下载目录确认
async function handleConfirmDownload(payload: { path: string; setAsDefault: boolean }) {
  const { path, setAsDefault } = payload
  showDownloadPicker.value = false

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
    await executeBatchDownload(path)
  }
}

// 处理使用默认目录下载
async function handleUseDefaultDownload() {
  showDownloadPicker.value = false

  const targetDir = downloadConfig.value?.default_directory || downloadConfig.value?.download_dir || 'downloads'

  if (pendingDownloadFile.value) {
    // 单文件下载
    await executeSingleDownload(pendingDownloadFile.value, targetDir)
    pendingDownloadFile.value = null
  } else {
    // 批量下载
    await executeBatchDownload(targetDir)
  }
}

// 分批处理常量
const BATCH_SIZE = 10 // 每批处理 10 个下载项

// 执行批量下载（支持分批处理）
async function executeBatchDownload(targetDir: string) {
  if (selectedFiles.value.length === 0) return

  batchDownloading.value = true

  try {
    // 构建批量下载请求项
    const allItems: BatchDownloadItem[] = selectedFiles.value.map(file => ({
      fs_id: file.fs_id,
      path: file.path,
      name: file.server_filename,
      is_dir: file.isdir === 1,
      size: file.isdir === 0 ? file.size : undefined
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
          target_dir: targetDir
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
    ElMessage.info('正在创建:' + file.server_filename + ' 下载任务...')

    // 使用批量下载 API 以支持自定义目录
    const response = await createBatchDownload({
      items: [{
        fs_id: file.fs_id,
        path: file.path,
        name: file.server_filename,
        is_dir: file.isdir === 1,
        size: file.isdir === 0 ? file.size : undefined
      }],
      target_dir: targetDir
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

// 组件挂载时加载根目录和配置
onMounted(() => {
  loadFiles('/')
  loadDownloadConfig()
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
</script>

<script lang="ts">
// 图标导入
export {Folder, Document, Refresh, HomeFilled, Upload, ArrowDown, FolderAdd, Download, Share, Loading} from '@element-plus/icons-vue'
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

  .el-breadcrumb {
    font-size: 14px;

    :deep(.el-breadcrumb__item) {
      cursor: pointer;

      &:hover {
        color: #409eff;
      }
    }
  }

  .toolbar-buttons {
    display: flex;
    gap: 12px;
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
}

:deep(.directory-row) {
  cursor: pointer;

  &:hover {
    background-color: #f5f7fa;
  }
}

:deep(.el-table__row) {
  &:hover .file-name {
    color: #409eff;
  }
}
</style>

