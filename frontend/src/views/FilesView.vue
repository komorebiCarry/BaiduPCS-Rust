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
        <el-button type="primary" @click="showCreateFolderDialog">
          <el-icon><FolderAdd /></el-icon>
          新建文件夹
        </el-button>
        <el-dropdown @command="handleUploadCommand" style="margin: 0 12px;">
          <el-button type="success">
            <el-icon><Upload /></el-icon>
            上传
            <el-icon class="el-icon--right"><ArrowDown /></el-icon>
          </el-button>
          <template #dropdown>
            <el-dropdown-menu>
              <el-dropdown-item command="uploadFile">
                <el-icon><Document /></el-icon>
                上传文件
              </el-dropdown-item>
              <el-dropdown-item command="uploadFolder">
                <el-icon><Folder /></el-icon>
                上传文件夹
              </el-dropdown-item>
            </el-dropdown-menu>
          </template>
        </el-dropdown>
        <el-button type="primary" @click="refreshFileList">
          <el-icon><Refresh /></el-icon>
          刷新
        </el-button>
      </div>
    </div>

    <!-- FilePicker 文件选择器弹窗 -->
    <FilePickerModal
        v-model="showFilePicker"
        :select-type="uploadType"
        :title="uploadType === 'file' ? '选择上传文件' : '选择上传文件夹'"
        :confirm-text="uploadType === 'file' ? '上传文件' : '上传文件夹'"
        @select="handleFilePickerSelect"
    />

    <!-- 文件列表 -->
    <div class="file-list">
      <el-table
          v-loading="loading"
          :data="fileList"
          style="width: 100%"
          @row-click="handleRowClick"
          :row-class-name="getRowClassName"
      >
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
  </div>
</template>

<script setup lang="ts">
import {ref, onMounted, computed} from 'vue'
import {ElMessage} from 'element-plus'
import {getFileList, formatFileSize, formatTime, createFolder, type FileItem} from '@/api/file'
import {createDownload, createFolderDownload} from '@/api/download'
import {createUpload, createFolderUpload} from '@/api/upload'
import {FilePickerModal} from '@/components/FilePicker'
import type {FileEntry} from '@/api/filesystem'

// 状态
const loading = ref(false)
const fileList = ref<FileItem[]>([])
const currentDir = ref('/')
const downloadingFolders = ref<Set<string>>(new Set())
const createFolderDialogVisible = ref(false)
const creatingFolder = ref(false)
const createFolderForm = ref({
  folderName: ''
})

// FilePicker 状态
const showFilePicker = ref(false)
const uploadType = ref<'file' | 'directory'>('file')

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
async function loadFiles(dir: string) {
  loading.value = true
  try {
    const data = await getFileList(dir)
    fileList.value = data.list
    currentDir.value = dir
  } catch (error: any) {
    ElMessage.error(error.message || '加载文件列表失败')
    console.error('加载文件列表失败:', error)
  } finally {
    loading.value = false
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

// 下载文件夹
async function handleDownloadFolder(folder: FileItem) {
  // 防止重复点击
  if (downloadingFolders.value.has(folder.path)) {
    return
  }

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

// 上传命令处理 - 打开 FilePicker 弹窗
function handleUploadCommand(command: string) {
  if (command === 'uploadFile') {
    uploadType.value = 'file'
    showFilePicker.value = true
  } else if (command === 'uploadFolder') {
    uploadType.value = 'directory'
    showFilePicker.value = true
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

  } catch (error: any) {
    ElMessage.error(error.message || '创建上传任务失败')
    console.error('创建上传任务失败:', error)
  }
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

// 组件挂载时加载根目录
onMounted(() => {
  loadFiles('/')
})
</script>

<script lang="ts">
// 图标导入
export {Folder, Document, Refresh, HomeFilled, Upload, ArrowDown, FolderAdd} from '@element-plus/icons-vue'
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

