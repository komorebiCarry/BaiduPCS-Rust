<template>
  <div class="settings-container" :class="{ 'is-mobile': isMobile }">
    <el-container>
      <!-- 顶部标题 -->
      <el-header height="60px" class="header">
        <h2 v-if="!isMobile">系统设置</h2>
        <div class="header-actions">
          <template v-if="!isMobile">
            <el-button @click="handleReset" :loading="resetting">
              <el-icon><RefreshLeft /></el-icon>
              恢复推荐配置
            </el-button>
            <el-button type="primary" @click="handleSave" :loading="saving">
              <el-icon><Check /></el-icon>
              保存设置
            </el-button>
          </template>
          <template v-else>
            <el-button circle @click="handleReset" :loading="resetting">
              <el-icon><RefreshLeft /></el-icon>
            </el-button>
            <el-button type="primary" circle @click="handleSave" :loading="saving">
              <el-icon><Check /></el-icon>
            </el-button>
          </template>
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
                <div class="input-with-button">
                  <el-input
                      v-model="formData.download.download_dir"
                      placeholder="请输入绝对路径，例如: /app/downloads 或 D:\Downloads"
                      clearable
                  >
                    <template #prepend>
                      <el-icon><Folder /></el-icon>
                    </template>
                  </el-input>
                  <el-button
                      type="primary"
                      @click="handleSelectDownloadDir"
                  >
                    <el-icon><FolderOpened /></el-icon>
                    <span v-if="!isMobile">浏览</span>
                  </el-button>
                </div>
                <div class="form-tip">
                  <div>文件下载的保存目录，必须使用绝对路径</div>
                  <div style="margin-top: 4px;">
                    Windows 示例: <code>D:\Downloads</code> 或 <code>C:\Users\YourName\Downloads</code><br/>
                    Linux/Docker 示例: <code>/app/downloads</code> 或 <code>/home/user/downloads</code>
                  </div>
                </div>
              </el-form-item>

              <el-form-item label="下载时选择目录" prop="download.ask_each_time">
                <el-switch
                    v-model="formData.download.ask_each_time"
                    active-text="每次询问"
                    inactive-text="使用默认"
                />
                <div class="form-tip">
                  开启后，每次下载都会弹出文件资源管理器让您选择保存位置；
                  关闭后将直接使用默认下载目录
                </div>
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

            <!-- 上传配置 -->
            <el-card class="setting-card" shadow="hover">
              <template #header>
                <div class="card-header">
                  <el-icon :size="20" color="#e6a23c">
                    <Upload />
                  </el-icon>
                  <span>上传配置</span>
                </div>
              </template>

              <el-form-item label="全局最大线程数" prop="upload.max_global_threads">
                <el-slider
                    v-model="formData.upload.max_global_threads"
                    :min="1"
                    :max="20"
                    :step="1"
                    :marks="threadMarks"
                    show-stops
                    style="width: calc(100% - 20px); margin-right: 20px"
                />
                <div class="value-display">
                  当前: {{ formData.upload.max_global_threads }} 个
                </div>
                <div class="form-tip">
                  <el-icon><InfoFilled /></el-icon>
                  所有上传任务共享的线程池大小
                </div>
              </el-form-item>

              <el-form-item label="最大同时上传数" prop="upload.max_concurrent_tasks">
                <el-slider
                    v-model="formData.upload.max_concurrent_tasks"
                    :min="1"
                    :max="10"
                    :step="1"
                    :marks="taskMarks"
                    show-stops
                    style="width: calc(100% - 20px); margin-right: 20px"
                />
                <div class="value-display">
                  当前: {{ formData.upload.max_concurrent_tasks }} 个
                </div>
                <div class="form-tip">
                  可以同时进行上传的文件数量上限
                </div>
              </el-form-item>

              <el-form-item label="最大重试次数" prop="upload.max_retries">
                <el-input-number
                    v-model="formData.upload.max_retries"
                    :min="0"
                    :max="10"
                    :step="1"
                    controls-position="right"
                    style="width: 100%"
                />
                <div class="form-tip">上传分片失败后的重试次数，0 表示不重试</div>
              </el-form-item>

              <el-form-item label="跳过隐藏文件" prop="upload.skip_hidden_files">
                <el-switch
                    v-model="formData.upload.skip_hidden_files"
                    active-text="跳过"
                    inactive-text="不跳过"
                />
                <div class="form-tip">
                  上传文件夹时是否跳过以"."开头的隐藏文件/文件夹（如 .git、.DS_Store 等）
                </div>
              </el-form-item>

              <!-- 分片大小说明（自适应，不可配置） -->
              <el-alert
                  title="智能分片大小（自动适配）"
                  type="success"
                  :closable="false"
              >
                <template #default>
                  <div style="line-height: 1.8">
                    系统会根据文件大小和您的VIP等级自动选择最优分片大小：<br/>
                    • 普通用户：固定 4MB 分片<br/>
                    • 普通会员：智能选择 4-16MB 分片<br/>
                    • 超级会员：智能选择 4-32MB 分片<br/>
                    <br/>
                    <strong>⚠️ 重要说明：</strong><br/>
                    • 上传时的实际分片大小（4-32MB）用于提升传输效率<br/>
                  </div>
                </template>
              </el-alert>
            </el-card>

            <!-- 转存配置 -->
            <el-card class="setting-card" shadow="hover">
              <template #header>
                <div class="card-header">
                  <el-icon :size="20" color="#909399">
                    <Share />
                  </el-icon>
                  <span>转存配置</span>
                </div>
              </template>

              <el-form-item label="默认行为">
                <el-radio-group v-model="transferBehavior">
                  <el-radio value="transfer_only">仅转存到网盘</el-radio>
                  <el-radio value="transfer_and_download">转存后自动下载</el-radio>
                </el-radio-group>
                <div class="form-tip">
                  选择"转存后自动下载"时，会根据下载配置决定是否弹出文件选择器
                </div>
              </el-form-item>
            </el-card>

            <!-- 加密设置 -->
            <el-card class="setting-card" shadow="hover">
              <template #header>
                <div class="card-header">
                  <el-icon :size="20" color="#f56c6c">
                    <Key />
                  </el-icon>
                  <span>加密设置</span>
                </div>
              </template>
              <!-- 免责声明 -->
              <el-alert
                  type="error"
                  :closable="false"
                  style="margin-bottom: 20px; border-left: 4px solid #f56c6c;"
              >
                <template #title>
                  <div style="font-weight: 600; margin-bottom: 8px;">⚠️ 重要提示</div>
                  <div style="line-height: 1.8; font-size: 13px;">
                    本加密功能采用客户端侧加密技术，可在一定程度上保护您的文件隐私，但请注意：
                    <ul style="margin: 8px 0 0 0; padding-left: 20px; line-height: 1.8;">
                      <li>加密技术无法提供100%的绝对安全保障，百度官方可能通过其他技术手段检测到加密文件</li>
                      <li>强烈建议对重要文件进行多重备份，并妥善保管加密密钥</li>
                      <li>使用前请充分了解相关风险，并自行评估是否适合您的使用场景</li>
                    </ul>
                  </div>
                </template>
              </el-alert>

              <!-- 加密状态卡片 -->
              <div class="encryption-status-card">
                <div class="status-header">
                  <span class="status-label">加密密钥状态</span>
                  <el-tag :type="encryptionStatus?.has_key ? 'success' : 'info'" size="small">
                    {{ encryptionStatus?.has_key ? '已配置' : '未配置' }}
                  </el-tag>
                </div>
                <div v-if="encryptionStatus?.has_key" class="status-detail">
                  算法: {{ encryptionStatus.algorithm }}<br>
                  创建时间: {{ encryptionStatus.key_created_at ? formatDate(encryptionStatus.key_created_at) : '-' }}
                </div>
              </div>

              <!-- 未配置密钥时显示 -->
              <div v-if="!encryptionStatus?.has_key" class="encryption-form">
                <el-form-item label="加密算法">
                  <el-select v-model="keyAlgorithm" style="width: 100%">
                    <el-option value="AES-256-GCM" label="AES-256-GCM（推荐）" />
                    <el-option value="ChaCha20-Poly1305" label="ChaCha20-Poly1305" />
                  </el-select>
                </el-form-item>
                <el-button type="primary" style="width: 100%" @click="handleGenerateKey">
                  <el-icon><Key /></el-icon>
                  生成新密钥
                </el-button>
                <el-divider>或</el-divider>
                <el-form-item label="导入密钥">
                  <el-input v-model="encryptionKey" placeholder="粘贴Base64编码的密钥" />
                </el-form-item>
                <el-button style="width: 100%" @click="handleImportKey">
                  <el-icon><Upload /></el-icon>
                  导入密钥
                </el-button>
              </div>

              <!-- 已配置密钥时显示 -->
              <div v-else class="encryption-actions">
                <el-button @click="handleExportKey">
                  <el-icon><CopyDocument /></el-icon>
                  导出密钥
                </el-button>
                <el-button type="danger" plain @click="handleDeleteKey">
                  <el-icon><Delete /></el-icon>
                  删除密钥
                </el-button>
              </div>

              <el-alert type="warning" :closable="false" show-icon style="margin-top: 16px">
                <template #title>
                  <strong>重要提示：</strong>请妥善保管加密密钥。如果密钥丢失，将无法解密已加密的文件！
                </template>
              </el-alert>

              <div class="form-tip" style="margin-top: 12px">
                加密密钥用于自动备份和上传时的客户端侧加密。配置密钥后，可在创建备份任务或上传文件时选择是否启用加密。
              </div>
            </el-card>

            <!-- 自动备份设置 -->
            <el-card class="setting-card" shadow="hover">
              <template #header>
                <div class="card-header">
                  <el-icon :size="20" color="#67c23a">
                    <Refresh />
                  </el-icon>
                  <span>自动备份设置</span>
                </div>
              </template>

              <!-- 前置条件提示 -->
              <el-alert
                  v-if="!encryptionStatus?.has_key"
                  title="需要先配置加密密钥才能使用自动备份功能"
                  type="warning"
                  :closable="false"
                  show-icon
                  style="margin-bottom: 16px"
              />

              <!-- 文件监听能力状态 -->
              <div class="watch-capability-card" :class="{ 'is-available': watchCapability?.available }">
                <div class="capability-header">
                  <span class="capability-label">文件监听能力</span>
                  <el-tag :type="watchCapability?.available ? 'success' : 'danger'" size="small">
                    {{ watchCapability?.available ? '可用' : '不可用' }}
                  </el-tag>
                </div>
                <div class="capability-detail">
                  平台: {{ watchCapability?.platform || '-' }} | 后端: {{ watchCapability?.backend || '-' }}
                </div>
                <div v-if="watchCapability?.reason" class="capability-reason">
                  原因: {{ watchCapability.reason }}
                </div>
                <div v-if="watchCapability?.suggestion" class="capability-suggestion">
                  建议: {{ watchCapability.suggestion }}
                </div>
                <div v-if="watchCapability?.warnings?.length" class="capability-warnings">
                  <div v-for="(warning, index) in watchCapability.warnings" :key="index" class="warning-item">
                    {{ warning }}
                  </div>
                </div>
              </div>

              <el-divider content-position="left">上传备份触发方式</el-divider>

              <!-- 文件系统监听 -->
              <el-form-item>
                <el-checkbox
                    v-model="triggerConfig.upload_trigger.watch_enabled"
                    :disabled="!encryptionStatus?.has_key || !watchCapability?.available"
                    @change="handleTriggerConfigChange"
                >
                  启用文件系统监听（实时检测本地文件变化）
                </el-checkbox>
              </el-form-item>

              <div v-if="triggerConfig.upload_trigger.watch_enabled" class="nested-config">
                <el-divider content-position="left" style="margin: 12px 0">
                  <span style="font-size: 12px; color: #909399">监听兜底设置（防止监听遗漏）</span>
                </el-divider>

                <!-- 间隔时间兜底 -->
                <el-form-item>
                  <el-checkbox
                      v-model="triggerConfig.upload_trigger.fallback_interval_enabled"
                      :disabled="!encryptionStatus?.has_key"
                      @change="handleTriggerConfigChange"
                  >
                    启用间隔时间兜底
                  </el-checkbox>
                </el-form-item>
                <el-form-item v-if="triggerConfig.upload_trigger.fallback_interval_enabled" label="间隔时间（分钟）">
                  <el-input-number
                      v-model="triggerConfig.upload_trigger.fallback_interval_minutes"
                      :min="5"
                      :max="1440"
                      :step="5"
                      :disabled="!encryptionStatus?.has_key"
                      @change="handleTriggerConfigChange"
                  />
                  <div class="form-tip">每隔指定时间进行一次增量扫描</div>
                </el-form-item>

                <!-- 指定时间全量扫描 -->
                <el-form-item>
                  <el-checkbox
                      v-model="triggerConfig.upload_trigger.fallback_scheduled_enabled"
                      :disabled="!encryptionStatus?.has_key"
                      @change="handleTriggerConfigChange"
                  >
                    启用指定时间全量扫描
                  </el-checkbox>
                </el-form-item>
                <el-form-item v-if="triggerConfig.upload_trigger.fallback_scheduled_enabled" label="扫描时间">
                  <el-time-picker
                      v-model="uploadScheduledTime"
                      format="HH:mm"
                      :disabled="!encryptionStatus?.has_key"
                      @change="handleUploadScheduledTimeChange"
                  />
                  <div class="form-tip">每天在指定时间进行一次全量扫描</div>
                </el-form-item>
              </div>

              <el-divider content-position="left">下载备份触发方式（仅支持轮询）</el-divider>

              <el-form-item label="轮询模式">
                <el-radio-group
                    v-model="triggerConfig.download_trigger.poll_mode"
                    :disabled="!encryptionStatus?.has_key"
                    @change="handleTriggerConfigChange"
                >
                  <el-radio value="interval">间隔轮询</el-radio>
                  <el-radio value="scheduled">指定时间（推荐）</el-radio>
                </el-radio-group>
              </el-form-item>

              <el-form-item v-if="triggerConfig.download_trigger.poll_mode === 'interval'" label="轮询间隔（分钟）">
                <el-input-number
                    v-model="triggerConfig.download_trigger.poll_interval_minutes"
                    :min="10"
                    :max="1440"
                    :step="10"
                    :disabled="!encryptionStatus?.has_key"
                    @change="handleTriggerConfigChange"
                />
                <div class="form-tip">每隔指定时间检查云端文件变化</div>
              </el-form-item>

              <el-form-item v-if="triggerConfig.download_trigger.poll_mode === 'scheduled'" label="轮询时间">
                <el-time-picker
                    v-model="downloadScheduledTime"
                    format="HH:mm"
                    :disabled="!encryptionStatus?.has_key"
                    @change="handleDownloadScheduledTimeChange"
                />
                <div class="form-tip">每天在指定时间检查云端文件变化（推荐凌晨，避免频繁API调用）</div>
              </el-form-item>

              <el-alert type="info" :closable="false" show-icon style="margin-top: 16px">
                <template #title>
                  下载备份不支持文件系统监听，因为百度网盘API不提供目录变化通知接口。建议使用"指定时间"模式，避免频繁调用API导致限速。
                </template>
              </el-alert>
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
                  <span class="value">v1.4.0</span>
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

    <!-- 目录选择器 -->
    <FilePickerModal
        v-model="showDirPicker"
        mode="select-directory"
        title="选择下载目录"
        confirm-text="确定"
        :initial-path="formData?.download?.download_dir"
        @confirm="handleDirConfirm"
    />

    <!-- 密钥显示对话框 -->
    <el-dialog v-model="showKeyDialog" title="加密密钥" width="450px" :close-on-click-modal="false">
      <el-alert type="warning" :closable="false" show-icon style="margin-bottom: 16px">
        <template #title>请立即备份此密钥到安全的地方！</template>
      </el-alert>
      <el-input
          :model-value="encryptionKey"
          :type="showKey ? 'text' : 'password'"
          readonly
          class="key-input"
      >
        <template #suffix>
          <el-button link @click="showKey = !showKey">
            <el-icon v-if="!showKey"><View /></el-icon>
            <el-icon v-else><Hide /></el-icon>
          </el-button>
          <el-button link @click="copyToClipboard(encryptionKey)">
            <el-icon><CopyDocument /></el-icon>
          </el-button>
        </template>
      </el-input>
      <template #footer>
        <el-button type="primary" style="width: 100%" @click="showKeyDialog = false; encryptionKey = ''; showKey = false">
          我已保存密钥
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { ElMessage, ElMessageBox, type FormInstance, type FormRules } from 'element-plus'
import { useIsMobile } from '@/utils/responsive'
import { useConfigStore } from '@/stores/config'
import type { AppConfig } from '@/api/config'
import { getRecommendedConfig, resetToRecommended } from '@/api/config'
import { FilePickerModal } from '@/components/FilePicker'
import {
  Check,
  RefreshLeft,
  Monitor,
  Connection,
  Download,
  Upload,
  Folder,
  InfoFilled,
  User,
  Files,
  Share,
  FolderOpened,
  Key,
  CopyDocument,
  Delete,
  View,
  Hide,
  Refresh,
} from '@element-plus/icons-vue'
import { getTransferConfig, updateTransferConfig } from '@/api/config'
import {
  getEncryptionStatus,
  generateEncryptionKey,
  importEncryptionKey,
  exportEncryptionKey,
  deleteEncryptionKey,
  getWatchCapability,
  getTriggerConfig,
  updateTriggerConfig,
  type EncryptionStatus,
  type WatchCapability,
  type GlobalTriggerConfig,
} from '@/api/autobackup'

const configStore = useConfigStore()

// 响应式检测
const isMobile = useIsMobile()

// 状态
const loading = ref(false)
const saving = ref(false)
const resetting = ref(false)
const formRef = ref<FormInstance>()
const formData = ref<AppConfig | null>(null)
const recommended = ref<any>(null)
const transferBehavior = ref('transfer_only')
const showDirPicker = ref(false)

// 加密相关状态
const encryptionStatus = ref<EncryptionStatus | null>(null)
const encryptionKey = ref('')
const keyAlgorithm = ref('AES-256-GCM')
const showKeyDialog = ref(false)
const showKey = ref(false)

// 自动备份相关状态
const watchCapability = ref<WatchCapability | null>(null)
const triggerConfig = ref<GlobalTriggerConfig>({
  upload_trigger: {
    watch_enabled: true,
    watch_debounce_ms: 3000,
    watch_recursive: true,
    fallback_interval_enabled: true,
    fallback_interval_minutes: 30,
    fallback_scheduled_enabled: true,
    fallback_scheduled_hour: 2,
    fallback_scheduled_minute: 0,
  },
  download_trigger: {
    poll_mode: 'scheduled',
    poll_interval_minutes: 60,
    poll_scheduled_hour: 2,
    poll_scheduled_minute: 0,
  },
})
const uploadScheduledTime = ref<Date | null>(null)
const downloadScheduledTime = ref<Date | null>(null)

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
    {
      validator: (_rule: any, value: any, callback: any) => {
        if (!value) {
          callback(new Error('请输入下载目录'))
          return
        }
        // 检查是否为绝对路径
        // Windows: 以盘符开头 (如 C:\, D:\) 或 UNC路径 (\\server\share)
        // Linux/Mac: 以 / 开头
        const isWindowsAbsolute = /^[A-Za-z]:\\/.test(value) || /^\\\\/.test(value)
        const isUnixAbsolute = /^\//.test(value)

        if (!isWindowsAbsolute && !isUnixAbsolute) {
          callback(new Error('请输入绝对路径（Windows: D:\\Downloads, Linux: /app/downloads）'))
          return
        }
        callback()
      },
      trigger: 'blur'
    }
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
  'upload.max_global_threads': [
    { required: true, message: '请选择上传全局最大线程数', trigger: 'change' },
    { type: 'number', min: 1, max: 20, message: '线程数范围: 1-20', trigger: 'change' },
  ],
  'upload.max_concurrent_tasks': [
    { required: true, message: '请选择最大同时上传数', trigger: 'change' },
    { type: 'number', min: 1, max: 10, message: '同时上传数范围: 1-10', trigger: 'change' },
  ],
  'upload.max_retries': [
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

    // 加载转存配置
    try {
      const transferConfig = await getTransferConfig()
      transferBehavior.value = transferConfig.default_behavior || 'transfer_only'
    } catch (error) {
      console.warn('获取转存配置失败:', error)
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

    // 同时保存转存配置
    try {
      await updateTransferConfig({ default_behavior: transferBehavior.value })
    } catch (error) {
      console.warn('保存转存配置失败:', error)
    }

    // 同时保存自动备份触发配置
    try {
      await updateTriggerConfig({
        upload_trigger: triggerConfig.value.upload_trigger,
        download_trigger: triggerConfig.value.download_trigger,
      })
    } catch (error) {
      console.warn('保存自动备份触发配置失败:', error)
    }

    ElMessage.success('配置已保存')

    // 重新加载推荐配置以更新警告
    try {
      recommended.value = await getRecommendedConfig()
    } catch (error) {
      console.warn('更新推荐配置失败:', error)
    }
  } catch (error: any) {
    // 提取详细的错误消息
    let errorMessage = '未知错误'

    if (error.response?.data?.details) {
      // 后端返回的详细错误信息在 response.data.details 中
      errorMessage = error.response.data.details
    } else if (error.response?.data?.message) {
      // 后端返回的通用错误消息
      errorMessage = error.response.data.message
    } else if (error.message) {
      // axios 默认的错误消息
      errorMessage = error.message
    }

    ElMessage.error('保存配置失败: ' + errorMessage)
  } finally {
    saving.value = false
  }
}

// 选择下载目录
function handleSelectDownloadDir() {
  showDirPicker.value = true
}

// 目录选择确认
function handleDirConfirm(path: string) {
  if (formData.value && path) {
    formData.value.download.download_dir = path
    ElMessage.success('已选择目录: ' + path)
  }
  showDirPicker.value = false
}

// 加载加密状态
async function loadEncryptionStatus() {
  try {
    encryptionStatus.value = await getEncryptionStatus()
  } catch (error) {
    console.warn('获取加密状态失败:', error)
  }
}

// 加载文件监听能力
async function loadWatchCapability() {
  try {
    watchCapability.value = await getWatchCapability()
  } catch (error) {
    console.warn('获取文件监听能力失败:', error)
  }
}

// 加载触发配置
async function loadTriggerConfig() {
  try {
    const config = await getTriggerConfig()
    triggerConfig.value = config

    // 初始化时间选择器的值
    uploadScheduledTime.value = new Date()
    uploadScheduledTime.value.setHours(config.upload_trigger.fallback_scheduled_hour)
    uploadScheduledTime.value.setMinutes(config.upload_trigger.fallback_scheduled_minute)

    downloadScheduledTime.value = new Date()
    downloadScheduledTime.value.setHours(config.download_trigger.poll_scheduled_hour)
    downloadScheduledTime.value.setMinutes(config.download_trigger.poll_scheduled_minute)
  } catch (error) {
    console.warn('获取触发配置失败:', error)
  }
}

// 处理触发配置变更
async function handleTriggerConfigChange() {
  try {
    await updateTriggerConfig({
      upload_trigger: triggerConfig.value.upload_trigger,
      download_trigger: triggerConfig.value.download_trigger,
    })
    ElMessage.success('自动备份触发配置已更新')
  } catch (error: any) {
    ElMessage.error('更新触发配置失败: ' + (error.message || '未知错误'))
  }
}

// 处理上传定时时间变更
function handleUploadScheduledTimeChange(time: Date | null) {
  if (time) {
    triggerConfig.value.upload_trigger.fallback_scheduled_hour = time.getHours()
    triggerConfig.value.upload_trigger.fallback_scheduled_minute = time.getMinutes()
    handleTriggerConfigChange()
  }
}

// 处理下载定时时间变更
function handleDownloadScheduledTimeChange(time: Date | null) {
  if (time) {
    triggerConfig.value.download_trigger.poll_scheduled_hour = time.getHours()
    triggerConfig.value.download_trigger.poll_scheduled_minute = time.getMinutes()
    handleTriggerConfigChange()
  }
}

// 生成密钥
async function handleGenerateKey() {
  try {
    const key = await generateEncryptionKey(keyAlgorithm.value)
    encryptionKey.value = key
    showKeyDialog.value = true
    await loadEncryptionStatus()
    ElMessage.success('密钥生成成功，请妥善保管')
  } catch (error: any) {
    ElMessage.error('生成密钥失败: ' + (error.message || '未知错误'))
  }
}

// 导入密钥
async function handleImportKey() {
  if (!encryptionKey.value) {
    ElMessage.warning('请输入密钥')
    return
  }
  try {
    await importEncryptionKey(encryptionKey.value, keyAlgorithm.value)
    encryptionKey.value = ''
    await loadEncryptionStatus()
    ElMessage.success('密钥导入成功')
  } catch (error: any) {
    ElMessage.error('导入密钥失败: ' + (error.message || '未知错误'))
  }
}

// 导出密钥
async function handleExportKey() {
  try {
    const key = await exportEncryptionKey()
    encryptionKey.value = key
    showKeyDialog.value = true
  } catch (error: any) {
    ElMessage.error('导出密钥失败: ' + (error.message || '未知错误'))
  }
}

// 删除密钥
async function handleDeleteKey() {
  try {
    await ElMessageBox.confirm(
        '确定要删除加密密钥吗？删除后将无法解密已加密的文件！',
        '危险操作',
        {
          confirmButtonText: '确定删除',
          cancelButtonText: '取消',
          type: 'error',
        }
    )
    await deleteEncryptionKey()
    await loadEncryptionStatus()
    ElMessage.success('密钥已删除')
  } catch (error: any) {
    if (error !== 'cancel') {
      ElMessage.error('删除密钥失败: ' + (error.message || '未知错误'))
    }
  }
}

// 复制到剪贴板
function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text)
  ElMessage.success('已复制到剪贴板')
}

// 格式化日期
function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleString('zh-CN')
}

// 组件挂载
onMounted(() => {
  loadConfig()
  loadEncryptionStatus()
  loadWatchCapability()
  loadTriggerConfig()
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

.input-with-button {
  display: flex;
  gap: 10px;
  width: 100%;

  .el-input {
    flex: 1;
  }
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

// 加密设置样式
.encryption-status-card {
  background: #f5f7fa;
  border-radius: 8px;
  padding: 16px;
  margin-bottom: 16px;

  .status-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;

    .status-label {
      font-size: 14px;
      font-weight: 500;
    }
  }

  .status-detail {
    font-size: 13px;
    color: #909399;
  }
}

.encryption-form {
  margin-top: 16px;
}

.encryption-actions {
  margin-top: 16px;
  display: flex;
  gap: 12px;

  .el-button {
    flex: 1;
  }
}

.key-input {
  font-family: monospace;
}

// 自动备份设置样式
.watch-capability-card {
  background: #fef0f0;
  border-radius: 8px;
  padding: 16px;
  margin-bottom: 16px;
  border-left: 4px solid #f56c6c;

  &.is-available {
    background: #f0f9eb;
    border-left-color: #67c23a;
  }

  .capability-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 8px;

    .capability-label {
      font-size: 14px;
      font-weight: 500;
    }
  }

  .capability-detail {
    font-size: 13px;
    color: #606266;
  }

  .capability-reason {
    font-size: 13px;
    color: #f56c6c;
    margin-top: 8px;
  }

  .capability-suggestion {
    font-size: 13px;
    color: #e6a23c;
    margin-top: 4px;
  }

  .capability-warnings {
    margin-top: 8px;

    .warning-item {
      font-size: 12px;
      color: #e6a23c;
      padding: 4px 0;
    }
  }
}

.nested-config {
  margin-left: 24px;
  padding-left: 16px;
  border-left: 2px solid #e4e7ed;
}

// =====================
// 移动端样式
// =====================
.is-mobile {
  .header {
    padding: 0 16px;

    h2 {
      font-size: 16px;
    }
  }

  .el-main {
    padding: 12px;
  }

  .setting-card {
    margin-bottom: 12px;

    :deep(.el-card__body) {
      padding: 16px;
    }

    .card-header {
      font-size: 14px;
    }
  }

  // 表单标签垂直布局
  :deep(.el-form-item) {
    flex-direction: column;
    align-items: flex-start;

    .el-form-item__label {
      width: 100% !important;
      text-align: left;
      padding-bottom: 8px;
    }

    .el-form-item__content {
      width: 100%;
    }
  }

  .form-tip {
    font-size: 11px;
  }

  .value-display {
    font-size: 12px;
    text-align: left;

    .recommend-hint {
      display: block;
      margin-left: 0;
      margin-top: 4px;
    }
  }

  .vip-info {
    flex-direction: column;
    gap: 8px;
  }

  .about-content .about-item {
    flex-direction: column;
    align-items: flex-start;
    gap: 4px;
  }

  // 29.1 加密操作按钮组优化 - 移动端垂直布局全宽按钮
  .encryption-actions {
    flex-direction: column;
    gap: 8px;

    .el-button {
      width: 100%;
      margin-left: 0 !important;
    }
  }

  // 29.2 嵌套配置减少左边距 - 适配小屏幕
  .nested-config {
    margin-left: 8px !important;
    padding-left: 8px !important;
  }

  // 29.3 时间选择器和数字输入框优化 - 全宽布局和触摸友好
  .el-time-picker,
  :deep(.el-time-picker) {
    width: 100% !important;
  }

  .el-input-number,
  :deep(.el-input-number) {
    width: 100% !important;
  }

  // 增加触摸友好的控件大小
  :deep(.el-input-number__decrease),
  :deep(.el-input-number__increase) {
    width: 40px;
    height: 40px;
  }

  :deep(.el-input-number .el-input__inner) {
    height: 40px;
    line-height: 40px;
  }

  // 时间选择器触摸优化
  :deep(.el-time-picker .el-input__inner) {
    height: 40px;
    line-height: 40px;
  }
}
</style>
