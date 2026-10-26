<template>
  <div class="share-sync-view">
    <!-- 顶部工具栏（与其他页面一致） -->
    <div class="toolbar">
      <div class="header-left">
        <h2 v-if="!isMobile">分享同步</h2>
        <AccountFilter
            v-if="authStore.hasMultipleAccounts"
            v-model="ownerFilter"
            :counts="ownerFilterCounts"
            :total-count="subscriptions.length"
            size="large"
            class="account-filter-slot"
        />
      </div>
      <div class="header-right">
        <el-button type="primary" :icon="Plus" @click="showTransferDialog = true">新增</el-button>
      </div>
    </div>

    <div class="ss-content">
      <!-- 订阅卡片列表（自动备份风格：卡片 + 内联进行中子任务），占满全宽 -->
      <div class="ss-list-title">订阅列表（{{ displayedSubscriptions.length }}）</div>

      <el-empty
          v-if="displayedSubscriptions.length === 0"
          :description="ownerFilter === null ? '还没有订阅' : '当前账号下没有订阅'"
      />

      <div v-else class="config-list">
        <el-card
            v-for="s in displayedSubscriptions"
            :key="s.id"
            class="config-card"
            :class="{ active: selected?.id === s.id, 'is-disabled': !s.enabled }"
            shadow="hover"
            @click="openDetail(s)"
        >
          <!-- 卡片头部 -->
          <div class="config-header">
            <div class="config-info">
              <div class="config-title">
                <el-icon :size="18" class="direction-icon"><Link /></el-icon>
                <span class="config-name">{{ s.name }}</span>
                <AccountBadge :owner-uid="s.owner_uid" size="small" class="task-account-badge" />
                <el-tag :type="s.enabled ? 'success' : 'info'" size="small">{{ s.enabled ? '已启用' : '已停用' }}</el-tag>
                <el-tooltip
                    v-if="s.link_invalid"
                    :content="s.link_invalid_reason || '分享链接已失效（被取消/过期/提取码失效），已暂停轮询；更新链接后点「恢复」'"
                    placement="top"
                >
                  <el-tag type="danger" size="small" effect="dark">链接失效·已暂停</el-tag>
                </el-tooltip>
                <el-tag :type="strategyTagType(s.conflict_strategy)" size="small">{{ describeStrategy(s.conflict_strategy) }}</el-tag>
              </div>
              <div class="config-path">
                <span>{{ describeTargets(s.targets) }}</span>
                <span class="dot">·</span>
                <span>{{ describeInterval(s.poll_config) }}</span>
                <template v-if="s.include_paths.length">
                  <span class="dot">·</span><span>范围 {{ s.include_paths.length }} 条</span>
                </template>
                <template v-if="s.exclude_patterns.length">
                  <span class="dot">·</span><span>排除通配符 {{ s.exclude_patterns.length }} 条</span>
                </template>
                <template v-if="(s.exclude_regex_patterns?.length ?? 0) > 0">
                  <span class="dot">·</span><span>排除正则 {{ s.exclude_regex_patterns!.length }} 条</span>
                </template>
                <template v-if="s.exclude_paths?.length">
                  <span class="dot">·</span><span>排除目录 {{ s.exclude_paths.length }} 个</span>
                </template>
                <template v-if="s.delete_missing">
                  <span class="dot">·</span><span class="danger-text">删除缺失</span>
                </template>
              </div>
            </div>

            <!-- 操作按钮 -->
            <div class="config-actions" @click.stop>
              <el-tooltip
                  :disabled="ownerLoggedIn(s)"
                  content="订阅所属账号未登录，请先登录该账号再触发同步"
                  placement="top"
              >
                  <span>
                    <el-button
                        size="small"
                        type="success"
                        :icon="Refresh"
                        :loading="triggeringId === s.id"
                        :disabled="!ownerLoggedIn(s)"
                        @click="triggerNow(s)"
                    >
                      立即同步
                    </el-button>
                  </span>
              </el-tooltip>
              <el-button
                  v-if="s.link_invalid"
                  size="small"
                  type="primary"
                  :icon="RefreshRight"
                  :loading="resumingId === s.id"
                  @click="resumeNow(s)"
              >
                我已更新链接，恢复
              </el-button>
              <el-button size="small" :icon="Edit" @click="openEdit(s)">编辑</el-button>
              <el-button
                  size="small"
                  :type="s.enabled ? 'warning' : 'success'"
                  :icon="s.enabled ? VideoPause : VideoPlay"
                  @click="toggleEnabled(s)"
              >
                {{ s.enabled ? '停用' : '启用' }}
              </el-button>
              <el-button size="small" type="danger" :icon="Delete" @click="removeSubscription(s)" />
            </div>
          </div>

          <!-- 抓取（递归列目录）阶段进度。
               这一段在 run_started 到 diff_detected 之间，大分享可长达数分钟且
               此前没有任何反馈，用户只能看到「运行中」并以为程序卡死 —— 这里把
               目录扫描的实时计数摊开，重试时还会明确告知是网络在抖。 -->
          <div v-if="scanOf(s.id)" class="active-task-container">
            <div class="active-task-card">
              <div class="task-progress-header">
                <div class="task-status-info">
                  <el-icon :size="16" class="status-icon text-blue-500"><Loading class="is-loading" /></el-icon>
                  <span class="task-status-text">扫描分享目录中…</span>
                </div>
                <span class="scan-stat">{{ scanSummary(scanOf(s.id)!) }}</span>
                <!-- 取消本次扫描：只停扫描，已开始的转存/下载不受影响 -->
                <el-tooltip content="取消本次扫描" placement="top">
                  <el-button
                      class="scan-cancel-btn"
                      size="small"
                      text
                      circle
                      aria-label="取消本次扫描"
                      :loading="cancellingId === s.id"
                      :disabled="cancellingId === s.id"
                      @click.stop="cancelScanNow(s)"
                  >
                    <el-icon><Close /></el-icon>
                  </el-button>
                </el-tooltip>
              </div>
              <div class="scan-body">
                <!-- BFS 边扫边发现新子目录，待扫描数会涨；给百分比会让进度条倒退，
                     比没有进度更糟，所以固定用 indeterminate 动画表示「在动」。 -->
                <el-progress
                    :percentage="100"
                    :stroke-width="6"
                    :show-text="false"
                    :indeterminate="true"
                    :duration="1"
                />
                <div class="scan-current" :title="scanOf(s.id)!.current_dir || '/'">
                  <span class="scan-current-label">当前：</span>
                  <span class="scan-current-path"><bdi>{{ scanOf(s.id)!.current_dir || '/' }}</bdi></span>
                </div>
                <div v-if="scanOf(s.id)!.attempt > 1" class="scan-retry">
                  网络异常，正在第 {{ scanOf(s.id)!.attempt }} 次重试<span
                    v-if="scanOf(s.id)!.cached_hits > 0"
                >（已扫描过的 {{ scanOf(s.id)!.cached_hits }} 个目录直接复用，不会重爬）</span>
                </div>
              </div>
            </div>
          </div>

          <!-- 进行中子任务（内联展示，无需展开；转存段 / 下载段各自独立进度条） -->
          <div v-else-if="topSubtasksOf(s.id).length" class="active-task-container">
            <div class="active-task-card">
              <div class="task-progress-header is-toggle" @click.stop="toggleSubtasks(s.id)">
                <div class="task-status-info">
                  <!-- 全在排队时不转圈：转圈意味着"在动"，而这时一个字节都没动 -->
                  <el-icon v-if="isWaitingForSlot(s.id)" :size="16" class="status-icon text-gray-400"><Clock /></el-icon>
                  <el-icon v-else :size="16" class="status-icon text-blue-500"><Loading class="is-loading" /></el-icon>
                  <span class="task-status-text">
                    {{ isWaitingForSlot(s.id) ? '等待中' : '进行中子任务' }}（{{ topSubtasksOf(s.id).length }}）
                  </span>
                </div>
                <el-icon :size="14" class="toggle-icon">
                  <ArrowDown v-if="subtasksExpanded(s.id)" />
                  <ArrowRight v-else />
                </el-icon>
              </div>
              <div v-if="subtasksExpanded(s.id)" class="file-tasks-preview">
                <div v-for="st in subtasksCapped(s.id)" :key="st.task_id" class="subtask-item">
                  <div class="subtask-head">
                    <el-tag :type="st.kind === 'download' ? 'success' : 'warning'" size="small">
                      {{ st.kind === 'download' ? '下载' : '转存' }}
                    </el-tag>
                    <span class="file-name" :title="st.name">{{ st.name }}</span>
                    <span class="subtask-stat">{{ subtaskStat(st) }}</span>
                    <el-tag :type="subtaskStatusColor(st.status)" size="small">{{ subtaskStatusText(st.status) }}</el-tag>
                  </div>
                  <el-progress
                      :percentage="clampPercent(st.progress)"
                      :stroke-width="6"
                      :show-text="false"
                      :status="subtaskProgressStatus(st.status)"
                  />

                  <!-- 文件夹聚合行：展开后逐个显示它当前正在下载的子文件。
                       只有已物化的那一批有进度（其余文件还在后端 pending 队列里），
                       所以这里刻意用「正在下载 N 个文件」而不是「共 N 个文件」。 -->
                  <template v-if="childrenOf(s.id, st.task_id).length">
                    <div class="child-toggle" @click.stop="toggleChildren(st.task_id)">
                      <el-icon :size="12" class="toggle-icon">
                        <ArrowDown v-if="childrenExpanded(st.task_id)" />
                        <ArrowRight v-else />
                      </el-icon>
                      <span>正在下载 {{ childrenOf(s.id, st.task_id).length }} 个文件</span>
                    </div>
                    <div v-if="childrenExpanded(st.task_id)" class="child-list">
                      <div
                          v-for="c in childrenCapped(s.id, st.task_id)"
                          :key="c.task_id"
                          class="subtask-item child-item"
                      >
                        <div class="subtask-head">
                          <span class="file-name" :title="c.name">{{ c.name }}</span>
                          <span class="subtask-stat">{{ subtaskStat(c) }}</span>
                          <el-tag :type="subtaskStatusColor(c.status)" size="small">
                            {{ subtaskStatusText(c.status) }}
                          </el-tag>
                        </div>
                        <el-progress
                            :percentage="clampPercent(c.progress)"
                            :stroke-width="4"
                            :show-text="false"
                            :status="subtaskProgressStatus(c.status)"
                        />
                      </div>
                      <div v-if="childrenOverflow(s.id, st.task_id) > 0" class="subtask-overflow">
                        仅显示前 {{ CHILD_RENDER_CAP }} 个，另有 {{ childrenOverflow(s.id, st.task_id) }} 个在下载…
                      </div>
                    </div>
                  </template>
                </div>
                <div v-if="subtasksOverflow(s.id) > 0" class="subtask-overflow">
                  仅显示前 {{ SUBTASK_RENDER_CAP }} 个，另有 {{ subtasksOverflow(s.id) }} 个进行中…
                </div>
              </div>
            </div>
          </div>
          <div v-else class="no-active-task">
            <span class="idle-text">当前无进行中子任务</span>
            <el-button size="small" text type="primary" @click.stop="openRunsDialog(s)">查看运行历史</el-button>
          </div>
        </el-card>
      </div>
    </div>

    <!-- 订阅详情对话框 -->
    <el-dialog v-model="detailDialogVisible" title="订阅详情" width="640px">
      <template v-if="selected">
        <div class="detail-dialog-toolbar">
          <el-button text :icon="Clock" @click="runsDialogVisible = true">
            运行历史<span v-if="runs.length > 0">（{{ runs.length }}）</span>
          </el-button>
        </div>
        <el-descriptions :column="1" border>
          <el-descriptions-item label="名称">{{ selected.name }}</el-descriptions-item>
          <el-descriptions-item label="分享链接">
            <el-link type="primary" :href="selected.share_url" target="_blank" :underline="false">
              {{ selected.share_url }}
            </el-link>
          </el-descriptions-item>
          <el-descriptions-item label="冲突策略">
            <el-tag :type="strategyTagType(selected.conflict_strategy)">
              {{ describeStrategy(selected.conflict_strategy) }}
            </el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="目标">
            <div v-for="(t, i) in selected.targets" :key="i" class="target-line">
              <el-tag :type="t.kind === 'netdisk' ? 'success' : 'warning'" size="small">
                {{ t.kind === 'netdisk' ? '网盘' : '本地' }}
              </el-tag>
              <span style="margin-left: 6px">
                {{ t.kind === 'netdisk' ? t.remote_path : t.local_path }}
              </span>
            </div>
          </el-descriptions-item>
          <el-descriptions-item label="同步范围">
            <div v-if="selected.include_paths.length > 0" class="path-tags">
              <el-tag
                  v-for="(p, i) in selected.include_paths"
                  :key="`inc-${i}`"
                  size="small"
                  type="info"
              >
                {{ p }}
              </el-tag>
            </div>
            <span v-else>同步整个分享</span>
          </el-descriptions-item>
          <el-descriptions-item label="排除规则">
            <div
                v-if="selected.exclude_patterns.length > 0 || (selected.exclude_regex_patterns?.length ?? 0) > 0"
                class="path-tags"
            >
              <el-tag
                  v-for="(p, i) in selected.exclude_patterns"
                  :key="`ex-${i}`"
                  size="small"
                  type="info"
              >
                {{ p }}
              </el-tag>
              <el-tag
                  v-for="(p, i) in (selected.exclude_regex_patterns ?? [])"
                  :key="`exr-${i}`"
                  size="small"
                  type="warning"
              >
                <span class="rule-kind">正则</span>{{ p }}
              </el-tag>
            </div>
            <span v-else>无</span>
          </el-descriptions-item>
          <el-descriptions-item label="排除目录">
            <div v-if="(selected.exclude_paths?.length ?? 0) > 0" class="path-tags">
              <el-tag
                  v-for="(p, i) in selected.exclude_paths"
                  :key="`exp-${i}`"
                  size="small"
                  type="warning"
              >
                {{ p }}
              </el-tag>
            </div>
            <span v-else>无</span>
          </el-descriptions-item>
          <el-descriptions-item label="轮询">
            <span v-if="isPollEnabled(selected.poll_config)">
              {{ describeInterval(selected.poll_config) }}
            </span>
            <span v-else>已禁用</span>
          </el-descriptions-item>
          <el-descriptions-item label="删除缺失">
            <el-tag v-if="selected.delete_missing" type="danger" size="small">开启</el-tag>
            <el-tag v-else type="info" size="small">关闭</el-tag>
          </el-descriptions-item>
        </el-descriptions>
      </template>
    </el-dialog>

    <!-- 创建/编辑对话框 -->
    <el-dialog
        v-model="dialogVisible"
        class="subscription-dialog"
        align-center
        title="编辑订阅"
        width="640px"
        @close="resetForm"
    >
      <el-form :model="form" label-width="100px" :rules="formRules" ref="formRef">
        <el-form-item label="名称" prop="name">
          <el-input v-model="form.name" placeholder="如：剧集合集同步" />
        </el-form-item>
        <el-form-item label="分享链接" prop="share_url">
          <el-input v-model="form.share_url" placeholder="https://pan.baidu.com/s/1xxx" />
        </el-form-item>
        <el-form-item label="提取码">
          <el-input v-model="form.password" placeholder="可选" maxlength="4" />
        </el-form-item>
        <el-form-item label="同步范围">
          <ShareIncludeExcludeEditor
              ref="excludeEditorRef"
              :share-url="form.share_url"
              :password="form.password || null"
              :owner-uid="selected?.owner_uid ?? null"
              :owner-logged-in="selectedOwnerLoggedIn"
              v-model:include-paths="form.include_paths"
              v-model:exclude-patterns="form.exclude_patterns"
              v-model:exclude-regex-patterns="form.exclude_regex_patterns"
              v-model:exclude-paths="form.exclude_paths"
          />
        </el-form-item>
        <el-form-item label="冲突策略">
          <el-radio-group v-model="form.conflict_strategy">
            <el-radio-button value="overwrite">覆盖式</el-radio-button>
            <el-radio-button value="versioned">新版本式</el-radio-button>
            <el-radio-button value="skip">跳过</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item label="同步目标">
          <div class="sync-mode-editor">
            <div class="target-row">
              <div class="target-switch">
                <el-switch v-model="netdiskEnabled" aria-label="转存到网盘" />
                <span class="target-label">转存到网盘</span>
              </div>
              <div class="target-path">
                <el-input
                    v-model="netdiskRemotePath"
                    :disabled="!netdiskEnabled"
                    aria-label="网盘目录"
                    placeholder="网盘路径，如 /我的资源/同步"
                />
              </div>
            </div>

            <div class="target-row">
              <div class="target-switch">
                <el-switch v-model="localEnabled" aria-label="下载到本地" />
                <span class="target-label">下载到本地</span>
              </div>
              <div class="target-path">
                <el-input
                    v-model="localTargetPath"
                    readonly
                    clearable
                    :disabled="!localEnabled"
                    aria-label="本地目录"
                    placeholder="点击「选择」指定本地目录"
                    @clear="localTargetPath = ''"
                >
                  <template #append>
                    <el-button
                        :icon="FolderOpened"
                        :disabled="!localEnabled"
                        aria-label="选择本地目录"
                        @click="openDirPicker()"
                    >选择</el-button>
                  </template>
                </el-input>
              </div>
            </div>

            <div v-if="syncModeHint" class="sync-mode-hint">{{ syncModeHint }}</div>
          </div>
        </el-form-item>
        <el-form-item label="轮询">
          <el-radio-group v-model="form.poll_config.mode" class="poll-mode-group">
            <el-radio-button value="interval">固定间隔</el-radio-button>
            <el-radio-button value="scheduled">每日定时</el-radio-button>
            <el-radio-button value="cron">Cron 表达式</el-radio-button>
            <el-radio-button value="disabled">禁用</el-radio-button>
          </el-radio-group>
        </el-form-item>
        <el-form-item v-if="form.poll_config.mode === 'interval'" label="间隔（秒）">
          <el-input-number v-model="form.poll_config.interval_secs" :min="600" :max="86400" :step="300" />
          <span style="margin-left: 8px; color: #909399; font-size: 12px">最少 600 秒（10 分钟）</span>
        </el-form-item>
        <el-form-item v-if="form.poll_config.mode === 'scheduled'" label="时刻">
          <el-time-picker
              v-model="scheduledTime"
              format="HH:mm"
              value-format="HH:mm"
              @change="onScheduledChange"
          />
        </el-form-item>
        <el-form-item v-if="form.poll_config.mode === 'cron'" label="表达式">
          <CronExprEditor ref="cronEditorRef" v-model="form.poll_config.cron_expr" />
        </el-form-item>
        <el-form-item label="删除缺失">
          <el-switch v-model="form.delete_missing" />
          <span style="margin-left: 8px; color: #909399; font-size: 12px">分享者删除文件时同步删除目标副本</span>
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="dialogVisible = false">取消</el-button>
        <el-button type="primary" @click="saveForm" :loading="saving">保存</el-button>
      </template>
    </el-dialog>

    <!-- 运行详情对话框 -->
    <el-dialog v-model="runDialogVisible" title="运行详情" width="700px">
      <div v-if="currentRun" class="run-detail">
        <el-descriptions :column="2" border size="small">
          <el-descriptions-item label="状态">
            <el-tag :type="runStatusType(currentRun.status)">{{ describeRunStatus(currentRun.status, currentRun.phase) }}</el-tag>
          </el-descriptions-item>
          <el-descriptions-item label="开始时间">{{ formatTime(currentRun.started_at) }}</el-descriptions-item>
          <el-descriptions-item label="结束时间">
            {{ currentRun.finished_at ? formatTime(currentRun.finished_at) : '—' }}
          </el-descriptions-item>
          <el-descriptions-item label="总文件">{{ runTotalCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="需处理">{{ runChangedCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="新增">{{ currentRun.added_count }}</el-descriptions-item>
          <el-descriptions-item label="修改">{{ currentRun.modified_count }}</el-descriptions-item>
          <el-descriptions-item label="删除">{{ currentRun.removed_count }}</el-descriptions-item>
          <el-descriptions-item label="覆盖">{{ runOverwrittenCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="一致跳过">{{ runUnchangedCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="其它跳过">{{ runSkippedCount(currentRun) }}</el-descriptions-item>
          <el-descriptions-item label="失败">{{ currentRun.failed_count }}</el-descriptions-item>
          <el-descriptions-item label="错误" v-if="currentRun.error">
            <span style="color: #f56c6c">{{ currentRun.error }}</span>
          </el-descriptions-item>
        </el-descriptions>
        <h4 style="margin-top: 16px">文件动作（{{ currentRun.item_total_count }}）</h4>
        <el-table :data="currentRun.items" size="small" max-height="400" v-loading="runItemsLoading">
          <el-table-column prop="path" label="路径" />
          <el-table-column prop="action" label="动作" width="80" :formatter="describeAction" />
          <el-table-column prop="target" label="目标" width="80" :formatter="describeTarget" />
          <el-table-column prop="status" label="状态" width="100" :formatter="describeItemStatus" />
          <el-table-column prop="reason" label="跳过原因" width="120" :formatter="describeReason" />
          <el-table-column prop="error" label="错误" />
        </el-table>
        <el-pagination
            v-if="currentRun.item_total_count > currentRun.item_page_size"
            style="margin-top: 12px; justify-content: flex-end"
            layout="prev, pager, next, total"
            background
            small
            :current-page="currentRun.item_page"
            :page-size="currentRun.item_page_size"
            :total="currentRun.item_total_count"
            @current-change="onRunItemsPageChange"
        />
      </div>
    </el-dialog>

    <!-- 运行历史对话框 -->
    <el-dialog v-model="runsDialogVisible" title="运行历史" width="640px">
      <el-empty v-if="runs.length === 0" description="暂无运行" />
      <el-timeline v-else>
        <el-timeline-item
            v-for="r in runs"
            :key="r.id"
            :timestamp="formatTime(r.started_at)"
            :type="runStatusType(r.status)"
        >
          <div @click="openRun(r.id)" class="run-item">
            <strong>{{ describeRunStatus(r.status, r.phase) }}</strong>
            <div class="run-stats">
              总 {{ runTotalCount(r) }} / 需处理 {{ runChangedCount(r) }}
              <span> +{{ r.added_count }}</span>
              <span> 覆盖 {{ runOverwrittenCount(r) }}</span>
              <span> 一致跳过 {{ runUnchangedCount(r) }}</span>
              <span v-if="runSkippedCount(r) > 0" style="color: #e6a23c"> 跳过 {{ runSkippedCount(r) }}</span>
              <span v-if="r.failed_count > 0" style="color: #f56c6c"> 失败 {{ r.failed_count }}</span>
            </div>
          </div>
        </el-timeline-item>
      </el-timeline>
    </el-dialog>

    <!-- 本地目录选择（对齐转存：FilePickerModal 选目录） -->
    <FilePickerModal
        v-model="dirPickerVisible"
        mode="download"
        select-type="directory"
        title="选择本地目录"
        :initial-path="dirPickerInitialPath"
        :default-download-dir="dirPickerDefaultDir"
        @confirm-download="handleDirConfirm"
        @use-default="handleDirUseDefault"
    />

    <!-- 创建入口：复用转存对话框（默认勾“保持同步”→ 创建订阅） -->
    <TransferDialog
        v-model="showTransferDialog"
        :default-keep-sync="true"
        :lock-keep-sync="true"
        @sync-created="onSyncCreated"
    />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import type { AxiosError } from 'axios'
import { useAuthStore } from '@/stores/auth'
import { useIsMobile } from '@/utils/responsive'
import AccountFilter from '@/components/AccountFilter.vue'
import AccountBadge from '@/components/AccountBadge.vue'
import TransferDialog from '@/components/TransferDialog.vue'
import ShareIncludeExcludeEditor from '@/components/ShareIncludeExcludeEditor.vue'
import CronExprEditor from '@/components/CronExprEditor.vue'
import { FilePickerModal } from '@/components/FilePicker'
import { getConfig, updateRecentDirDebounced, setDefaultDownloadDir, type DownloadConfig } from '@/api/config'
import {
  Plus, Edit, Delete, Refresh, RefreshRight, Link,
  FolderOpened, VideoPause, VideoPlay, Loading, Clock, ArrowDown, ArrowRight, Close,
} from '@element-plus/icons-vue'
import {
  type ShareSubscription,
  type SyncTarget,
  type NetdiskTarget,
  type LocalTarget,
  type UpdateShareSubscriptionRequest,
  type RunRecord,
  type RunDetail,
  type ConflictStrategy,
  type PollConfig,
  type ShareSyncWsEvent,
  type ShareSyncSubtask,
  listSubscriptions, updateSubscription,
  deleteSubscription, setSubscriptionEnabled, triggerSubscription, resumeSubscription, cancelScan, listRuns, getRun, listRunItems, listSubtasks,
} from '@/api/shareSync'
import { getWebSocketClient, connectWebSocket, type ConnectionState } from '@/utils/websocket'
import { createAdaptivePoller } from '@/utils/backendHealth'

const subscriptions = ref<ShareSubscription[]>([])
const selected = ref<ShareSubscription | null>(null)

const isMobile = useIsMobile()

// 多账号：账号过滤（null=全部账号）
const authStore = useAuthStore()
const ownerFilter = ref<number | null>(null)

// 按账号过滤后的订阅列表（与 transfer/autobackup 一致：null 显示全部）
const displayedSubscriptions = computed(() => {
  if (ownerFilter.value === null) return subscriptions.value
  return subscriptions.value.filter(s => s.owner_uid === ownerFilter.value)
})

// 当前选中订阅的所属账号是否已登录：未登录则禁用依赖网盘 client 的操作（触发同步 / 预览目录树）
// 并提示登录该账号（对齐 AutoBackup：不要求用户先切号，但 owner 未登录时网盘操作不可用）。
const selectedOwnerLoggedIn = computed(() => {
  if (!selected.value) return false
  return authStore.accounts.some(a => a.uid === selected.value!.owner_uid)
})

// 各账号订阅数量（AccountFilter badge 展示）
const ownerFilterCounts = computed(() => {
  const map: Record<number, number> = {}
  for (const s of subscriptions.value) {
    if (typeof s.owner_uid === 'number') map[s.owner_uid] = (map[s.owner_uid] || 0) + 1
  }
  return map
})
const runs = ref<RunRecord[]>([])
const currentRun = ref<RunDetail | null>(null)
const currentRunId = ref<string | null>(null)
const runItemsLoading = ref(false)

// 进行中子任务：subscription_id -> 子任务列表（WS item_progress 实时更新 + REST 轮询兜底）
const activeSubtasks = ref<Map<string, ShareSyncSubtask[]>>(new Map())

function subtasksOf(id: string): ShareSyncSubtask[] {
  return activeSubtasks.value.get(id) ?? []
}

/**
 * 顶层子任务行（转存段 / 单文件下载 / 文件夹聚合行），文件夹的子文件不算在内。
 *
 * 用它而不是裸的 `subtasksOf` 做计数和「等待槽位」判定：子文件行是同一批工作的
 * 更细粒度视图，混进来会把「进行中子任务（1）」显示成（6），也会让
 * `isWaitingForSlot` 把文件夹和它的子任务重复计一遍。
 *
 * **孤儿兜底**：父行不在列表里的子行提升为顶层。REST 快照由后端
 * `drop_orphan_children` 保证自洽，但 WS 是增量的——父行到终态被 `upsertSubtask`
 * 移除后，子行可能还在 map 里。不兜底的话这些行会从界面上凭空消失。
 */
function topSubtasksOf(id: string): ShareSyncSubtask[] {
  const list = subtasksOf(id)
  if (list.length === 0) return list
  const ids = new Set(list.map(st => st.task_id))
  return list.filter(st => !st.parent_task_id || !ids.has(st.parent_task_id))
}

/** 某个文件夹聚合行当前已物化的子文件行（按名称稳定排序，避免每帧乱跳） */
function childrenOf(id: string, parentTaskId: string): ShareSyncSubtask[] {
  return subtasksOf(id)
      .filter(st => st.parent_task_id === parentTaskId)
      .sort((a, b) => a.name.localeCompare(b.name))
}

/**
 * 转存段真正在干活的状态。
 *
 * 注意不含 `downloading`：转存任务在下载阶段会被标成 `TransferStatus::Downloading`，
 * 那只是它对自己下载子任务的镜像，不代表转存段还在做事。若把它当成「在干活」，
 * 下面的 `isWaitingForSlot` 会永远返回 false（列表里始终有这条转存任务）。
 */
const TRANSFER_ACTIVE = new Set(['checkingshare', 'transferring', 'cleaning'])

/**
 * 该订阅的下载子任务是不是全都还在排队（没有一个真的在跑）。
 *
 * 下载槽位数 = max_concurrent_tasks（非会员 1 / VIP 3 / SVIP 5）。同时触发多个
 * 订阅时，抢不到槽位的那些子任务停在 `pending`，此时卡片仍写「进行中子任务」会
 * 让用户以为在下载、实际一个字节没动。槽位多的时候本来就能几个订阅一起跑，
 * 这个状态自然不会出现。
 *
 * 只看 `kind === 'download'`：子任务列表里还混着转存段那条任务，拿它一起判会
 * 误判（见 `TRANSFER_ACTIVE`）。转存段确实在干活时也不算「等待中」——那时是真
 * 有进展，只是进展不在下载段。
 */
function isWaitingForSlot(id: string): boolean {
  const list = topSubtasksOf(id).filter(st => !SUBTASK_TERMINAL.has(st.status))
  if (list.some(st => st.kind === 'transfer' && TRANSFER_ACTIVE.has(st.status))) return false
  const downloads = list.filter(st => st.kind === 'download')
  return downloads.length > 0 && downloads.every(st => st.status === 'pending')
}

// ==================== 抓取阶段进度 ====================

/**
 * 抓取阶段进度：subscription_id -> 最近一帧 scan_progress。
 *
 * 纯 WS 驱动、不持久化——它只在扫描进行中有意义。WS 断线或页面刷新导致这里为空时，
 * 由 run 记录上的 `phase` 字段兜底（见 `describeRunStatus`），用户至少还能看到
 * 「扫描目录中」而不是裸的「运行中」。
 */
type ScanProgressState = {
  run_id: string
  dirs_done: number
  dirs_pending: number
  files_seen: number
  current_dir: string
  attempt: number
  cached_hits: number
  /** 本地收到该帧的时间戳，用于剔除「run 已死但结束事件没送达」的僵尸卡片 */
  received_at: number
}
const scanProgress = ref<Map<string, ScanProgressState>>(new Map())

function scanOf(id: string): ScanProgressState | undefined {
  return scanProgress.value.get(id)
}

function setScanProgress(id: string, st: Omit<ScanProgressState, 'received_at'> | null) {
  const next = new Map(scanProgress.value)
  if (st) next.set(id, { ...st, received_at: Date.now() })
  else next.delete(id)
  scanProgress.value = next
}

/**
 * 各订阅当前已知的最新一轮 run_id。按订阅清理状态前先比对 run 身份，
 * 迟到的旧事件不得抹掉新一轮的扫描进度。
 */
const latestRunIds = ref<Map<string, string>>(new Map())

function noteLatestRun(subId: string, runId?: string | null) {
  if (!runId) return
  const next = new Map(latestRunIds.value)
  next.set(subId, runId)
  latestRunIds.value = next
}

/**
 * 该事件是否仍对应当前订阅的最新一轮；尚无记录（页面刚打开）时视为最新。
 */
function isLatestRun(subId: string, runId?: string | null): boolean {
  const latest = latestRunIds.value.get(subId)
  if (!latest) return true
  if (runId && latest !== runId) return false
  return true
}

/**
 * 只在状态仍属于该轮时清理扫描进度，避免旧响应/旧事件抹掉新一轮的取消入口。
 */
function clearScanProgressForRun(subId: string, runId?: string | null): boolean {
  if (!isLatestRun(subId, runId)) return false
  setScanProgress(subId, null)
  return true
}

/** 同上，并连同该订阅的进行中子任务一起清理。 */
function clearRunStateForRun(subId: string, runId?: string | null): boolean {
  if (!clearScanProgressForRun(subId, runId)) return false
  const next = new Map(activeSubtasks.value)
  next.delete(subId)
  activeSubtasks.value = next
  return true
}

// 后端每 ~500ms 推一帧；超过 20s 没有新帧，说明 run 早已结束或断连，
// 而结束事件没送达（WS 掉线等）。留着会让卡片永远转圈，比没有进度更误导。
const SCAN_STALE_MS = 20_000
let scanStaleTimer: ReturnType<typeof setInterval> | null = null

function pruneStaleScanProgress() {
  if (scanProgress.value.size === 0) return
  const now = Date.now()
  const next = new Map(scanProgress.value)
  let changed = false
  for (const [k, v] of next) {
    if (now - v.received_at > SCAN_STALE_MS) {
      next.delete(k)
      changed = true
    }
  }
  if (changed) scanProgress.value = next
}

/** 计数式摘要。刻意不给百分比：BFS 的待扫描数会边扫边涨，百分比只会倒退。 */
function scanSummary(p: ScanProgressState): string {
  const parts = [`已扫描 ${p.dirs_done} 个目录`]
  if (p.dirs_pending > 0) parts.push(`待扫描 ${p.dirs_pending}`)
  parts.push(`发现 ${p.files_seen} 个文件`)
  return parts.join(' · ')
}

// 子任务列表默认折叠（几千文件时不铺满卡片）；按订阅记忆展开态。
const expandedSubtasks = ref<Set<string>>(new Set())
function subtasksExpanded(id: string): boolean {
  return expandedSubtasks.value.has(id)
}
function toggleSubtasks(id: string) {
  const next = new Set(expandedSubtasks.value)
  if (next.has(id)) next.delete(id)
  else next.add(id)
  expandedSubtasks.value = next
}

// 展开时也只渲染前 N 行（避免上千个 el-progress 撑爆 DOM），其余用计数提示。
const SUBTASK_RENDER_CAP = 200
function subtasksCapped(id: string): ShareSyncSubtask[] {
  return topSubtasksOf(id).slice(0, SUBTASK_RENDER_CAP)
}
function subtasksOverflow(id: string): number {
  return Math.max(0, topSubtasksOf(id).length - SUBTASK_RENDER_CAP)
}

/**
 * 文件夹子文件行的渲染上限。
 *
 * 正常情况下后端只会吐出「已物化」的那一批（约等于下载槽位数，个位数），这个上限
 * 是防御性的：多文件夹并发 + 借调槽位时条数会涨，不设上限的话嵌套列表可能把卡片撑爆。
 */
const CHILD_RENDER_CAP = 50
function childrenCapped(id: string, parentTaskId: string): ShareSyncSubtask[] {
  return childrenOf(id, parentTaskId).slice(0, CHILD_RENDER_CAP)
}
function childrenOverflow(id: string, parentTaskId: string): number {
  return Math.max(0, childrenOf(id, parentTaskId).length - CHILD_RENDER_CAP)
}

// 文件夹子文件列表默认折叠：一个订阅可能同时跑多个文件夹，全展开会很吵。
// 按 `task_id`（folder:{id}）记忆展开态，与订阅级的 `expandedSubtasks` 相互独立。
const expandedChildren = ref<Set<string>>(new Set())
function childrenExpanded(parentTaskId: string): boolean {
  return expandedChildren.value.has(parentTaskId)
}
function toggleChildren(parentTaskId: string) {
  const next = new Set(expandedChildren.value)
  if (next.has(parentTaskId)) next.delete(parentTaskId)
  else next.add(parentTaskId)
  expandedChildren.value = next
}

// 某订阅所属账号是否已登录（卡片级触发同步前置条件）
function ownerLoggedIn(s: ShareSubscription): boolean {
  return authStore.accounts.some(a => a.uid === s.owner_uid)
}

// 本地目录选择（与转存一致：FilePickerModal 选目录 + 最近目录联动）
const downloadConfig = ref<DownloadConfig | null>(null)
const dirPickerVisible = ref(false)

const dialogVisible = ref(false)
// 创建入口：复用转存对话框
const showTransferDialog = ref(false)
const runDialogVisible = ref(false)
const runsDialogVisible = ref(false)
const detailDialogVisible = ref(false)
const saving = ref(false)
const triggeringId = ref<string | null>(null)
const resumingId = ref<string | null>(null)
// 正在取消扫描的订阅：用于禁用重复点击
const cancellingId = ref<string | null>(null)
const formRef = ref()
const scheduledTime = ref<string>('03:00')

// cron 编辑器（保存前用其预览结果拦截已知无效的表达式）
const cronEditorRef = ref<InstanceType<typeof CronExprEditor> | null>(null)
const excludeEditorRef = ref<InstanceType<typeof ShareIncludeExcludeEditor> | null>(null)

// 路径编辑
function createDefaultTarget(): SyncTarget {
  return { kind: 'local', local_path: '', conflict_strategy: null, mode: 'share_direct' }
}

// 目标模型收窄：最多 1 个网盘 + 1 个本地（可共存=转存并下载）。
function targetKindCounts(): { netdisk: number; local: number } {
  let netdisk = 0
  let local = 0
  for (const t of form.value.targets) {
    if (t.kind === 'netdisk') netdisk++
    else if (t.kind === 'local') local++
  }
  return { netdisk, local }
}

// ==================== 同步目标开关 ====================
// 不再让用户手选模式，改为「转存到网盘」「下载到本地」两个开关，行为由组合自动推导
// （与后端 effective_transfer_ops 一致）：
//   仅网盘        = 仅 Netdisk target
//   网盘+本地     = Netdisk + Local{ mode: transfer_and_download }（后端合并成一条腿：转存一次→从副本下载）
//   仅本地        = 仅 Local{ mode: share_direct }（分享直下：转临时目录→下载→清理，网盘不留存）
// 约束：至少开启一个。

function findNetdiskTarget(): NetdiskTarget | undefined {
  return form.value.targets.find((t): t is NetdiskTarget => t.kind === 'netdisk')
}
function findLocalTarget(): LocalTarget | undefined {
  return form.value.targets.find((t): t is LocalTarget => t.kind === 'local')
}

// 关闭的开关只在表单里移除目标，但路径等草稿留在本次对话框会话里，
// 重新打开时原样恢复（不写回订阅、不跨订阅/新建泄漏）。
const netdiskDraft = ref<NetdiskTarget>({ kind: 'netdisk', remote_path: '/', save_fs_id: 0, conflict_strategy: null })
const localDraft = ref<LocalTarget>({ kind: 'local', local_path: '', conflict_strategy: null, mode: 'share_direct' })

function seedTargetDrafts(targets: SyncTarget[]) {
  const nd = targets.find((t): t is NetdiskTarget => t.kind === 'netdisk')
  const lo = targets.find((t): t is LocalTarget => t.kind === 'local')
  netdiskDraft.value = nd
      ? { ...nd }
      : { kind: 'netdisk', remote_path: '/', save_fs_id: 0, conflict_strategy: null }
  localDraft.value = lo ? { ...lo } : { kind: 'local', local_path: '', conflict_strategy: null, mode: 'share_direct' }
}

// 按开关重建目标列表，尽量保留已填的网盘/本地路径，避免误清空
function rebuildTargets(netdisk: boolean, local: boolean) {
  if (!netdisk && !local) {
    ElMessage.warning('至少需要开启一个同步目标（网盘或本地）')
    return
  }
  const nd = findNetdiskTarget()
  const lo = findLocalTarget()
  if (nd) netdiskDraft.value = { ...nd }
  if (lo) localDraft.value = { ...lo }
  const next: SyncTarget[] = []
  if (netdisk) {
    next.push({ ...netdiskDraft.value })
  }
  if (local) {
    // 有网盘目标=转存并下载（复用网盘副本）；无=分享直下。后端最终按目标存在性推导，这里保持一致。
    next.push({ ...localDraft.value, mode: netdisk ? 'transfer_and_download' : 'share_direct' })
  }
  form.value.targets = next
}

const netdiskEnabled = computed<boolean>({
  get() { return !!findNetdiskTarget() },
  set(on: boolean) { rebuildTargets(on, !!findLocalTarget()) },
})
const localEnabled = computed<boolean>({
  get() { return !!findLocalTarget() },
  set(on: boolean) { rebuildTargets(!!findNetdiskTarget(), on) },
})

const syncModeHint = computed(() => {
  const nd = netdiskEnabled.value
  const lo = localEnabled.value
  if (nd && lo) return '从网盘副本下载到本地，网盘保留。'
  if (nd) return ''
  return '下载后清理网盘临时文件。'
})

const netdiskRemotePath = computed<string>({
  get() { return findNetdiskTarget()?.remote_path ?? netdiskDraft.value.remote_path },
  set(v: string) {
    const nd = findNetdiskTarget()
    if (nd) nd.remote_path = v
    netdiskDraft.value.remote_path = v
  },
})
const localTargetPath = computed<string>({
  get() { return findLocalTarget()?.local_path ?? localDraft.value.local_path },
  set(v: string) {
    const lo = findLocalTarget()
    if (lo) lo.local_path = v
    localDraft.value.local_path = v
  },
})

const defaultForm = (): {
  name: string
  share_url: string
  password: string
  include_paths: string[]
  exclude_patterns: string[]
  exclude_regex_patterns: string[]
  exclude_paths: string[]
  targets: SyncTarget[]
  conflict_strategy: ConflictStrategy
  delete_missing: boolean
  poll_config: PollConfig
} => ({
  name: '',
  share_url: '',
  password: '',
  include_paths: [],
  exclude_patterns: [],
  exclude_regex_patterns: [],
  exclude_paths: [],
  targets: [createDefaultTarget()],
  conflict_strategy: 'overwrite',
  delete_missing: false,
  poll_config: {
    enabled: true,
    mode: 'interval',
    interval_secs: 1800,
    schedule_hour: null,
    schedule_minute: null,
    cron_expr: null,
  },
})

const form = ref(defaultForm())
seedTargetDrafts(form.value.targets)

function normalizePollConfigForUi(p?: PollConfig | null): PollConfig {
  const rawMode = p?.mode || 'interval'
  const enabled = p?.enabled !== false
  const mode = (!enabled || rawMode === 'disabled' ? 'disabled' : rawMode) as PollConfig['mode']
  const intervalSecs = Number.isFinite(Number(p?.interval_secs))
      ? Math.max(600, Number(p?.interval_secs))
      : 1800

  return {
    enabled: mode !== 'disabled',
    mode,
    interval_secs: intervalSecs,
    schedule_hour: mode === 'scheduled' ? (p?.schedule_hour ?? 3) : null,
    schedule_minute: mode === 'scheduled' ? (p?.schedule_minute ?? 0) : null,
    cron_expr: p?.cron_expr ?? null,
  }
}

function normalizePollConfigForSubmit(p: PollConfig): PollConfig {
  const mode = (p.mode || 'interval') as PollConfig['mode']
  const intervalSecs = Number.isFinite(Number(p.interval_secs))
      ? Math.max(600, Number(p.interval_secs))
      : 1800

  if (mode === 'disabled') {
    return {
      enabled: false,
      mode: 'disabled',
      interval_secs: intervalSecs,
      schedule_hour: null,
      schedule_minute: null,
      cron_expr: null,
    }
  }

  if (mode === 'interval') {
    return {
      enabled: true,
      mode: 'interval',
      interval_secs: intervalSecs,
      schedule_hour: null,
      schedule_minute: null,
      cron_expr: null,
    }
  }

  if (mode === 'cron') {
    return {
      enabled: true,
      mode: 'cron',
      interval_secs: intervalSecs,
      schedule_hour: null,
      schedule_minute: null,
      cron_expr: (p.cron_expr || '').trim(),
    }
  }

  return {
    enabled: true,
    mode: 'scheduled',
    interval_secs: intervalSecs,
    schedule_hour: p.schedule_hour ?? 3,
    schedule_minute: p.schedule_minute ?? 0,
    cron_expr: null,
  }
}

const formRules = {
  name: [{ required: true, message: '请输入名称', trigger: 'blur' }],
  share_url: [
    { required: true, message: '请输入分享链接', trigger: 'blur' },
    { pattern: /pan\.baidu\.com/, message: '必须是 pan.baidu.com 链接', trigger: 'blur' },
  ],
}

// ==================== 数据加载 ====================

async function refresh() {
  try {
    subscriptions.value = await listSubscriptions()
  } catch (e) {
    ElMessage.error(`加载订阅失败: ${getApiErrorMessage(e)}`)
    return
  }
  if (selected.value) {
    const fresh = subscriptions.value.find(s => s.id === selected.value!.id)
    if (fresh) selected.value = fresh
  }
}

async function select(s: ShareSubscription) {
  selected.value = s
  await loadRuns(s.id)
}

// 打开订阅详情弹窗
async function openDetail(s: ShareSubscription) {
  await select(s)
  detailDialogVisible.value = true
}

// 直接打开运行历史弹窗
async function openRunsDialog(s: ShareSubscription) {
  await select(s)
  runsDialogVisible.value = true
}

// 切换账号过滤时，若当前选中项被过滤掉，则清空选择，保持详情面板与列表一致
watch(ownerFilter, () => {
  if (!selected.value) return
  if (!displayedSubscriptions.value.some(s => s.id === selected.value!.id)) {
    selected.value = null
    runs.value = []
  }
})

async function loadRuns(id: string) {
  try {
    runs.value = await listRuns(id, 1, 30)
  } catch (e) {
    runs.value = []
    const status = (e as AxiosError)?.response?.status
    if (status !== 404) {
      console.error('load runs failed', e)
    }
  }
}

// 转存对话框创建订阅后回调（WS 事件也会刷新，这里显式刷一次更可靠）
function onSyncCreated() {
  refresh()
}

function openEdit(s?: ShareSubscription) {
  if (s) selected.value = s
  if (!selected.value) return
  form.value = {
    name: selected.value.name,
    share_url: selected.value.share_url,
    password: selected.value.password || '',
    include_paths: [...selected.value.include_paths],
    exclude_patterns: [...selected.value.exclude_patterns],
    exclude_regex_patterns: [...(selected.value.exclude_regex_patterns ?? [])],
    exclude_paths: [...(selected.value.exclude_paths ?? [])],
    targets: selected.value.targets.map(t => ({ ...t })) as SyncTarget[],
    conflict_strategy: selected.value.conflict_strategy,
    delete_missing: selected.value.delete_missing,
    poll_config: normalizePollConfigForUi(selected.value.poll_config),
  }
  if (form.value.poll_config.mode === 'scheduled') {
    const h = String(form.value.poll_config.schedule_hour || 0).padStart(2, '0')
    const m = String(form.value.poll_config.schedule_minute || 0).padStart(2, '0')
    scheduledTime.value = `${h}:${m}`
  } else {
    scheduledTime.value = '03:00'
    form.value.poll_config.schedule_hour = null
    form.value.poll_config.schedule_minute = null
  }
  seedTargetDrafts(form.value.targets)
  dialogVisible.value = true
}

function getApiErrorMessage(e: unknown): string {
  const ax = e as AxiosError<{ message?: string; error?: string; msg?: string; details?: string }>
  return ax?.response?.data?.message
      || ax?.response?.data?.error
      || ax?.response?.data?.msg
      || ax?.response?.data?.details
      || (e as Error)?.message
      || '未知错误'
}

function resetForm() {
  formRef.value?.resetFields?.()
  seedTargetDrafts(defaultForm().targets)
}

async function saveForm() {
  try {
    await formRef.value.validate()
  } catch {
    return
  }
  syncScheduledTimeFromPicker()
  // 存在正则规则时，必须由后端确认整份配置合法后才提交（旧后端不会被静默忽略）
  const ruleCheck = await excludeEditorRef.value?.validateRules?.()
  if (ruleCheck && !ruleCheck.ok) {
    ElMessage.error(ruleCheck.message || '排除规则校验失败')
    return
  }
  buildSanitizedPayload()
  if (!validateForm()) {
    return
  }
  saving.value = true
  try {
    if (selected.value) {
      const req: UpdateShareSubscriptionRequest = {
        name: form.value.name,
        share_url: form.value.share_url,
        password: form.value.password || null,
        include_paths: form.value.include_paths,
        exclude_patterns: form.value.exclude_patterns,
        exclude_regex_patterns: form.value.exclude_regex_patterns,
        exclude_paths: form.value.exclude_paths,
        targets: form.value.targets,
        conflict_strategy: form.value.conflict_strategy,
        delete_missing: form.value.delete_missing,
        poll_config: form.value.poll_config,
      }
      await updateSubscription(selected.value.id, req)
      ElMessage.success('已更新订阅')
    }
    dialogVisible.value = false
    await refresh()
  } catch (e) {
    ElMessage.error(`保存失败: ${getApiErrorMessage(e)}`)
  } finally {
    saving.value = false
  }
}

function validateForm(): boolean {
  if (form.value.targets.length === 0) {
    ElMessage.error('请至少配置一个同步目标')
    return false
  }

  // 目标模型收窄：最多 1 网盘 + 1 本地（与后端 validate 一致）
  const { netdisk, local } = targetKindCounts()
  if (netdisk > 1) {
    ElMessage.error('最多只能配置 1 个网盘目标')
    return false
  }
  if (local > 1) {
    ElMessage.error('最多只能配置 1 个本地目标')
    return false
  }

  for (let i = 0; i < form.value.targets.length; i++) {
    const t = form.value.targets[i] as NetdiskTarget | LocalTarget
    if (t.kind === 'netdisk') {
      if (!t.remote_path || !String(t.remote_path).trim()) {
        ElMessage.error(`目标 #${i + 1}：网盘路径不能为空`)
        return false
      }
    } else if (t.kind === 'local') {
      // 仅校验非空；绝对路径/目录存在/可写交给后端 validate_local_path()
      // （按平台用 Path::is_absolute() 判断，避免前端写死 Linux 的 / 前缀误伤 Windows 的 D:\）
      const lp = String(t.local_path || '').trim()
      if (!lp) {
        ElMessage.error(`目标 #${i + 1}：本地路径不能为空`)
        return false
      }
    } else {
      ElMessage.error(`目标 #${i + 1}：未知目标类型`)
      return false
    }
  }

  if (form.value.poll_config.mode === 'interval' && form.value.poll_config.interval_secs < 600) {
    ElMessage.error('间隔模式下最少间隔为 600 秒（10 分钟）')
    return false
  }

  if (form.value.poll_config.mode === 'cron') {
    const expr = form.value.poll_config.cron_expr || ''
    const reason = expr
      ? cronEditorRef.value?.invalidReason(expr) ?? null
      : '请填写 cron 表达式'
    if (reason) {
      ElMessage.error(reason)
      return false
    }
  }

  return true
}

function syncScheduledTimeFromPicker() {
  if (form.value.poll_config.mode === 'disabled') {
    form.value.poll_config.enabled = false
    form.value.poll_config.schedule_hour = null
    form.value.poll_config.schedule_minute = null
    return
  }

  form.value.poll_config.enabled = true
  if (form.value.poll_config.mode !== 'scheduled') {
    form.value.poll_config.schedule_hour = null
    form.value.poll_config.schedule_minute = null
    return
  }

  const [hourStr = '03', minuteStr = '00'] = (scheduledTime.value || '03:00').split(':')
  const h = Number(hourStr)
  const m = Number(minuteStr)
  const hour = Number.isFinite(h) ? h : 3
  const minute = Number.isFinite(m) ? m : 0
  form.value.poll_config.schedule_hour = Math.min(23, Math.max(0, Math.round(hour)))
  form.value.poll_config.schedule_minute = Math.min(59, Math.max(0, Math.round(minute)))
  scheduledTime.value = `${String(form.value.poll_config.schedule_hour).padStart(2, '0')}:${String(form.value.poll_config.schedule_minute).padStart(2, '0')}`
}

function normalizePath(v: string): string {
  const s = v.trim().replace(/\/+/g, '/')
  if (!s) return ''
  const prefixed = s.startsWith('/') ? s : `/${s}`
  if (prefixed.length === 1) return '/'
  return prefixed.endsWith('/') ? prefixed.slice(0, -1) : prefixed
}

function normalizeRemotePath(v: string): string {
  return normalizePath(v) || '/'
}

function buildSanitizedPayload() {
  const includeSet = new Set<string>()
  for (const p of form.value.include_paths) {
    const n = normalizePath(p)
    if (n) {
      includeSet.add(n)
    }
  }
  const excludeSet = new Set<string>()
  for (const p of form.value.exclude_patterns) {
    const n = p.trim()
    if (n) {
      excludeSet.add(n)
    }
  }
  const regexSet = new Set<string>()
  for (const p of form.value.exclude_regex_patterns) {
    const n = p.trim()
    if (n && !regexSet.has(n)) {
      regexSet.add(n)
    }
  }
  const excludePathSet = new Set<string>()
  for (const p of form.value.exclude_paths) {
    const n = normalizePath(p)
    if (n) {
      excludePathSet.add(n)
    }
  }
  form.value.include_paths = Array.from(includeSet)
  form.value.exclude_patterns = Array.from(excludeSet)
  form.value.exclude_regex_patterns = Array.from(regexSet)
  form.value.exclude_paths = Array.from(excludePathSet)

  const nextTargets: SyncTarget[] = []
  for (const raw of form.value.targets) {
    const t = raw as NetdiskTarget | LocalTarget
    if (!t.kind) {
      continue
    }
    if (t.kind === 'netdisk') {
      const remote = normalizeRemotePath(String(t.remote_path || ''))
      nextTargets.push({
        kind: 'netdisk',
        remote_path: remote,
        save_fs_id: Number.isFinite(Number(t.save_fs_id)) ? Number(t.save_fs_id) : 0,
        ...(t.conflict_strategy ? { conflict_strategy: t.conflict_strategy } : {}),
      })
    } else if (t.kind === 'local') {
      // 本地路径只做 trim，不做 / 归一化（保留 Windows 的 D:\ 等平台路径原样交后端）
      const local = String(t.local_path || '').trim()
      nextTargets.push({
        kind: 'local',
        local_path: local,
        mode: t.mode === 'transfer_and_download' ? 'transfer_and_download' : 'share_direct',
        ...(t.conflict_strategy ? { conflict_strategy: t.conflict_strategy } : {}),
      })
    }
  }
  form.value.targets = nextTargets
  form.value.poll_config = normalizePollConfigForSubmit(form.value.poll_config)
}

async function removeSubscription(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  try {
    await ElMessageBox.confirm(
        `确定删除订阅 "${target.name}"？历史快照与运行记录将一并清理。`,
        '删除确认',
        { type: 'warning' }
    )
  } catch {
    return
  }
  try {
    await deleteSubscription(target.id)
    ElMessage.success('已删除')
    if (selected.value?.id === target.id) {
      selected.value = null
      runs.value = []
    }
    activeSubtasks.value.delete(target.id)
    setScanProgress(target.id, null)
    await refresh()
  } catch (e) {
    ElMessage.error(`删除失败: ${getApiErrorMessage(e)}`)
  }
}

async function toggleEnabled(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  try {
    await setSubscriptionEnabled(target.id, !target.enabled)
    ElMessage.success('已切换启用状态')
    await refresh()
  } catch (e) {
    ElMessage.error(`操作失败: ${getApiErrorMessage(e)}`)
  }
}

async function resumeNow(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  resumingId.value = target.id
  try {
    await resumeSubscription(target.id)
    ElMessage.success('已恢复轮询并立即重试一次')
    await refresh()
  } catch (e) {
    ElMessage.error(`恢复失败: ${getApiErrorMessage(e)}`)
  } finally {
    resumingId.value = null
  }
}

async function triggerNow(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  triggeringId.value = target.id
  try {
    const res = await triggerSubscription(target.id)
    if (res?.pending_resume) {
      ElMessage.warning('正在恢复重启前未完成的同步，请稍候再试')
      return
    }
    ElMessage.success(
        res?.resumed_existing
            ? '该订阅正在继续重启前未完成的同步，已唤醒其下载任务（无空闲槽位时将排队等待）'
            : '已触发同步，结果将稍后出现在运行历史'
    )
    setTimeout(() => {
      if (selected.value?.id === target.id) loadRuns(target.id)
      loadSubtasksFor(target.id)
    }, 1500)
  } catch (e) {
    ElMessage.error(`触发失败: ${getApiErrorMessage(e)}`)
  } finally {
    triggeringId.value = null
  }
}

/**
 * 取消当前扫描：只停扫描，订阅保持启用，已开始的转存/下载不受影响，
 * 已进入执行阶段时后端返回 409 并如实提示。
 *
 * 请求带上观察到的那一轮 run_id：迟到请求不会执行，迟到的旧响应也不会
 * 抹掉新一轮的扫描进度。
 */
async function cancelScanNow(s?: ShareSubscription) {
  const target = s ?? selected.value
  if (!target) return
  const scan = scanOf(target.id)
  if (!scan) return
  if (cancellingId.value) return
  cancellingId.value = target.id
  try {
    await cancelScan(target.id, scan.run_id)
    // WS 事件到达时还会再清一次（幂等）
    clearRunStateForRun(target.id, scan.run_id)
    ElMessage.success('已取消本次扫描')
    if (selected.value?.id === target.id) loadRuns(target.id)
  } catch (e) {
    ElMessage.error(`取消扫描失败: ${getApiErrorMessage(e)}`)
  } finally {
    cancellingId.value = null
  }
}

async function openRun(runId: string) {
  try {
    currentRunId.value = runId
    currentRun.value = await getRun(runId)
    runDialogVisible.value = true
  } catch (e) {
    ElMessage.error(`加载运行详情失败: ${getApiErrorMessage(e)}`)
  }
}

async function onRunItemsPageChange(page: number) {
  if (!currentRunId.value || !currentRun.value) return
  runItemsLoading.value = true
  try {
    const res = await listRunItems(currentRunId.value, page, currentRun.value.item_page_size)
    currentRun.value.items = res.items
    currentRun.value.item_page = res.page
  } catch (e) {
    ElMessage.error(`加载运行明细失败: ${getApiErrorMessage(e)}`)
  } finally {
    runItemsLoading.value = false
  }
}

// ==================== 本地目录选择（对齐转存） ====================

const dirPickerInitialPath = computed(
    () => downloadConfig.value?.recent_directory
        || downloadConfig.value?.default_directory
        || downloadConfig.value?.download_dir
        || '',
)
const dirPickerDefaultDir = computed(
    () => downloadConfig.value?.default_directory || downloadConfig.value?.download_dir || '',
)

function openDirPicker() {
  if (!findLocalTarget()) return
  dirPickerVisible.value = true
}

function applyPickedDir(path: string) {
  const lo = findLocalTarget()
  if (lo) {
    lo.local_path = String(path || '').trim()
  }
}

// FilePickerModal mode="download"：选定目录（path 原样，不做归一化）
function handleDirConfirm(payload: { path: string; setAsDefault: boolean }) {
  const { path, setAsDefault } = payload
  dirPickerVisible.value = false
  applyPickedDir(path)
  if (setAsDefault) {
    setDefaultDownloadDir({ path })
        .then(() => { if (downloadConfig.value) downloadConfig.value.default_directory = path })
        .catch(() => { /* 设默认失败不阻断填写 */ })
  }
  // 与转存一致：联动最近下载目录
  updateRecentDirDebounced({ dir_type: 'download', path })
  if (downloadConfig.value) downloadConfig.value.recent_directory = path
}

function handleDirUseDefault() {
  dirPickerVisible.value = false
  const target = dirPickerDefaultDir.value
  if (target) applyPickedDir(target)
}

// ==================== 路径编辑 / 树选择 ====================

function onScheduledChange(val: string | null) {
  if (form.value.poll_config.mode !== 'scheduled') {
    return
  }
  if (!val) return
  syncScheduledTimeFromPicker()
}

// ==================== 辅助显示 ====================

function isPollEnabled(p: PollConfig): boolean {
  return normalizePollConfigForUi(p).mode !== 'disabled'
}

function describeInterval(p: PollConfig): string {
  const normalized = normalizePollConfigForUi(p)
  if (normalized.mode === 'disabled') return '已禁用'
  if (normalized.mode === 'interval') {
    const m = Math.floor(normalized.interval_secs / 60)
    return `每 ${m} 分钟`
  }
  if (normalized.mode === 'scheduled') {
    const h = String(normalized.schedule_hour || 0).padStart(2, '0')
    const m = String(normalized.schedule_minute || 0).padStart(2, '0')
    return `每日 ${h}:${m}`
  }
  if (normalized.mode === 'cron') {
    return `Cron ${normalized.cron_expr || ''}`.trim()
  }
  return '已禁用'
}

function describeTargets(targets: SyncTarget[]): string {
  return targets.map(t => t.kind === 'netdisk' ? '网盘' : '本地').join(' + ')
}

function describeStrategy(s: ConflictStrategy): string {
  return s === 'overwrite' ? '覆盖式' : s === 'versioned' ? '新版本式' : '跳过'
}

function strategyTagType(s: ConflictStrategy): 'success' | 'warning' | 'info' {
  return s === 'overwrite' ? 'success' : s === 'versioned' ? 'warning' : 'info'
}

/**
 * 运行中 run 的阶段文案。
 *
 * 之所以要落到 run 记录上而不是只靠 WS：`scan_progress` 是瞬时事件，页面刷新或
 * WS 断线重连后就没了，只靠它用户又会退回看到裸的「运行中」。
 */
const RUN_PHASE_LABELS: Record<string, string> = {
  scanning: '扫描目录中',
  diffing: '计算差异中',
  executing: '转存/下载中',
}

function describeRunStatus(s: string, phase?: string | null): string {
  // 仅运行中的 run 展示阶段；终态 run 后端会把 phase 置空，这里再兜一层。
  if (s === 'running' && phase && RUN_PHASE_LABELS[phase]) {
    return RUN_PHASE_LABELS[phase]
  }
  return s === 'running' ? '运行中' :
      s === 'completed' ? '已完成' :
          s === 'completed_with_errors' ? '完成（部分失败）' :
              s === 'failed' ? '失败' :
                  s === 'cancelled' ? '已取消' :
                      s === 'interrupted' ? '已中断（自动重跑）' : s
}

function runStatusType(s: string): 'success' | 'warning' | 'danger' | 'info' {
  return s === 'completed' ? 'success' :
      s === 'completed_with_errors' ? 'warning' :
          s === 'failed' ? 'danger' : 'info'
}

// 运行历史「文件动作」表格里的列值原本是后端枚举英文（added/netdisk/failed…），
// 这里统一汉化展示。
const ACTION_LABELS: Record<string, string> = {
  added: '新增',
  modified: '修改',
  removed: '删除',
  skipped: '跳过',
}
const TARGET_LABELS: Record<string, string> = {
  netdisk: '网盘',
  local: '本地',
  netdisk_and_local: '网盘+本地',
}
const ITEM_STATUS_LABELS: Record<string, string> = {
  pending: '等待',
  transferring: '转存中',
  downloading: '下载中',
  deleting: '删除中',
  completed: '完成',
  failed: '失败',
  skipped: '跳过',
}
const REASON_LABELS: Record<string, string> = {
  quota_full: '网盘空间不足',
  local_disk_full: '本地磁盘空间不足',
  skip_due_to_quota_full: '网盘空间不足',
  skip_due_to_local_disk_full: '本地磁盘空间不足',
}
function describeAction(_r: unknown, _c: unknown, v: string): string {
  return ACTION_LABELS[v] ?? v
}
function describeTarget(_r: unknown, _c: unknown, v: string): string {
  return TARGET_LABELS[v] ?? v
}
function describeItemStatus(_r: unknown, _c: unknown, v: string): string {
  return ITEM_STATUS_LABELS[v] ?? v
}
function describeReason(_r: unknown, _c: unknown, v: string | null | undefined): string {
  if (!v) return '—'
  return REASON_LABELS[v] ?? v
}

function runTotalCount(r: RunRecord | RunDetail): number {
  return r.total_count || (r.added_count + r.modified_count + r.removed_count + runUnchangedCount(r))
}

function runChangedCount(r: RunRecord | RunDetail): number {
  return r.added_count + r.modified_count + r.removed_count
}

function runUnchangedCount(r: RunRecord | RunDetail): number {
  return r.unchanged_count ?? 0
}

function runSkippedCount(r: RunRecord | RunDetail): number {
  return r.skipped_count ?? 0
}

function runOverwrittenCount(r: RunRecord | RunDetail): number {
  return r.overwritten_count ?? 0
}

function formatTime(ts: number): string {
  if (!ts) return '—'
  return new Date(ts * 1000).toLocaleString('zh-CN')
}

// ==================== 子任务进度（内联展示） ====================

function formatBytes(bytes: number): string {
  if (!bytes || bytes <= 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
  const i = Math.min(sizes.length - 1, Math.floor(Math.log(bytes) / Math.log(k)))
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`
}

function formatSpeed(bps: number): string {
  if (!bps || bps <= 0) return '0 B/s'
  return `${formatBytes(bps)}/s`
}

function clampPercent(p: number): number {
  if (!Number.isFinite(p)) return 0
  return Math.min(100, Math.max(0, Math.round(p)))
}

// 预计剩余时间（秒）格式化，与自动备份 `formatETA` 口径一致
function formatEtaSeconds(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return '--'
  if (seconds < 60) return `${Math.ceil(seconds)} 秒`
  if (seconds < 3600) {
    const minutes = Math.floor(seconds / 60)
    const secs = Math.ceil(seconds % 60)
    return `${minutes} 分 ${secs} 秒`
  }
  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  return `${hours} 小时 ${minutes} 分`
}

// 下载段：已下载 / 总大小 · 速度 · 剩余预计；转存段：已完成 / 总文件数
function subtaskStat(st: ShareSyncSubtask): string {
  if (st.kind === 'download') {
    let s = `${formatBytes(st.downloaded)} / ${formatBytes(st.total)}`
    if (st.speed > 0) {
      s += ` · ${formatSpeed(st.speed)}`
      const eta = st.eta_seconds ?? (st.total > st.downloaded ? (st.total - st.downloaded) / st.speed : null)
      if (eta != null && eta > 0) s += ` · 剩余 ${formatEtaSeconds(eta)}`
    }
    return s
  }
  return `${st.downloaded}/${st.total} 文件`
}

/**
 * 子任务终态：WS `item_progress` 收到这些状态时把该条从列表移除。
 *
 * 必须与后端 `is_terminal_subtask_status` 一致 —— REST `subtasks()` 用后端那份口径
 * 过滤，两边不一致时会出现「刷新后没了、不刷新一直挂着」的割裂。
 *
 * 转存段(`TransferStatus`)特有的三个终态容易漏：
 * - `transferred`：纯网盘目标的**正常终点**（无自动下载）。漏了它，纯网盘订阅每次
 *   同步完成后都会永久留着一条「已转存」的转存子任务。
 * - `transferfailed` / `downloadfailed`：失败终态，失败原因在运行历史里逐文件可查。
 */
const SUBTASK_TERMINAL = new Set([
  'completed',
  'failed',
  'cancelled',
  'success',
  'transferred',
  'transferfailed',
  'downloadfailed',
])

/**
 * 子任务状态的中文文案。
 *
 * 状态字符串是后端三个枚举 lowercased `{:?}` 的并集（见 `collect_share_sync_subtasks`）：
 * - 下载段 `TaskStatus`：pending / downloading / decrypting / paused / completed / failed
 * - 转存段 `TransferStatus`：queued / checkingshare / transferring / transferred /
 *   transferfailed / downloading / downloadfailed / cleaning / completed
 * - 文件夹段 `FolderStatus`：scanning / downloading / paused / completed / failed /
 *   cancelled（外加抢不到槽位时被改写成的 pending）
 *
 * 漏一个的后果不是不显示，而是 `|| status` 兜底把**英文原文**直接甩给用户
 * （之前 checkingshare / cleaning / scanning / decrypting 就是这样露出来的）。
 *
 * 终态（transferred / transferfailed / downloadfailed）刻意不列：它们会被
 * `SUBTASK_TERMINAL` 从列表里剔除，永远走不到这里。
 */
function subtaskStatusText(status: string): string {
  const map: Record<string, string> = {
    pending: '等待中',
    queued: '排队中',
    preparing: '准备中',
    waiting_transfer: '等待传输',
    scanning: '扫描中',
    checkingshare: '检查分享',
    transferring: '转存中',
    downloading: '下载中',
    decrypting: '解密中',
    cleaning: '清理中',
    paused: '已暂停',
    completed: '已完成',
    success: '已完成',
    failed: '失败',
    cancelled: '已取消',
  }
  return map[status] || status
}

function subtaskStatusColor(status: string): 'success' | 'warning' | 'danger' | 'info' | 'primary' {
  switch (status) {
    case 'completed':
    case 'success': return 'success'
    case 'failed': return 'danger'
    case 'cancelled':
    case 'paused': return 'warning'
    // 真的在干活的都走 primary；等待类（pending/queued）留给 default 的 info
    case 'scanning':
    case 'checkingshare':
    case 'transferring':
    case 'downloading':
    case 'decrypting':
    case 'cleaning':
    case 'preparing':
    case 'waiting_transfer': return 'primary'
    default: return 'info'
  }
}

function subtaskProgressStatus(status: string): '' | 'success' | 'exception' | 'warning' {
  if (status === 'failed') return 'exception'
  if (status === 'paused') return 'warning'
  return ''
}

// WS item_progress 到达：按 task_id upsert；终态则移除（与 REST「仅返回进行中」语义一致）
function upsertSubtask(sid: string, st: ShareSyncSubtask) {
  const list = activeSubtasks.value.get(sid) ?? []
  const idx = list.findIndex(x => x.task_id === st.task_id)
  if (SUBTASK_TERMINAL.has(st.status)) {
    if (idx >= 0) list.splice(idx, 1)
    // 父行走了，它的子行必须跟着走：后端对终态文件夹不再吐子行（见
    // `collect_subtasks_impl`），这些残留子行永远等不到下一帧更新，不清掉就会
    // 以孤儿形态挂在卡片上不消失。
    for (let i = list.length - 1; i >= 0; i--) {
      if (list[i].parent_task_id === st.task_id) list.splice(i, 1)
    }
  } else if (idx >= 0) {
    list[idx] = st
  } else {
    list.push(st)
  }
  if (list.length > 0) {
    activeSubtasks.value.set(sid, list)
  } else {
    activeSubtasks.value.delete(sid)
  }
  // 触发 Map 的响应式刷新（Map 的原地修改不会触发 ref 更新）
  activeSubtasks.value = new Map(activeSubtasks.value)
}

async function loadSubtasksFor(id: string) {
  try {
    const list = await listSubtasks(id)
    // 防御：只展示未到终态的子任务。后端「进行中」接口已过滤终态，这里再兜一层，
    // 与 WS upsertSubtask 的剔除口径一致，避免切换页面后已完成的文件夹被当成进行中。
    const active = list.filter(st => !SUBTASK_TERMINAL.has(st.status))
    const next = new Map(activeSubtasks.value)
    if (active.length > 0) {
      next.set(id, active)
    } else {
      next.delete(id)
    }
    activeSubtasks.value = next
  } catch {
    // 子任务加载失败不阻断页面（账号未就绪时后端返回空，异常时静默）
  }
}

async function loadSubtasksForAll() {
  await Promise.all(subscriptions.value.map(s => loadSubtasksFor(s.id)))
}

// ==================== 生命周期 ====================

let unsubWs: (() => void) | null = null
let unsubConnState: (() => void) | null = null
const wsConnected = ref(false)

const hasActiveSubtasks = computed(() => {
  for (const list of activeSubtasks.value.values()) {
    if (list.length > 0) return true
  }
  return false
})

// 自适应轮询器：WS 未连接且存在活跃子任务时兜底刷新 /subtasks
const subtaskPoller = createAdaptivePoller(() => {
  if (wsConnected.value || !hasActiveSubtasks.value) {
    subtaskPoller.stop()
    return
  }
  loadSubtasksForAll()
}, { baseDelayMs: 2000, maxDelayMs: 30000 })

function updateSubtaskPolling() {
  if (!wsConnected.value && hasActiveSubtasks.value) {
    subtaskPoller.start()
  } else {
    subtaskPoller.stop()
  }
}

watch(hasActiveSubtasks, updateSubtaskPolling)

onMounted(async () => {
  await refresh()
  if (subscriptions.value.length > 0 && !selected.value) {
    await select(subscriptions.value[0])
  }

  // 初始拉取一次进行中子任务（与 AutoBackup 一致：进页面即有进度，不空窗）
  await loadSubtasksForAll()

  // 加载下载目录配置（本地目标选目录时用作初始/默认目录，与转存一致）
  try {
    const appConfig = await getConfig()
    downloadConfig.value = appConfig.download
  } catch {
    // 配置加载失败不阻断页面；选目录时回退到根目录
  }

  // 订阅 WebSocket
  connectWebSocket()
  const ws = getWebSocketClient()
  ws.subscribe(['share_sync'])
  const handler = (event: CustomEvent<ShareSyncWsEvent>) => {
    const evt = event?.detail
    if (!evt || !evt.type) return
    const sid = evt.subscription_id
    if (!sid) return
    if (evt.type === 'item_progress') {
      upsertSubtask(sid, {
        task_id: evt.task_id,
        name: evt.name,
        kind: evt.kind,
        status: evt.status,
        downloaded: evt.downloaded,
        total: evt.total,
        progress: evt.progress,
        speed: evt.speed,
        eta_seconds: evt.eta_seconds ?? null,
        parent_task_id: evt.parent_task_id ?? null,
        owner_uid: evt.owner_uid ?? 0,
      })
      return
    }
    if (evt.type === 'scan_progress') {
      noteLatestRun(sid, evt.run_id)
      setScanProgress(sid, {
        run_id: evt.run_id,
        dirs_done: evt.dirs_done,
        dirs_pending: evt.dirs_pending,
        files_seen: evt.files_seen,
        current_dir: evt.current_dir,
        attempt: evt.attempt,
        cached_hits: evt.cached_hits,
      })
      return
    }
    if (['subscription_created', 'subscription_updated', 'subscription_deleted', 'status_changed'].includes(evt.type)) {
      refresh()
      return
    }
    if (evt.type === 'run_started') {
      // 新一轮开始：清掉旧的进度残留，随后由 item_progress / 轮询补齐
      noteLatestRun(sid, evt.run_id)
      loadSubtasksFor(sid)
      setScanProgress(sid, null)
    }
    if (evt.type === 'diff_detected') {
      // 进入执行段，撤掉扫描卡片；旧轮的 diff 事件不得撤掉新一轮的卡片。
      clearScanProgressForRun(sid, evt.run_id)
    }
    if (evt.type === 'run_completed' || evt.type === 'run_failed') {
      // 只清理仍属于这一轮的状态：迟到的旧完成事件不得抹掉新一轮的进度。
      clearRunStateForRun(sid, evt.run_id)
    }
    if (evt.type === 'run_cancelled') {
      // 取消只停扫描，不影响订阅；旧轮的取消事件不得抹掉新一轮的进度。
      clearRunStateForRun(sid, evt.run_id)
    }
    if (sid === selected.value?.id) {
      if (['run_started', 'run_completed', 'run_failed', 'run_cancelled', 'diff_detected'].includes(evt.type)) {
        loadRuns(sid)
      }
      // run_completed 不再弹提示（结果可在运行历史里看）；仅失败时提醒。
      if (evt.type === 'run_failed') {
        ElMessage.error(`同步失败：${evt.error || '未知错误'}`)
      }
    }
  }
  unsubWs = ws.onShareSyncEvent(handler)

  // 抓取进度是纯 WS 驱动的瞬时状态，需要定时剔除断连留下的僵尸卡片
  scanStaleTimer = setInterval(pruneStaleScanProgress, 5000)

  // 连接状态：断线时启动轮询兜底，恢复后停止并刷新一次进行中子任务
  unsubConnState = ws.onConnectionStateChange((state: ConnectionState) => {
    const wasConnected = wsConnected.value
    wsConnected.value = state === 'connected'
    updateSubtaskPolling()
    if (!wasConnected && wsConnected.value) {
      loadSubtasksForAll()
      // 断连期间的扫描进度必然已过期；清掉等新帧，避免显示陈旧计数。
      // 期间的展示由 run 记录的 phase 兜底。
      scanProgress.value = new Map()
    }
    if (!wsConnected.value) {
      // 断线立即丢弃：没有新帧还继续转圈会让用户以为仍在扫描
      scanProgress.value = new Map()
    }
  })
  updateSubtaskPolling()
})

onUnmounted(() => {
  unsubWs?.()
  unsubConnState?.()
  subtaskPoller.stop()
  if (scanStaleTimer) {
    clearInterval(scanStaleTimer)
    scanStaleTimer = null
  }
})
</script>

<style scoped lang="scss">
.share-sync-view {
  width: 100%;
  height: 100%;
  display: flex;
  flex-direction: column;
  background: #f5f5f5;
}

// 顶部工具栏（与转存管理等页面一致）
.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: white;
  border-bottom: 1px solid #e0e0e0;
  padding: 16px 20px;

  .header-left {
    display: flex;
    align-items: center;
    gap: 20px;

    h2 {
      margin: 0;
      font-size: 18px;
      color: #333;
    }
  }

  .header-right {
    display: flex;
    gap: 10px;
  }
}

.ss-content {
  flex: 1;
  // 允许收缩并内部滚动，避免内容撑高后被外层 overflow:hidden 裁掉
  min-height: 0;
  overflow: auto;
  padding: 16px 20px;
}

// 移动端底部为 fixed 标签栏（56px + 安全区）：限制本页高度，余下由 .ss-content 内部滚动
.main-layout.is-mobile .share-sync-view {
  max-height: calc(100vh - 60px - 56px - env(safe-area-inset-bottom, 0px));
}
.main-layout.is-mobile .ss-content {
  padding-bottom: 12px;
}

// ==================== 订阅卡片列表（自动备份风格） ====================
.ss-list-title {
  font-weight: 500;
  color: #303133;
  margin-bottom: 12px;
}

.config-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.config-card {
  border-left: 4px solid #409eff;
  transition: all 0.3s;
  cursor: pointer;

  &.is-disabled { border-left-color: #c0c4cc; }
  &.active { box-shadow: 0 0 0 1px #409eff inset; }
  &:hover { transform: translateY(-2px); }
}

.config-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
}
.config-info { flex: 1; min-width: 0; }
.config-title {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 8px;
  flex-wrap: wrap;
  .direction-icon { flex-shrink: 0; color: #409eff; }
  .config-name { font-size: 16px; font-weight: 500; color: #333; }
}
.config-path {
  font-size: 12px;
  color: #909399;
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px;
  .dot { color: #c0c4cc; }
  .danger-text { color: #f56c6c; }
}
.config-actions {
  display: flex;
  gap: 8px;
  flex-shrink: 0;
  flex-wrap: wrap;
}

// 进行中子任务（内联）
.active-task-container {
  margin-top: 16px;
  padding-top: 16px;
  border-top: 1px solid #ebeef5;
}
.active-task-card {
  background: #f5f7fa;
  border-radius: 8px;
  overflow: hidden;
}
.task-progress-header {
  display: flex;
  align-items: center;
  padding: 10px 12px 4px;
  &.is-toggle { cursor: pointer; user-select: none; }
  .toggle-icon { margin-left: auto; color: #909399; }
}
.task-status-info {
  display: flex;
  align-items: center;
  gap: 8px;
  .task-status-text { font-size: 14px; font-weight: 500; color: #303133; }
  .status-icon.text-blue-500 { color: #409eff; }
  .is-loading { animation: ss-rotate 1.2s linear infinite; }
}
// 固定高度 + 滚动条：几千个子任务时不再撑满卡片
.file-tasks-preview {
  padding: 4px 12px 12px;
  max-height: 320px;
  overflow-y: auto;
}
.subtask-overflow {
  padding: 8px 0 2px;
  font-size: 12px;
  color: #909399;
  text-align: center;
}
.subtask-item {
  padding: 8px 0;
  border-bottom: 1px solid #ebeef5;
  &:last-child { border-bottom: none; }
}
.subtask-head {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-bottom: 6px;
  .file-name {
    font-size: 13px;
    color: #303133;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex: 1;
    min-width: 0;
  }
  .subtask-stat { font-size: 12px; color: #909399; flex-shrink: 0; }
}

// ===== 文件夹子文件进度（嵌套一层） =====
// 文件夹聚合行下面的展开开关；点击不冒泡到订阅级的折叠开关。
.child-toggle {
  display: flex;
  align-items: center;
  gap: 4px;
  margin-top: 6px;
  font-size: 12px;
  color: #909399;
  cursor: pointer;
  user-select: none;
  &:hover { color: #409eff; }
  .toggle-icon { flex-shrink: 0; }
}
.child-list {
  // 左侧竖线 + 缩进：一眼看出这些行从属于上面那个文件夹，而不是同级子任务
  margin: 4px 0 2px 10px;
  padding-left: 10px;
  border-left: 2px solid #ebeef5;
}
.child-item {
  padding: 6px 0;
  border-bottom: none;
  .subtask-head {
    margin-bottom: 4px;
    .file-name { font-size: 12px; color: #606266; }
    .subtask-stat { font-size: 11px; }
  }
}

// ===== 抓取（递归列目录）阶段进度 =====
// 计数摘要靠右，与头部标题同一行
.scan-stat {
  margin-left: auto;
  font-size: 12px;
  color: #909399;
  flex-shrink: 0;
}
.scan-body {
  padding: 4px 12px 12px;
}
.scan-current {
  margin-top: 6px;
  font-size: 12px;
  color: #909399;
  display: flex;
  align-items: baseline;
  min-width: 0;
}
.scan-current-label {
  flex: none;
}
// rtl 让省略号出现在行首（保留结尾子目录），bdi 保持路径 LTR 顺序
.scan-current-path {
  flex: 1 1 auto;
  min-width: 0;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  direction: rtl;
  text-align: left;
}
.scan-current-path > bdi {
  direction: ltr;
  unicode-bidi: isolate;
}
// 重试提示用告警色：让用户一眼看出是网络在抖，而不是程序卡死
.scan-retry {
  margin-top: 4px;
  font-size: 12px;
  color: #e6a23c;
}
// 取消扫描按钮：贴在扫描统计右侧，弱化但可点
.scan-cancel-btn {
  margin-left: 4px;
  flex-shrink: 0;
  color: #909399;
}
.scan-cancel-btn:hover {
  color: var(--el-color-danger);
}

.no-active-task {
  margin-top: 12px;
  padding: 12px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  background: #f5f7fa;
  border-radius: 6px;
  .idle-text { font-size: 13px; color: #909399; }
}

@keyframes ss-rotate {
  from { transform: rotate(0deg); }
  to { transform: rotate(360deg); }
}

.detail-dialog-toolbar {
  display: flex;
  justify-content: flex-end;
  margin-bottom: 8px;
}

.target-line { margin: 4px 0; }
.target-form-row { display: flex; align-items: center; margin-bottom: 8px; }
.target-tip { margin-left: 8px; font-size: 12px; color: #909399; }

// 订阅编辑弹窗：头部与底部固定，只有表单主体滚动；四周至少保留 16px 视口边距。
:deep(.subscription-dialog) {
  display: flex;
  flex-direction: column;
  max-width: calc(100vw - 32px);
  max-height: calc(100vh - 32px);
  max-height: calc(100dvh - 32px);

  .el-dialog__header,
  .el-dialog__footer {
    flex: 0 0 auto;
  }

  .el-dialog__body {
    flex: 1 1 auto;
    min-height: 0;
    overflow-y: auto;
  }
}

.sync-mode-editor {
  width: 100%;

  .sync-mode-hint { margin: 8px 0 4px; font-size: 12px; color: #909399; }

  .target-row {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 4px 8px;
    margin-top: 8px;
  }

  .target-switch {
    display: flex;
    align-items: center;
    flex-shrink: 0;
    gap: 6px;
  }

  .target-label {
    font-size: 13px;
    color: #606266;
    white-space: nowrap;
  }

  .target-path {
    flex: 1 1 240px;
    min-width: 0;
  }
}

.run-item { cursor: pointer; &:hover { color: #409eff; } }
.run-stats { font-size: 12px; color: #909399; margin-top: 2px; }

.run-detail { h4 { margin: 16px 0 8px; } }

.path-editor {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
}
.path-empty, .path-empty-inline {
  color: #909399;
  font-size: 12px;
}
.path-tags { display: flex; flex-wrap: wrap; align-items: center; gap: 4px; }
.rule-kind {
  margin-right: 4px;
  padding: 0 3px;
  border-radius: 2px;
  background: rgba(0, 0, 0, 0.06);
  font-size: 11px;
}
.path-actions { display: flex; align-items: center; }

.tree-picker-toolbar {
  display: flex;
  align-items: center;
  margin-bottom: 8px;
  flex-wrap: wrap;
  gap: 4px;
}
.tree-picker-hint { margin-right: 12px; color: #909399; font-size: 12px; }
.tree-node { display: inline-flex; align-items: center; gap: 4px; }
.tree-size { margin-left: 6px; color: #909399; font-size: 11px; }
.poll-mode-group { flex-wrap: wrap; }
</style>
