/**
 * 多账号 owner 显示工具
 *
 * 用途：在任意任务展示组件（DownloadCard / FolderCard / TransferCard / BackupCard / CloudDlCard）
 * 中将 `owner_uid` 转换为可读名（昵称 / 用户名 / `UID:xxxx`）。
 *
 * 设计要点：
 *   1. 直接读 `useAuthStore().accounts`，自动 reactive；
 *   2. 当 `owner_uid` 缺失（后端老路径未注入）或在 accounts 中找不到对应条目时，
 *      返回空字符串（即 UI 上不渲染 chip），由调用方决定是否显示 fallback；
 *   3. 提供同步函数式 API（不是 ref/computed）便于在模板里写 `:label="ownerName(task.owner_uid)"`。
 */

import { computed } from 'vue'
import type { ComputedRef } from 'vue'
import { useAuthStore } from '@/stores/auth'
import type { AccountSummary } from '@/api/accounts'

export interface UseOwnerName {
  /** 根据 uid 解析名称（昵称 → 用户名 → `UID:xxxx`） */
  ownerName: (uid?: number | null) => string
  /** 根据 uid 解析头像 URL（缺省 ''） */
  ownerAvatar: (uid?: number | null) => string
  /** 根据 uid 解析完整 AccountSummary（缺省 null） */
  ownerSummary: (uid?: number | null) => AccountSummary | null
  /** 是否需要在 UI 显示账号 chip（即 accounts.length > 1） */
  showOwnerChip: ComputedRef<boolean>
}

export function useOwnerName(): UseOwnerName {
  const authStore = useAuthStore()

  const showOwnerChip = computed(() => authStore.accounts.length > 1)

  function ownerSummary(uid?: number | null): AccountSummary | null {
    if (uid === undefined || uid === null) return null
    return authStore.accounts.find((a) => a.uid === uid) ?? null
  }

  function ownerName(uid?: number | null): string {
    if (uid === undefined || uid === null) return ''
    const summary = ownerSummary(uid)
    if (summary) {
      return summary.nickname || summary.username || `UID:${uid}`
    }
    // 找不到对应账号（可能已被删除 / accounts 尚未加载）
    return `UID:${uid}`
  }

  function ownerAvatar(uid?: number | null): string {
    if (uid === undefined || uid === null) return ''
    return ownerSummary(uid)?.avatar_url || ''
  }

  return { ownerName, ownerAvatar, ownerSummary, showOwnerChip }
}
