/**
 * 文件工具模块
 * 参考文档 6.5.6 节
 */

/** 加密文件名前缀 */
export const ENCRYPTED_FILE_PREFIX = 'BPR_BKUP_'

/** 加密文件扩展名 */
export const ENCRYPTED_FILE_EXTENSION = '.bkup'

/** 文件展示信息 */
export interface FileDisplayInfo {
  /** 图标名称 */
  icon: string
  /** 图标颜色 */
  iconColor: string
  /** 显示名称 */
  displayName: string
  /** 提示信息 */
  tooltip: string | null
  /** 是否为加密文件 */
  isEncrypted: boolean
  /** 原始文件名（如果可用） */
  originalName: string | null
}

/**
 * 判断文件名是否为加密文件
 * 检查文件名是否以 BPR_BKUP_ 开头且以 .bkup 结尾
 */
export function isEncryptedFile(filename: string): boolean {
  return filename.startsWith(ENCRYPTED_FILE_PREFIX) && filename.endsWith(ENCRYPTED_FILE_EXTENSION)
}

/**
 * 从加密文件名提取 UUID
 * @returns UUID 字符串，如果不是加密文件则返回 null
 */
export function extractUuidFromEncryptedName(filename: string): string | null {
  if (!isEncryptedFile(filename)) {
    return null
  }
  // BPR_BKUP_<uuid>.bkup -> 提取 uuid 部分
  const withoutPrefix = filename.substring(ENCRYPTED_FILE_PREFIX.length)
  const withoutSuffix = withoutPrefix.substring(0, withoutPrefix.length - ENCRYPTED_FILE_EXTENSION.length)
  return withoutSuffix
}

/**
 * 根据文件扩展名获取图标名称
 */
export function getFileIcon(filename: string): string {
  const ext = filename.split('.').pop()?.toLowerCase() || ''

  const iconMap: Record<string, string> = {
    // 文档
    pdf: 'Document',
    doc: 'Document',
    docx: 'Document',
    txt: 'Document',
    md: 'Document',
    rtf: 'Document',
    // 表格
    xls: 'Grid',
    xlsx: 'Grid',
    csv: 'Grid',
    // 演示
    ppt: 'DataBoard',
    pptx: 'DataBoard',
    // 图片
    jpg: 'Picture',
    jpeg: 'Picture',
    png: 'Picture',
    gif: 'Picture',
    bmp: 'Picture',
    webp: 'Picture',
    svg: 'Picture',
    ico: 'Picture',
    // 视频
    mp4: 'VideoPlay',
    avi: 'VideoPlay',
    mov: 'VideoPlay',
    mkv: 'VideoPlay',
    wmv: 'VideoPlay',
    flv: 'VideoPlay',
    webm: 'VideoPlay',
    // 音频
    mp3: 'Headset',
    wav: 'Headset',
    flac: 'Headset',
    aac: 'Headset',
    ogg: 'Headset',
    wma: 'Headset',
    // 压缩
    zip: 'Files',
    rar: 'Files',
    '7z': 'Files',
    tar: 'Files',
    gz: 'Files',
    // 代码
    js: 'Tickets',
    ts: 'Tickets',
    py: 'Tickets',
    java: 'Tickets',
    c: 'Tickets',
    cpp: 'Tickets',
    h: 'Tickets',
    rs: 'Tickets',
    go: 'Tickets',
    html: 'Tickets',
    css: 'Tickets',
    json: 'Tickets',
    xml: 'Tickets',
    // 加密文件
    bkup: 'Lock',
  }

  return iconMap[ext] || 'Document'
}

/**
 * 根据文件扩展名获取图标颜色
 */
export function getFileIconColor(filename: string): string {
  const ext = filename.split('.').pop()?.toLowerCase() || ''

  const colorMap: Record<string, string> = {
    // 文档 - 蓝色
    pdf: '#F56C6C',
    doc: '#409EFF',
    docx: '#409EFF',
    txt: '#909399',
    md: '#909399',
    // 表格 - 绿色
    xls: '#67C23A',
    xlsx: '#67C23A',
    csv: '#67C23A',
    // 演示 - 橙色
    ppt: '#E6A23C',
    pptx: '#E6A23C',
    // 图片 - 紫色
    jpg: '#9B59B6',
    jpeg: '#9B59B6',
    png: '#9B59B6',
    gif: '#9B59B6',
    webp: '#9B59B6',
    svg: '#9B59B6',
    // 视频 - 红色
    mp4: '#E74C3C',
    avi: '#E74C3C',
    mov: '#E74C3C',
    mkv: '#E74C3C',
    // 音频 - 青色
    mp3: '#17A2B8',
    wav: '#17A2B8',
    flac: '#17A2B8',
    // 压缩 - 棕色
    zip: '#8B4513',
    rar: '#8B4513',
    '7z': '#8B4513',
    // 代码 - 灰色
    js: '#F0DB4F',
    ts: '#3178C6',
    py: '#3776AB',
    java: '#007396',
    rs: '#DEA584',
    go: '#00ADD8',
    // 加密文件 - 红色
    bkup: '#F56C6C',
  }

  return colorMap[ext] || '#909399'
}

/**
 * 获取文件展示信息
 * 对加密文件进行特殊展示
 */
export function getFileDisplayInfo(filename: string, serverFilename?: string): FileDisplayInfo {
  const name = serverFilename || filename

  if (isEncryptedFile(name)) {
    return {
      icon: 'Lock',
      iconColor: '#F56C6C',
      displayName: name,
      tooltip: '加密文件 - 下载时自动解密',
      isEncrypted: true,
      originalName: null, // 原始文件名需要从快照表查询
    }
  }

  return {
    icon: getFileIcon(name),
    iconColor: getFileIconColor(name),
    displayName: name,
    tooltip: null,
    isEncrypted: false,
    originalName: null,
  }
}

/**
 * 格式化文件大小
 */
export function formatFileSize(bytes: number): string {
  if (bytes === 0) return '0 B'

  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let unitIndex = 0
  let size = bytes

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex++
  }

  return `${size.toFixed(2)} ${units[unitIndex]}`
}

/**
 * 获取文件扩展名
 */
export function getFileExtension(filename: string): string {
  const lastDot = filename.lastIndexOf('.')
  if (lastDot === -1 || lastDot === filename.length - 1) {
    return ''
  }
  return filename.substring(lastDot + 1).toLowerCase()
}

/**
 * 获取不带扩展名的文件名
 */
export function getFileNameWithoutExtension(filename: string): string {
  const lastDot = filename.lastIndexOf('.')
  if (lastDot === -1) {
    return filename
  }
  return filename.substring(0, lastDot)
}

/**
 * 检查是否为图片文件
 */
export function isImageFile(filename: string): boolean {
  const ext = getFileExtension(filename)
  return ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'webp', 'svg', 'ico'].includes(ext)
}

/**
 * 检查是否为视频文件
 */
export function isVideoFile(filename: string): boolean {
  const ext = getFileExtension(filename)
  return ['mp4', 'avi', 'mov', 'mkv', 'wmv', 'flv', 'webm'].includes(ext)
}

/**
 * 检查是否为音频文件
 */
export function isAudioFile(filename: string): boolean {
  const ext = getFileExtension(filename)
  return ['mp3', 'wav', 'flac', 'aac', 'ogg', 'wma'].includes(ext)
}

/**
 * 检查是否为文档文件
 */
export function isDocumentFile(filename: string): boolean {
  const ext = getFileExtension(filename)
  return ['pdf', 'doc', 'docx', 'txt', 'md', 'rtf', 'xls', 'xlsx', 'csv', 'ppt', 'pptx'].includes(ext)
}

/**
 * 跨设备场景错误提示
 */
export const CROSS_DEVICE_ERROR_MESSAGES = {
  NO_MAPPING: '无法解密：缺少映射信息。此加密文件可能是在其他设备上传的。',
  KEY_MISMATCH: '无法解密：密钥版本不匹配。请确认使用正确的加密密钥。',
  NO_KEY: '无法解密：未配置加密密钥。请在设置中导入密钥后重试。',
  DECRYPT_FAILED: '解密失败：密钥错误或文件已损坏。',
}
