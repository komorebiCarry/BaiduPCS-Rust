export type FileIconCategory =
  | 'folder'
  | 'video'
  | 'audio'
  | 'image'
  | 'archive'
  | 'subtitle'
  | 'pdf'
  | 'word'
  | 'excel'
  | 'ppt'
  | 'text'
  | 'code'
  | 'app'
  | 'torrent'
  | 'ebook'
  | 'file'

const VIDEO_EXT = /\.(mkv|mp4|avi|ts|m2ts|flv|wmv|mov|rmvb|webm|m4v|mpg|mpeg)$/i
const AUDIO_EXT = /\.(mp3|flac|m4a|wav|aac|ogg|opus|ape|wma|alac|aiff)$/i
const IMAGE_EXT = /\.(jpg|jpeg|png|gif|webp|bmp|svg|heic|heif|avif|ico|tiff|raw)$/i
const ARCHIVE_EXT = /\.(zip|rar|7z|tar|gz|bz2|xz|tgz|tbz2|zst|iso|cab|lz4)$/i
const SUBTITLE_EXT = /\.(srt|ass|ssa|vtt|sub|idx|sup)$/i
const PDF_EXT = /\.pdf$/i
const WORD_EXT = /\.(doc|docx|rtf|odt|wps)$/i
const EXCEL_EXT = /\.(xls|xlsx|xlsm|csv|ods|et)$/i
const PPT_EXT = /\.(ppt|pptx|pps|ppsx|odp|dps)$/i
const TEXT_EXT = /\.(txt|md|markdown|log|nfo|readme)$/i
const CODE_EXT = /\.(json|toml|ya?ml|xml|ini|conf|env|js|mjs|cjs|ts|tsx|jsx|vue|py|rs|go|java|c|cpp|h|hpp|cs|php|rb|sh|bat|ps1|sql|html?|s?css|less)$/i
const APP_EXT = /\.(exe|msi|apk|apks|xapk|ipa|dmg|pkg|deb|rpm|appimage|bin)$/i
const TORRENT_EXT = /\.(torrent|magnet)$/i
const EBOOK_EXT = /\.(epub|mobi|azw3|fb2|djvu)$/i

/**
 * 根据是否目录与文件名扩展名识别文件图标分类
 */
export function getFileCategory(filename: string, isDir = false): FileIconCategory {
  if (isDir) return 'folder'
  const name = filename || ''
  if (VIDEO_EXT.test(name)) return 'video'
  if (AUDIO_EXT.test(name)) return 'audio'
  if (IMAGE_EXT.test(name)) return 'image'
  if (ARCHIVE_EXT.test(name)) return 'archive'
  if (SUBTITLE_EXT.test(name)) return 'subtitle'
  if (PDF_EXT.test(name)) return 'pdf'
  if (WORD_EXT.test(name)) return 'word'
  if (EXCEL_EXT.test(name)) return 'excel'
  if (PPT_EXT.test(name)) return 'ppt'
  if (TEXT_EXT.test(name)) return 'text'
  if (CODE_EXT.test(name)) return 'code'
  if (APP_EXT.test(name)) return 'app'
  if (TORRENT_EXT.test(name)) return 'torrent'
  if (EBOOK_EXT.test(name)) return 'ebook'
  return 'file'
}
