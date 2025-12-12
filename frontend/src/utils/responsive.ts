import { ref, onMounted, onUnmounted, computed } from 'vue'

/**
 * 响应式断点配置
 */
export const BREAKPOINTS = {
  mobile: 768,   // 移动端最大宽度
  tablet: 1024,  // 平板最大宽度
} as const

/**
 * 移动设备 UA 检测正则
 * 包括：Android、iOS、Windows Phone、BlackBerry 等
 */
const MOBILE_UA_REGEX = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini|Mobile|mobile|CriOS/i

/**
 * 平板设备 UA 检测正则
 */
const TABLET_UA_REGEX = /iPad|Android(?!.*Mobile)|Tablet/i

/**
 * 检测 UA 是否为移动设备
 */
export function isMobileUA(): boolean {
  if (typeof navigator === 'undefined') return false
  return MOBILE_UA_REGEX.test(navigator.userAgent)
}

/**
 * 检测 UA 是否为平板设备
 */
export function isTabletUA(): boolean {
  if (typeof navigator === 'undefined') return false
  return TABLET_UA_REGEX.test(navigator.userAgent)
}

/**
 * 使用 matchMedia 的响应式检测（性能更好）
 * @param query 媒体查询字符串
 */
export function useMediaQuery(query: string) {
  const matches = ref(false)
  let mediaQuery: MediaQueryList | null = null

  function updateMatches() {
    if (mediaQuery) {
      matches.value = mediaQuery.matches
    }
  }

  onMounted(() => {
    mediaQuery = window.matchMedia(query)
    matches.value = mediaQuery.matches

    // 使用 addEventListener（现代浏览器）
    if (mediaQuery.addEventListener) {
      mediaQuery.addEventListener('change', updateMatches)
    } else {
      // 兼容旧浏览器
      mediaQuery.addListener(updateMatches)
    }

    // 监听屏幕旋转事件
    window.addEventListener('orientationchange', updateMatches)
    // 兼容某些浏览器的 resize 事件
    window.addEventListener('resize', updateMatches)
  })

  onUnmounted(() => {
    if (mediaQuery) {
      if (mediaQuery.removeEventListener) {
        mediaQuery.removeEventListener('change', updateMatches)
      } else {
        mediaQuery.removeListener(updateMatches)
      }
    }
    window.removeEventListener('orientationchange', updateMatches)
    window.removeEventListener('resize', updateMatches)
  })

  return matches
}

/**
 * 响应式状态管理
 * 返回当前是否为移动端、平板、桌面端
 */
export function useResponsive() {
  const isSmallScreen = useMediaQuery(`(max-width: ${BREAKPOINTS.mobile - 1}px)`)
  const isMediumScreen = useMediaQuery(
    `(min-width: ${BREAKPOINTS.mobile}px) and (max-width: ${BREAKPOINTS.tablet - 1}px)`
  )
  const isLargeScreen = useMediaQuery(`(min-width: ${BREAKPOINTS.tablet}px)`)

  // 组合判断：屏幕宽度 < 768px 或 UA 检测为移动设备
  const isMobile = computed(() => isSmallScreen.value || isMobileUA())
  const isTablet = computed(() => isMediumScreen.value || isTabletUA())
  const isDesktop = computed(() => isLargeScreen.value && !isMobileUA() && !isTabletUA())

  return {
    isMobile,
    isTablet,
    isDesktop,
    // 原始屏幕尺寸判断（不含UA）
    isSmallScreen,
    isMediumScreen,
    isLargeScreen,
  }
}

/**
 * 便捷方法：检测是否为移动端
 * 组合屏幕宽度 + UA 检测
 */
export function useIsMobile() {
  const isSmallScreen = useMediaQuery(`(max-width: ${BREAKPOINTS.mobile - 1}px)`)
  
  // 组合判断
  return computed(() => isSmallScreen.value || isMobileUA())
}

/**
 * 检测屏幕方向
 */
export function useOrientation() {
  const orientation = ref<'portrait' | 'landscape'>(
    typeof window !== 'undefined' && window.innerHeight > window.innerWidth 
      ? 'portrait' 
      : 'landscape'
  )

  function updateOrientation() {
    if (typeof window !== 'undefined') {
      orientation.value = window.innerHeight > window.innerWidth ? 'portrait' : 'landscape'
    }
  }

  onMounted(() => {
    window.addEventListener('orientationchange', updateOrientation)
    window.addEventListener('resize', updateOrientation)
  })

  onUnmounted(() => {
    window.removeEventListener('orientationchange', updateOrientation)
    window.removeEventListener('resize', updateOrientation)
  })

  return orientation
}

/**
 * 获取当前设备类型（一次性判断，非响应式）
 */
export function getDeviceType(): 'mobile' | 'tablet' | 'desktop' {
  if (typeof window === 'undefined') return 'desktop'
  
  const width = window.innerWidth
  const ua = navigator.userAgent
  
  // 优先 UA 检测
  if (MOBILE_UA_REGEX.test(ua) && !TABLET_UA_REGEX.test(ua)) {
    return 'mobile'
  }
  if (TABLET_UA_REGEX.test(ua)) {
    return 'tablet'
  }
  
  // 然后屏幕宽度判断
  if (width < BREAKPOINTS.mobile) return 'mobile'
  if (width < BREAKPOINTS.tablet) return 'tablet'
  return 'desktop'
}

/**
 * 检测是否支持触摸
 */
export function isTouchDevice(): boolean {
  if (typeof window === 'undefined') return false
  return 'ontouchstart' in window || navigator.maxTouchPoints > 0
}
