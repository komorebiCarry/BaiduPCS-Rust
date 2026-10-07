/**
 * 会员展示工具
 *
 * vip_type：0=普通用户，1=普通会员(VIP)，2=超级会员(SVIP)
 * vip_level：会员成长等级（后端后台刷新，可能暂缺）
 */

/** 会员标签，如 `VIP2` / `SVIP5`；普通用户返回空串，等级暂缺时只返回 `VIP` / `SVIP` */
export function vipLabel(vipType?: number | null, vipLevel?: number | null): string {
  if (!vipType) return ''
  return `${vipType === 2 ? 'SVIP' : 'VIP'}${vipLevel ?? ''}`
}
