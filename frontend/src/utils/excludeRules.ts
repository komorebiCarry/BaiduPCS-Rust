/**
 * 分享同步「排除规则」输入解析工具。
 *
 * 被订阅编辑器（ShareIncludeExcludeEditor）与系统设置页（常用词表）共用，
 * 保证两处输入行为一致。
 *
 * 设计约束：**换行是唯一的批量分隔符。**
 * - 逗号/分号/顿号都是合法的文件名字符（Windows/百度网盘仅禁止 `\/:*?"<>|`），
 *   拿它们当分隔符会把 `*第1，2集*` 这类真实规则静默切坏；换行不可能出现在
 *   文件名里，永不冲突。
 * - 输入框用 textarea：回车提交当前内容（逐条输入），粘贴多行文本则每行一条。
 * - 不按空格切分 —— 规则常含空格（如 `*S02 连载中*`）。
 */

/** 规则类型：历史字段按通配符解释，正则需显式选择 */
export type ExcludeRuleKind = 'glob' | 'regex'

/** 把输入文本解析成规则列表：按行拆分，行内内容（含逗号等标点）原样保留。 */
export function parseRuleInput(text: string): string[] {
  return text
    .split(/[\r\n]+/)
    .map((s) => s.trim())
    .filter(Boolean)
}

/** 把若干规则合并进已有列表：去重、保序，返回新数组（不修改入参） */
export function mergeRules(existing: readonly string[], incoming: readonly string[]): string[] {
  const next = [...existing]
  for (const p of incoming) {
    const t = p.trim()
    if (t && !next.includes(t)) next.push(t)
  }
  return next
}

/** 旧响应可能缺字段：统一归一为数组（去空白、去重、保序） */
export function normalizeRuleList(list: readonly string[] | null | undefined): string[] {
  return mergeRules([], list ?? [])
}

/** 两份规则列表内容是否完全一致（用于丢弃过期的异步结果） */
export function sameRuleList(a: readonly string[], b: readonly string[]): boolean {
  if (a.length !== b.length) return false
  return a.every((v, i) => v === b[i])
}

/** 规则类型的展示名 */
export function ruleKindLabel(kind: ExcludeRuleKind): string {
  return kind === 'regex' ? '正则表达式' : '通配符'
}

/**
 * 新增批次的错误文案：落在本次新增范围内的下标换算成「本次新增第 N 条」，
 * 让多行一次添加时能对应回输入框里的那一行；其余错误仍用整份列表下标。
 */
export function describeRuleErrorFrom(
  err: { kind: string; index?: number; message: string },
  addedKind: ExcludeRuleKind,
  baseLength: number,
): string {
  if (err.kind === addedKind && typeof err.index === 'number' && err.index >= baseLength) {
    const label = err.kind === 'regex' ? '正则' : '通配符'
    return `${label}本次新增第 ${err.index - baseLength + 1} 条：${err.message}`
  }
  return describeRuleError(err)
}

/**
 * 规则错误转成一行可读文案。
 *
 * 下标是 0 起的列表位置，展示时 +1；与后端 `kind/index/message` 契约一致。
 */
export function describeRuleError(err: {
  kind: string
  index?: number
  message: string
}): string {
  const label = err.kind === 'regex'
    ? '正则'
    : err.kind === 'glob'
      ? '通配符'
      : err.kind === 'path'
        ? '测试路径'
        : '规则'
  const where = typeof err.index === 'number' ? `第 ${err.index + 1} 条：` : ''
  return `${label}${where}${err.message}`
}
