<template>
  <!-- 排除规则使用说明。订阅编辑器与系统设置页共用同一份，避免两处文案漂移。 -->
  <div class="exclude-help">
    <template v-if="kind === 'regex'">
      <div>· 正则匹配的是<strong>完整路径</strong>（相对分享根目录），默认<strong>区分大小写</strong>。</div>
      <div>
        · Rust 正则语法：可用 <code>^</code> <code>$</code> <code>.*</code> <code>(a|b)</code>；
        不支持 lookaround（<code>(?=)</code> 等）与反向引用。
      </div>
      <div>
        · 例：<code>^/股票/按月归档/2025.*$</code> 排除 2025 年各月的整棵子树。
      </div>
      <div>· 文件夹路径命中时，该文件夹及其中<strong>全部内容</strong>一起排除。</div>
      <div>· 一次添加多条：<strong>每行一条</strong>；逗号等标点不作分隔。</div>
      <div>
        · 与「排除目录」（精确匹配）和通配符规则彼此独立，不会互相转换。
      </div>
    </template>
    <template v-else>
      <div>· 规则匹配的是<strong>完整路径</strong>（相对分享根目录），不只是文件名，且<strong>区分大小写</strong>。</div>
      <div>· <code>*</code> 匹配任意字符（可跨目录层级），<code>?</code> 匹配单个字符。</div>
      <div>· 文件夹路径命中规则时，该文件夹及其中<strong>全部内容</strong>一起排除。</div>
      <div>
        · 按关键词：<code>*关键词*</code>，排除路径中含该词的文件/文件夹（文件夹连同其中内容）；
        按扩展名：<code>*.格式</code>，如 <code>*.nfo</code> 排除所有 nfo 文件。
      </div>
      <div>
        · 因区分大小写，不同写法需各加一条（如 <code>*.mkv</code> 与 <code>*.MKV</code>）。
      </div>
      <div>
        · 一次添加多条：<strong>每行一条</strong>（输入一条按回车，或直接粘贴多行文本后点「添加」）。
        逗号等标点不作分隔——它们可能是文件名的一部分。
      </div>
      <div>
        · 「排除目录」是精确匹配（只认选中的那个目录及其内容），与这里的 glob 规则互不影响。
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import type { ExcludeRuleKind } from '@/utils/excludeRules'

withDefaults(defineProps<{ kind?: ExcludeRuleKind }>(), { kind: 'glob' })
</script>

<style scoped lang="scss">
.exclude-help {
  margin-top: 6px;
  padding: 8px 10px;
  font-size: 12px;
  line-height: 1.8;
  color: var(--el-text-color-secondary);
  background: var(--el-fill-color-light);
  border-radius: 6px;

  code {
    padding: 0 4px;
    background: var(--el-fill-color);
    border-radius: 3px;
    color: var(--el-text-color-primary);
  }
}
</style>
