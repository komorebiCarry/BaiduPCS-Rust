import js from '@eslint/js'
import globals from 'globals'
import tsPlugin from '@typescript-eslint/eslint-plugin'
import tsParser from '@typescript-eslint/parser'
import vue from 'eslint-plugin-vue'
import vueParser from 'vue-eslint-parser'

// ESLint v9 flat config。Vue3 + TypeScript（<script setup>）。
export default [
  {
    ignores: ['dist/**', 'node_modules/**', 'coverage/**', '*.config.js', '*.config.ts'],
  },
  js.configs.recommended,
  ...vue.configs['flat/essential'],
  {
    files: ['**/*.{js,mjs,cjs,ts,tsx,vue}'],
    languageOptions: {
      // .vue 用 vue-eslint-parser，<script> 块再委托给 ts parser
      parser: vueParser,
      parserOptions: {
        parser: tsParser,
        ecmaVersion: 'latest',
        sourceType: 'module',
        extraFileExtensions: ['.vue'],
      },
      globals: {
        ...globals.browser,
        ...globals.node,
      },
    },
    plugins: {
      '@typescript-eslint': tsPlugin,
    },
    rules: {
      ...tsPlugin.configs.recommended.rules,
      // TS 编译器已做未定义检查，no-undef 在含类型引用（如 DOM 的 EventListener）的 .ts 上会误报，关闭。
      'no-undef': 'off',
      // 项目里大量使用 _ 前缀占位参数 / 解构丢弃；按 _ 前缀豁免
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': [
        'error',
        { argsIgnorePattern: '^_', varsIgnorePattern: '^_', caughtErrors: 'none' },
      ],
      // 零风险静态项已全部清理，提为 error 守住后续回归。
      '@typescript-eslint/no-empty-object-type': 'error',
      'no-case-declarations': 'error',
      'no-useless-escape': 'error',
      'vue/no-parsing-error': 'error',
      // no-explicit-any 暂保留为 warning：存量 ~140 处 any 收紧涉及大量运行时相关改动，
      // 留作技术债信号，后续单独 PR 逐步收紧，不在本次 lint 修复内强制。
      '@typescript-eslint/no-explicit-any': 'warn',
    },
  },
]
