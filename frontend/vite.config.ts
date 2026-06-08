import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  },
  build: {
    rollupOptions: {
      output: {
        // 拆分大体积第三方依赖，避免全部打进单个 index chunk（原 ~1.2MB，触发
        // Vite 500KB 警告）。vue 全家桶与 element-plus 各自成块，利于浏览器并行
        // 加载与长效缓存（业务代码改动不会让 vendor chunk 失效）。
        manualChunks: {
          vue: ['vue', 'vue-router', 'pinia'],
          'element-plus': ['element-plus', '@element-plus/icons-vue']
        }
      }
    }
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:18888',
        changeOrigin: true
      },
      '/ws': {
        target: 'ws://127.0.0.1:18888',
        ws: true,
        changeOrigin: true,
        // 去掉 /ws 前缀，转发为 /api/v1/ws
        rewrite: (path) => path.replace(/^\/ws/, '')
      },
      // 兼容直接访问 /api/v1/ws（部分场景可能未走 /ws 前缀）
      '/api/v1/ws': {
        target: 'ws://127.0.0.1:18888',
        ws: true,
        changeOrigin: true
      }
    }
  }
})

