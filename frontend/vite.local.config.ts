import { defineConfig, mergeConfig } from 'vite'
import base from './vite.config'

export default mergeConfig(base, defineConfig({
  server: {
    host: '0.0.0.0',
    port: 4923,
    strictPort: true,
    allowedHosts: true,
    proxy: {
      '/api': { target: 'http://127.0.0.1:4924', changeOrigin: true },
      '/ws':  { target: 'ws://127.0.0.1:4924', ws: true, changeOrigin: true, rewrite: (p) => p.replace(/^\/ws/, '') },
      '/api/v1/ws': { target: 'ws://127.0.0.1:4924', ws: true, changeOrigin: true }
    }
  },
  preview: {
    host: '0.0.0.0',
    port: 4923,
    strictPort: true,
    allowedHosts: true,
    proxy: {
      '/api': { target: 'http://127.0.0.1:4924', changeOrigin: true },
      '/ws':  { target: 'ws://127.0.0.1:4924', ws: true, changeOrigin: true, rewrite: (p) => p.replace(/^\/ws/, '') },
      '/api/v1/ws': { target: 'ws://127.0.0.1:4924', ws: true, changeOrigin: true }
    }
  }
}))
