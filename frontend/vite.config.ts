import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// https://vite.dev/config/
export default defineConfig({
  plugins: [vue()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (
            id.includes('/node_modules/@codemirror/') ||
            id.includes('/node_modules/codemirror/') ||
            id.includes('/node_modules/@lezer/')
          ) {
            return 'codemirror-vendor'
          }
          if (id.includes('/node_modules/vue/')) {
            return 'vue-vendor'
          }
          return undefined
        },
      },
    },
  },
})
