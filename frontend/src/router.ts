import { createRouter, createWebHashHistory } from 'vue-router'
import HomePage from './views/HomePage.vue'

const router = createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', name: 'home', component: HomePage },
    {
      path: '/notebook/:sessionId',
      name: 'notebook',
      component: () => import('./views/NotebookPage.vue'),
      props: true,
    },
  ],
})

export default router
