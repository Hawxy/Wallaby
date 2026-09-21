import { h } from 'vue';
import type { Theme } from 'vitepress';
import DefaultTheme from 'vitepress/theme';
import HeroPipeline from './HeroPipeline.vue';
import SinkLogos from './SinkLogos.vue';
import './custom.css';

export default {
  extends: DefaultTheme,
  Layout: () =>
    h(DefaultTheme.Layout, null, {
      'home-hero-image': () => h(HeroPipeline),
    }),
  enhanceApp({ app }) {
    app.component('SinkLogos', SinkLogos);
  },
} satisfies Theme;
