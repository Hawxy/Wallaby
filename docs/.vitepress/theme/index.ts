import { h, nextTick, watch } from 'vue';
import type { Theme } from 'vitepress';
import DefaultTheme from 'vitepress/theme';
import { useData } from 'vitepress';
import { createMermaidRenderer } from 'vitepress-mermaid-renderer';
import './custom.css';

export default {
  extends: DefaultTheme,
  Layout: () => {
    const { isDark, localeIndex } = useData();

    const initMermaid = () => {
      // Map diagrams onto the site palette: monochrome surfaces from
      // custom.css, amber for flow lines, blue for notes (informational)
      const fontFamily = "'Berkeley Mono', ui-monospace, monospace";
      const mermaidRenderer = createMermaidRenderer({
        theme: 'base',
        fontFamily,
        themeVariables: isDark.value
          ? {
              darkMode: true,
              fontFamily,
              background: '#0d0d0d',
              primaryColor: '#1a1a1a',
              primaryTextColor: '#e0e0e0',
              primaryBorderColor: '#3f3f3f',
              secondaryColor: '#141414',
              tertiaryColor: '#161616',
              lineColor: '#ffb454',
              textColor: '#ababab',
              clusterBkg: '#141414',
              clusterBorder: '#2a2a2a',
              edgeLabelBackground: '#1a1a1a',
              noteBkgColor: '#16222b',
              noteTextColor: '#e0e0e0',
              noteBorderColor: '#59c2ff',
            }
          : {
              darkMode: false,
              fontFamily,
              background: '#f4f3ef',
              primaryColor: '#eceae5',
              primaryTextColor: '#1c1c1c',
              primaryBorderColor: '#bab8b2',
              secondaryColor: '#e9e7e1',
              tertiaryColor: '#efede8',
              lineColor: '#9a5410',
              textColor: '#4a4a4a',
              clusterBkg: '#e9e7e1',
              clusterBorder: '#d8d6d0',
              edgeLabelBackground: '#e9e7e1',
              noteBkgColor: '#e4eaef',
              noteTextColor: '#1c1c1c',
              noteBorderColor: '#0a5d8f',
            },
      });
      mermaidRenderer.setToolbar({
        showLanguageLabel: false,
        downloadFormat: 'svg',
        fullscreenMode: 'browser',
        desktop: {
          copyCode: 'enabled',
          toggleFullscreen: 'enabled',
          resetView: 'enabled',
          zoomOut: 'enabled',
          zoomIn: 'enabled',
          zoomLevel: 'enabled',
          download: 'enabled',
        },
        fullscreen: {
          copyCode: 'disabled',
          toggleFullscreen: 'enabled',
          resetView: 'disabled',
          zoomLevel: 'disabled',
          download: 'enabled',
        },
      });
    };

    nextTick(() => initMermaid());

    watch(
      () => [isDark.value, localeIndex.value] as const,
      () => {
        initMermaid();
      },
    );

    return h(DefaultTheme.Layout);
  },
} satisfies Theme;