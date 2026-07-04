import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Wallaby",
  description: "Postgres CDC Engine for .NET + EF Core",
  base: '/Wallaby/',
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Docs', link: '/getting-started' }
    ],

    search: {
      provider: 'local'
    },

    sidebar: [
      {
        text: 'Usage',
        items: [
          { text: 'Getting Started', link: '/getting-started' },
          { text: 'How it works', link: '/how-it-works' },
          { text: 'Configuration', link: '/configuration' },
          { text: 'Transforms', link: '/transforms' },
          { text: 'Backfill', link: '/backfill' },
          { text: 'Multi-tenancy', link: '/multi-tenancy' },
          { text: 'External slots', link: '/external-slots' },
          { text: 'Testing', link: '/testing' },
        ]
      },
      {
        text: 'Sinks',
        items: [
          { text: 'Meilisearch', link: '/sinks/meilisearch' },
          { text: 'Custom', link: '/sinks/custom' },
        ]
      },
      {
        text: 'Operations',
        items: [
          { text: 'Observability', link: '/operations/observability' },
          { text: 'Health checks', link: '/operations/health-checks' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/Hawxy/Wallaby' }
    ],

  },
  markdown: {
    theme: { light: 'github-light-default', dark: 'ayu-dark' },
  }
})
