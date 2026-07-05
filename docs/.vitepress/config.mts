import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Wallaby",
  description: "Postgres CDC Engine for .NET",
  base: '/Wallaby/',
  head: [
    ['link', { rel: 'icon', type: 'image/png', sizes: '32x32', href: '/Wallaby/favicon-32.png' }],
    ['link', { rel: 'icon', type: 'image/png', sizes: '16x16', href: '/Wallaby/favicon-16.png' }],
  ],
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
          { text: 'How It Works', link: '/how-it-works' },
          { text: 'Configuration', link: '/configuration' },
          { text: 'Transforms', link: '/transforms' },
          { text: 'Backfill', link: '/backfill' },
          { text: 'Multi-Tenancy', link: '/multi-tenancy' },
          { text: 'External Slots', link: '/external-slots' },
          { text: 'Testing', link: '/testing' },
        ]
      },
      {
        text: 'Storage Providers',
        items: [
          { text: 'Overview', link: '/providers/overview' },
          { text: 'EF Core', link: '/providers/entity-framework-core' },
          { text: 'Marten', link: '/providers/marten' },
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
          { text: 'Health Checks', link: '/operations/health-checks' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/Hawxy/Wallaby' }
    ],

  },
  markdown: {
    theme: { light: 'github-light-high-contrast', dark: 'ayu-dark' },
  }
})
