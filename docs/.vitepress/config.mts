import { defineConfig } from 'vitepress'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Wallaby",
  description: "Postgres CDC for .NET & EFCore",
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Docs', link: '/getting-started' }
    ],

    sidebar: [
      {
        text: 'Usage',
        items: [
          { text: 'Getting Started', link: '/getting-started' },
          { text: 'How it works', link: '/how-it-works' },
          { text: 'Transforms', link: '/transforms' },
          { text: 'Backfill', link: '/backfill' },
          { text: 'Multi-tenancy', link: '/multi-tenancy' }
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
          { text: 'Observability', link: '/observability' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/Hawxy/Wallaby' }
    ]
  }
})
