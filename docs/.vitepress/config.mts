import { defineConfig } from 'vitepress'
import llmstxt from 'vitepress-plugin-llms'

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: "Wallaby",
  description: "Postgres CDC Engine for .NET",
  base: '/',
  cleanUrls: true,
  sitemap: {
    hostname: 'https://wallabycdc.net'
  },
  // Canonical + Open Graph tags on every page.
  transformPageData(pageData) {
    const path = pageData.relativePath.replace(/index\.md$/, '').replace(/\.md$/, '')
    const canonical = `https://wallabycdc.net/${path}`
    pageData.frontmatter.head ??= []
    pageData.frontmatter.head.push(
      ['link', { rel: 'canonical', href: canonical }],
      ['meta', { property: 'og:url', content: canonical }],
      ['meta', { property: 'og:type', content: 'website' }],
      ['meta', { property: 'og:site_name', content: 'Wallaby' }],
      ['meta', { property: 'og:title', content: pageData.title ? `${pageData.title} | Wallaby` : 'Wallaby' }],
      ['meta', { property: 'og:description', content: pageData.description || 'Postgres CDC Engine for .NET' }],
    )
  },
  vite: {
    plugins: [llmstxt({ domain: 'https://wallabycdc.net' })]
  },
  head: [
    ['link', { rel: 'icon', type: 'image/png', sizes: '32x32', href: '/favicon-32.png' }],
    ['link', { rel: 'icon', type: 'image/png', sizes: '16x16', href: '/favicon-16.png' }],
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
          { text: 'Why Wallaby?', link: '/why-wallaby' },
          { text: 'How It Works', link: '/how-it-works' },
          { text: 'Getting Started', link: '/getting-started' },
          { text: 'Mappings', link: '/mappings' },
          { text: 'Backfill', link: '/backfill' },
          { text: 'External Slots', link: '/external-slots' },
          { text: 'Configuration', link: '/configuration' },
          { text: 'Transaction Spill', link: '/transaction-spill' },
          { text: 'Testing', link: '/testing' },
        ]
      },
      {
        text: 'Storage Providers',
        items: [
          { text: 'Overview', link: '/providers/overview' },
          {
            text: 'EF Core', link: '/providers/entity-framework-core/',
            items: [
              { text: 'Multi-Tenancy', link: '/providers/entity-framework-core/multi-tenancy' },
            ]
          },
          {
            text: 'Marten', link: '/providers/marten/',
            items: [
              { text: 'Multi-Tenancy', link: '/providers/marten/multi-tenancy' },
            ]
          },
        ]
      },
      {
        text: 'Sinks',
        items: [
          { text: 'Meilisearch', link: '/sinks/meilisearch' },
          { text: 'HTTP (Webhook)', link: '/sinks/http' },
          { text: 'Kafka', link: '/sinks/kafka' },
          { text: 'Custom', link: '/sinks/custom' },
        ]
      },
      {
        text: 'Operations',
        items: [
          { text: 'Observability', link: '/operations/observability' },
          { text: 'Health Checks', link: '/operations/health-checks' },
          { text: 'External Control', link: '/operations/external-control' },
          { text: 'Upgrading Wallaby', link: '/operations/upgrades' },
          { text: 'Major-Version Upgrades', link: '/operations/major-version-upgrades' }
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
