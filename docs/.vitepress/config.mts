import { defineConfig } from 'vitepress'
import llmstxt from 'vitepress-plugin-llms'

const HOSTNAME = 'https://wallabycdc.net'
const SITE_TITLE = 'Wallaby'
const SITE_DESCRIPTION = 'Postgres CDC Engine for .NET'

// Escaped so the JSON can't terminate the surrounding <script>; VitePress emits head innerHTML verbatim.
const ldJson = (value: object) => JSON.stringify(value).replace(/</g, '\\u003c')

// https://vitepress.dev/reference/site-config
export default defineConfig({
  title: SITE_TITLE,
  description: SITE_DESCRIPTION,
  base: '/',
  cleanUrls: true,
  sitemap: {
    hostname: HOSTNAME
  },
  // Canonical, Open Graph and JSON-LD on every page.
  transformPageData(pageData) {
    const path = pageData.relativePath.replace(/index\.md$/, '').replace(/\.md$/, '')
    const canonical = `${HOSTNAME}/${path}`
    const isHome = pageData.relativePath === 'index.md'

    // Mirrors VitePress' own createTitle, so a page opting out of the suffix isn't given one here.
    const title = pageData.title || SITE_TITLE
    const template = pageData.frontmatter.titleTemplate ?? true
    const ogTitle = template === false || title === SITE_TITLE
      ? title
      : typeof template === 'string' ? `${title} | ${template}` : `${title} | ${SITE_TITLE}`

    const structuredData = isHome
      ? {
          '@context': 'https://schema.org',
          '@type': 'SoftwareSourceCode',
          name: SITE_TITLE,
          description: pageData.description || SITE_DESCRIPTION,
          url: HOSTNAME,
          codeRepository: 'https://github.com/Hawxy/Wallaby',
          programmingLanguage: 'C#',
          runtimePlatform: '.NET',
          license: 'https://www.apache.org/licenses/LICENSE-2.0',
          applicationCategory: 'DeveloperApplication'
        }
      : {
          '@context': 'https://schema.org',
          '@type': 'BreadcrumbList',
          itemListElement: [
            { '@type': 'ListItem', position: 1, name: SITE_TITLE, item: HOSTNAME },
            { '@type': 'ListItem', position: 2, name: pageData.title, item: canonical }
          ]
        }

    pageData.frontmatter.head ??= []
    pageData.frontmatter.head.push(
      ['link', { rel: 'canonical', href: canonical }],
      ['meta', { property: 'og:url', content: canonical }],
      ['meta', { property: 'og:type', content: 'website' }],
      ['meta', { property: 'og:site_name', content: SITE_TITLE }],
      ['meta', { property: 'og:title', content: ogTitle }],
      ['meta', { property: 'og:description', content: pageData.description || SITE_DESCRIPTION }],
      ['script', { id: 'ld-page', type: 'application/ld+json' }, ldJson(structuredData)],
    )
  },
  vite: {
    plugins: [llmstxt({ domain: HOSTNAME })]
  },
  head: [
    ['link', { rel: 'icon', type: 'image/png', sizes: '32x32', href: '/favicon-32.png' }],
    ['link', { rel: 'icon', type: 'image/png', sizes: '16x16', href: '/favicon-16.png' }],
    ['link', { rel: 'apple-touch-icon', sizes: '180x180', href: '/apple-touch-icon.png' }],
    ['meta', { property: 'og:image', content: `${HOSTNAME}/og.png` }],
    ['meta', { property: 'og:image:width', content: '1200' }],
    ['meta', { property: 'og:image:height', content: '630' }],
    ['meta', { property: 'og:image:alt', content: 'Wallaby, Postgres change data capture for .NET' }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
  ],
  themeConfig: {
    // https://vitepress.dev/reference/default-theme-config
    nav: [
      { text: 'Home', link: '/' },
      { text: 'Docs', link: '/getting-started' },
      { text: 'Sinks', link: '/sinks/' }
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
          { text: 'RAG & Embeddings', link: '/rag' },
          { text: 'External Slots', link: '/external-slots' },
          { text: 'Configuration', link: '/configuration' },
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
          { text: 'Plain Tables', link: '/providers/tables/' },
        ]
      },
      {
        text: 'Sinks',
        link: '/sinks/',
        items: [
          { text: 'Overview', link: '/sinks/' },
          { text: 'Meilisearch', link: '/sinks/meilisearch' },
          { text: 'HTTP (Webhook)', link: '/sinks/http' },
          { text: 'Kafka', link: '/sinks/kafka' },
          { text: 'Elasticsearch', link: '/sinks/elasticsearch' },
          { text: 'OpenSearch', link: '/sinks/opensearch' },
          { text: 'Pgvector', link: '/sinks/pgvector' },
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
          { text: 'Upgrading Postgres', link: '/operations/major-version-upgrades' }
        ]
      }
    ],

    socialLinks: [
      { icon: 'github', link: 'https://github.com/Hawxy/Wallaby' }
    ],

    footer: {
      message: 'Released under the Apache 2.0 License.',
      copyright: 'Copyright © 2026-present JT'
    },

  },
  markdown: {
    theme: { light: 'github-light-high-contrast', dark: 'ayu-dark' },
  }
})
