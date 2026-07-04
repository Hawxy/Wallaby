---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Wallaby"
  text: "Postgres CDC Engine for .NET"
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: Star on GitHub
      link: https://github.com/Hawxy/Wallaby

features:
  - title: Automated Configuration
    details: Point at your EF Core entities or custom tables and get up & running with minimal effort. Get compile-time errors as your model changes.
  - title: Transform + Enrich
    details: Convert, enhance & flatten materialized changes into the required shape for your output destination. Use your existing DBContext or drop down to manual SQL.
  - title: Pluggable Sinks
    details: Wallaby's sink model permits you to ship your transformed data to anywhere it needs to go, be it a search index, vector DB or just a plain HTTP endpoint.
  - title: Versioned Backfilling
    details: Automatically run backfill operations as output shape is changed. Ensure your destination is always kept up to date.
---

