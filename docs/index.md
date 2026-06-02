---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Wallaby"
  text: "Postgres CDC Engine for .NET + EF Core"
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started

features:
  - title: Automated CDC Configuration
    details: Point at your EF entities and get up & running with minimal effort. Get compile-time errors as your model changes.
  - title: Transform/Enrich Pipeline
    details: Convert, enhance & flatten materialized changes into the required shape for your output destination. Use your existing DBContext or drop down to manual SQL.
  - title: Version-triggered Backfilling
    details: Automatically run backfill operations as output shape is changed. Ensure your destination is always kept up to date.
---

