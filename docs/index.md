---
description: "Postgres change data capture for .NET: stream row changes through typed transforms into search indexes, webhooks, and custom sinks."
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
    details: Point at your EF Core or Marten entities and get up & running with minimal effort. Get compile-time errors as your model changes.
  - title: Transform + Enrich
    details: Convert, enhance & flatten materialized changes into the required shape for your output destination. Use your existing EF & Marten tooling or drop down to manual SQL.
  - title: Pluggable Sinks
    details: Ship your transformed data to anywhere it needs to go, be it a search index, vector DB or just a plain HTTP endpoint. At-least-once delivery ensures your data never goes missing.
  - title: Versioned Backfilling
    details: Automatically run backfill operations as output shape is changed. Ensure your destination is always up to date.
---

