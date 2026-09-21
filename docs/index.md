---
description: "Postgres change data capture for .NET: stream row changes through typed transforms into Meilisearch, Elasticsearch, OpenSearch, Kafka, pgvector or any HTTP endpoint."
titleTemplate: "Postgres CDC for .NET"
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Wallaby"
  text: "Postgres CDC Engine for .NET"
  tagline: "Stream Postgres changes into search indexes, vector stores and event streams."
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: Browse Sinks
      link: /sinks/
    - theme: alt
      text: Star on GitHub
      link: https://github.com/Hawxy/Wallaby

features:
  - title: Automated Configuration
    details: Point at your EF Core entities, Marten documents or plain tables and get up & running with minimal effort. Get compile-time errors as your model changes.
  - title: Transform + Enrich
    details: Convert, enhance & flatten materialized changes into the required shape for your output destination. Use your existing EF & Marten tooling, or Dapper and raw Npgsql on plain tables.
  - title: Pluggable Sinks
    details: Ship your transformed data to anywhere it needs to go, be it a search index, vector DB or just a plain HTTP endpoint. At-least-once delivery ensures your data never goes missing.
  - title: Versioned Backfilling
    details: Automatically run backfill operations as output shape is changed. Ensure your destination is always up to date.
---

<SinkLogos />
