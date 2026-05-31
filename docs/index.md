---
# https://vitepress.dev/reference/default-theme-home-page
layout: home

hero:
  name: "Wallaby"
  text: "Postgres CDC for .NET & EF Core"
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started

features:
  - title: Automated CDC Configuration
    details: Point at your EF entities and get up & running with minimal effort. Get compile-time errors as your model changes.
  - title: Transform/Enrich pipeline
    details: Convert, enhance & flatten materialized changes into the required shape for your output destination. 
  - title: Version-triggered backfilling
    details: Automatically run backfill operations as output shape is changed.  
---

