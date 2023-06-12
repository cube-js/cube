---
title: APIs & Integrations
menuTitle: Overview
permalink: /apis-integrations
category: APIs & Integrations
menuOrder: 1
---

With a rich set of APIs, Cube can power and deliver data to all kinds of
data applications.

<img
  src="https://ucarecdn.com/df9de86e-4829-4c9d-9882-0984fbd8c719/"
  style="border: 0;"
/>

## Data APIs

A few rules of thumb to help you choose an API:

When implementing internal or self-serve [business intelligence][cube-issbi]
use case, pick the [SQL API][ref-sql-api] and [Semantic Layer Sync][ref-sls].
The SQL API allows querying Cube with a Postgres-compatible
[dialect of SQL][ref-sql-syntax], either by writing queries manually or
generating them with BI tools.

When implementing [embedded analytics][cube-ea] and
[real-time analytics][cube-rta] use cases, pick [REST API][ref-rest-api] or
[GraphQL API][ref-graphql-api]. Also, the [JavaScript SDK][ref-js-sdk] will
simplify integration with your front-end code. The REST API uses a
[JSON-based query format][ref-json-syntax], and the GraphQL API accepts
[GraphQL queries][ref-graphql-syntax].

## Management APIs

In case you'd like Cube to work with data orchestration tools and let them
push changes from upstream data sources to Cube, explore the
[Orchestration API][ref-orchestration-api].

[cube-issbi]: https://cube.dev/use-cases/semantic-layer
[cube-ea]: https://cube.dev/use-cases/embedded-analytics
[cube-rta]: https://cube.dev/use-cases/real-time-analytics
[ref-sql-api]: /backend/sql
[ref-rest-api]: /http-api/rest
[ref-graphql-api]: /http-api/graphql
[ref-orchestration-api]: /orchestration-api
[ref-sls]: /semantic-layer-sync
[ref-js-sdk]: /frontend-introduction
[ref-sql-syntax]: /backend/sql#querying-fundamentals
[ref-json-syntax]: http://localhost:8000/query-format
[ref-graphql-syntax]: /http-api/graphql#getting-started