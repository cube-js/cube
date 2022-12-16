---
title: Introduction
permalink: /introduction
category: Cube.js Introduction
redirect_from:
  - /cubejs-introduction
---

**Cube is the headless business intelligence platform.** It helps data engineers
and application developers access data from modern data stores, organize it into
consistent definitions, and deliver it to every application.

<img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/cube-scheme.png"
  style="border: none"
  width="100%"
/>

Cube was designed to work with all SQL-enabled data sources, including cloud
data warehouses like Snowflake or Google BigQuery, query engines like Presto or
Amazon Athena, and application databases like Postgres. Cube has a built-in
caching engine to provide sub-second latency and high concurrency for API
requests.

With Cube, you can build a data model, manage access control and caching, and
expose your data to every application via REST, GraphQL, and SQL APIs. Cube is
headless, API-first, and decoupled from visualizations. You can use any charting
library to build custom UI, or connect existing dashboarding and reporting tools
to Cube.

## Why Cube?

If you are building a data application—such as a business intelligence tool or a
customer-facing analytics feature—you’ll probably face the following problems:

1. **SQL code organization.** Sooner or later, modeling even a dozen metrics
   with a dozen dimensions using pure SQL queries becomes a maintenance
   nightmare, which leads to building a modeling framework.
2. **Performance.** Most of the time and effort in modern analytics software
   development is spent providing adequate time to insight. In a world where
   every company’s data is big data, writing just SQL queries to get insight
   isn’t enough anymore.
3. **Access Control.** It is important to secure and govern access to data for
   all downstream data consuming applications.

Cube has the necessary infrastructure and features to implement efficient data
modeling, access control, and performance optimizations so that every
application can access consistent data via REST, SQL, and GraphQL APIs. Achieve
insights from raw data within minutes, and get an API with sub-second response
times on up to a trillion data points.

<div
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/old-was-vs-cubejs-way.png"
  style="border: none"
  width="80%"
  />
</div>

## Architecture

**Cube acts as a data access layer**, translating API requests into SQL,
managing caching, queuing, and database connection.

The Cube accepts queries via REST, GraphQL or SQL interfaces. Based on the data
model and an incoming query, Cube generates a SQL query and executes it in your
database. Cube fully manages query orchestration, database connections, as well
as caching and access control layers. The result is then sent back to the
client.

<div
  style="text-align: center"
>
  <img
  src="https://i.imgur.com/FluGFqo.png"
  style="border: none"
  width="100%"
  />
</div>

[ref-query-format]: /backend/rest/reference/backend/rest/reference/query-format
