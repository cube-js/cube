---
title: Introduction
permalink: /introduction
category: Cube.js Introduction
redirect_from:
  - /cubejs-introduction
---

**Cube.js is an open-source modular framework to build analytical web
applications**. It is primarily used to build internal business intelligence
tools or to add customer-facing analytics to an existing application.

Cube.js was designed to work with Serverless Query Engines like AWS Athena and
Google BigQuery. A multi-stage querying approach makes it suitable for handling
trillions of data points. Most modern RDBMS work with Cube.js as well and can be
further tuned for performance.

Cube.js provides modules to run transformations and modeling in data warehouses,
along with querying and caching, managing API gateway and building UI.

### Cube.js Backend

- **Schema.** Acts as an ORM for analytics and allows modelling everything from
  simple counts to cohort retention and funnel analysis
- **Query Orchestration and Cache.** Optimizes query execution by breaking
  queries into small, fast, reusable and materialized pieces
- **API Gateway.** Provides an idempotent long polling API as well as a
  WebSockets API which guarantees delivery of analytical query results without
  request timeframe limitations and tolerance to connectivity issues

### Cube.js Frontend

- **Javascript Client.** Core SDK for accessing the Cube.js API Gateway and
  functionality for working with query result sets
- **React, Angular and Vue.** Framework-specific wrappers for Cube.js API

## Why Cube.js?

If you are building your own business intelligence tool or customer-facing
analytics, it is quite likely you'll face one or more of the following problems:

1. **Performance.** A significant amount of effort in modern analytics software
   development is spent providing adequate time to insight. In a world where
   every company's data is big data, just writing SQL queries for insights isn't
   enough anymore
2. **SQL code organization.** Modelling even a modest number of metrics and
   dimensions using pure SQL queries eventually becomes a maintenance nightmare,
   which then requires engineering effort in building a modelling framework
3. **Infrastructure.** Any production-ready analytics solution requires key
   components such as analytic SQL generation, query results caching and
   execution orchestration, data pre-aggregation, security, a querying API and
   support for visualization libraries

Cube.js has the necessary infrastructure for any analytics application that
heavily relies on a caching and pre-aggregation layer to provide insights from
raw data within minutes and an API with sub-second response times on up to a
trillion data points.

<img src="https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/content/old-was-vs-cubejs-way.png" style="border: none" />

## Architecture

**Cube.js acts as an analytics backend**, translating business logic (metrics
and dimensions) into SQL and managing caching, queuing and database connection.

The Cube.js JavaScript client sends queries conforming to the [Query
Format][ref-query-format] to the REST API. The server uses a Schema to generate
an SQL query, which is executed by your chosen database. The server handles all
database connections, as well as pre-aggregations and caching layers. The result
is then sent back to the client. The client itself is visualization-agnostic and
works well with any chart library.

<p align="center"><img src="https://i.imgur.com/FluGFqo.png" alt="Cube.js" width="100%"></p>

[ref-query-format]: https://cube.dev/docs/query-format
