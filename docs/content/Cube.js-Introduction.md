---
title: Introduction
permalink: /introduction
category: Cube.js Introduction
redirect_from:
  - /cubejs-introduction
---

**Cube.js is an open-source analytical API platform.** It is primarily used to build internal business intelligence tools or add customer-facing analytics to existing applications.

Cube.js was designed to work with serverless data warehouses and query engines like Google BigQuery and AWS Athena. A multi-stage querying approach makes it suitable for handling
trillions of data points. Most modern RDBMS work with Cube.js as well and can be
further tuned for performance.

With Cube.js, you can create a semantic API layer on top of your data,
manage access control, cache, and aggregate data. Since Cube.js is visualization agnostic,
you can use any frontend library to build your own custom UI.

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

**Cube.js acts as an analytics backend**, translating business logic (metrics
and dimensions) into SQL and managing caching, queuing and database connection.

The Cube.js JavaScript client sends queries conforming to the [Query
Format][ref-query-format] to the REST API. The server uses a Schema to generate
an SQL query, which is executed by your chosen database. The server handles all
database connections, as well as pre-aggregations and caching layers. The result
is then sent back to the client. The client itself is visualization-agnostic and
works well with any chart library.

<div
  style="text-align: center"
>
  <img
  src="https://i.imgur.com/FluGFqo.png"
  style="border: none"
  width="100%"
  />
</div>

[ref-query-format]: https://cube.dev/docs/query-format
