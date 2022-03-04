---
title: Introduction
permalink: /introduction
category: Cube.js Introduction
redirect_from:
  - /cubejs-introduction
---

**Cube is the headless business intelligence platform.** It helps data engineers and application developers access data from modern data stores, organize it into consistent definitions, and deliver it to every application.

Cube.js was designed to work with data warehouses and query engines like Google BigQuery and AWS Athena. A multi-stage querying approach makes it suitable for handling trillions of data points. Most modern RDBMS work with Cube.js as well and can be further tuned for performance.

With Cube.js, you can create a semantic API layer on top of your data,
manage access control, cache, and aggregate data. Since Cube.js is visualization agnostic,
you can use any frontend library to build your own custom UI.

## Why Cube?

If you are building a data application—such as a business intelligence tool or a customer-facing analytics feature—you’ll probably face the following problems:

1. __SQL code organization.__ Sooner or later, modeling even a dozen metrics with a dozen dimensions using pure SQL queries becomes a maintenance nightmare, which leads to building a modelling framework.
2. __Performance.__ Most of the time and effort in modern analytics software development is spent providing adequate time to insight. In a world where every company’s data is big data, writing just SQL queries to get insight isn’t enough anymore.
3. __Access Control.__ It is important to secure and govern access to data for all downstream data consuming applications.

Cube has the necessary infrastructure and features to implement efficient data modeling, access control, and performance optimizations so that every application can access consistent data via REST, SQL, and GraphQL APIs. Achieve insights from raw data within minutes, and get an API with sub-second response times on up to a trillion data points.

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
and dimensions) into SQL, and managing caching, queuing and database connection.

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
