---
order: 1
title: "What is ClickHouse?"
---

[ClickHouse](https://clickhouse.tech) is a fast open-source column-oriented analytical database. Unlike transactional databases like Postgres or MySQL, it claims to be able to generate analytical reports using SQL queries in real-time.

While relatively obscure, ClickHouse is [adopted](https://clickhouse.tech/docs/en/introduction/adopters/) and used at Bloomberg, Cloudflare, eBay, Spotify, Uber, and even by nuclear physicists at CERN.

Also, it claims to be blazing fast due to its columnar storage engine. Sounds legit, because it's generally faster to apply analytical operations such as `AVG`, `DISTINCT`, or `MIN` to densely packed values (columns) rather than sparsely kept data.

**In this tutorial we're going to explore how to:**
* start working with ClickHouse,
* build an analytical API on top of it with [Cube.js](https://cube.dev), and
* query this API from a front-end dashboard, so you can
* visualize query results with charts.

Here's what our end result will look like:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/zo3lnouaxh6xodk3xsbe.gif)

**Also, here's the [live demo](https://clickhouse-dashboard-demo.cube.dev) you can use right away.** And yeah, you surely can use it to observe drastic price surges of the stocks that were popular on the [WallStreetBets](https://www.reddit.com/r/wallstreetbets/) subreddit, including GameStop.

We're taking off, so fasten your seatbelts! ✈️