---
title: String Time Dimensions
permalink: /working-with-string-time-dimensions
scope: cubejs
category: Guides
subCategory: Tutorials
menuOrder: 19
---
Cube.js always expects `timestamp with timezone` or compatible type as an input to time dimension.
There're a lot of cases when in your underlying fact table datetime information is stored as a string.
Hopefully most of SQL backends support datetime parsing which allows you to convert strings into timestamps.

Let's consider an example for BigQuery:

```javascript
cube(`Events`, {
  sql: `SELECT * FROM schema.events`,

  // ...

  dimensions: {
    date: {
      sql: `PARSE_TIMESTAMP('%Y-%m-%d', date)`,
      type: `time`
    }
  }
});
```

In this particular cube `date` column will be parsed using `%Y-%m-%d` format.
Please note that as we do not pass timezone parameter to `PARSE_TIMESTAMP` it'll set default `UTC` timezone.
You should always set timezone appropriately for parsed timestamp as Statsbot always do timezone conversions according to user settings.

Although query performance of big data backends like BigQuery or Presto won't likely suffer from date parsing, performance of RDBMS backends like Postgres most likely will.
Adding timestamp columns with indexes should be considered in this case.
