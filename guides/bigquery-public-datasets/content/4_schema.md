---
order: 4
title: "How to Define a Data Schema"
---

Before we can explore the data, we need to describe it with a data schema. The [data schema](https://cube.dev/docs/getting-started-cubejs-schema) is a high-level domain-specific description of your data. It allows you to skip writing SQL queries and rely on Cube.js query generation engine.

Create two schema files with the following contents: take `schema/Measures.js` from [this file](https://github.com/cube-js/cube.js/blob/master/examples/bigquery-public-datasets/schema/Measures.js), and `schema/Mobility.js` from [that file](https://github.com/cube-js/cube.js/blob/master/examples/bigquery-public-datasets/schema/Mobility.js). Here is a redacted version of the first file with a few interesting things:

```js
cube(`Measures`, {
  sql: '
    SELECT *
    FROM `bigquery-public-data.covid19_govt_response.oxford_policy_tracker`
  ',

  measures: {
    confirmed_cases: {
      sql: `confirmed_cases`,
      type: `max`
    },

    cancelPublicEvents: {
      sql: `CAST(cancel_public_events AS NUMERIC)`,
      type: `max`
    },
  },

  dimensions: {
    country: {
      sql: `country_name`,
      type: `string`
    },

    date: {
      sql: `TIMESTAMP(${Measures}.date)`,
      type: `time`
    },
  },
});
```

Note that:
* in this data schema, you describe an analytical `cube`
* it contains the data retrieved via a straightforward `sql` query
* you can define `measures`, i.e., numerical values to be calculated
* measures are calculated using various functions, such as `max`
* you can define `dimensions`, i.e., attributes for which the measures are calculated
* dimensions can have different data types, such as `string` or `time`
* in measure and dimension definitions, you can use BigQuery functions, e.g., `CAST(... AS NUMERIC)` or `TIMESTAMP`

And here's a part of another file:

```js
cube(`Mobility`, {
  sql: '
    SELECT *
    FROM `bigquery-public-data.covid19_google_mobility.mobility_report`
  ',

  measures: {

  },

  dimensions: {

  },

  joins: {
    Measures: {
      sql: `${Measures}.country_name = ${Mobility}.country_region AND
            ${Measures}.date = ${Mobility}.date`,
      relationship: `hasOne`,
    }
  }
});
```

Here you can see that our two cubes, based on different tables from different BigQuery datasets, are joined together with `join`, where a join condition is provided as an SQL statement. Cube.js takes care of the rest.

Now we have the data schema in place, and we can explore the data! 🦠