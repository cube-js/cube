---
title: Connecting to Superset/Preset
permalink: /config/downstream/superset
redirect_from:
  - /recipes/using-apache-superset-with-cube-sql
---

You can connect a Cube project to [Apache Superset][superset] using the Cube SQL
API. [Apache Superset][superset] is an open-source data exploration and
visualization platform, commonly used to visualize business metrics and
performance.

## Enable Cube SQL API

<InfoBox>

Don't have a Cube project yet? [Learn how to get started
here][ref-getting-started].

</InfoBox>

### Cube Cloud

Click **How to connect your BI tool** link on the Overview page, navigate to the SQL API tab
and enable it. Once enabled, you should see the screen like the one below with
your connection credentials:

<div style="text-align: center">
  <img
    src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/bac4cfb4-d89c-46fa-a7d4-552c2ece4a18.GIF"
    style="border: none"
    width="80%"
  />
</div>

### Self-hosted Cube

You need to set the following environment variables to enable the Cube SQL API.
These credentials will be required to connect to Cube from Apache Superset
later.

```dotenv
CUBEJS_PG_SQL_PORT=5432
CUBE_SQL_USERNAME=myusername
CUBE_SQL_PASSWORD=mypassword
```

## Connecting from Superset

Apache Superset connects to Cube as to a Postgres database.

In Apache Superset, go to Data > Databases, then click '+ Database' to add a new
database:

<div style="text-align: center">
  <img
    alt="Apache Superset: databases page"
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Configuration/Downstream/apache-superset-1.png"
    style="border: none"
    width="100%"
  />
</div>

## Querying data

Your cubes will be exposed as tables, where both your measures and dimensions are columns.

Let's use the following Cube data schema:

```js
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    status: {
      sql: `status`,
      type: `string`,
    },

    created: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
```

Using the SQL API, `Orders` will be exposed as a table. In Superset, we can
create datasets based on tables. Let's create one from `Orders` table:

<div style="text-align: center">
  <img
    alt="Apache Superset: SQL Editor page with successful query"
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Configuration/Downstream/apache-superset-4.png"
    style="border: none"
    width="100%"
  />
</div>

Now, we can explore this dataset. Let's create a new chart of type line with
**Orders** dataset.

<div style="text-align: center">
  <img
    alt="Apache Superset: SQL Editor page with successful query"
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Configuration/Downstream/apache-superset-5.png"
    style="border: none"
    width="100%"
  />
</div>

We can select `COUNT(*)` as metric and `created` as time column with time grain
**month**.

`COUNT(*)` aggregate function is being mapped to measure with type [count](/schema/reference/types-and-formats#measures-types-count) in the cube.

[ref-getting-started]: /cloud/getting-started
[superset]: https://superset.apache.org/
[ref-cube-getting-started-docker]: https://cube.dev/docs/getting-started/docker
[superset-docs-installation-docker]:
  https://superset.apache.org/docs/installation/installing-superset-using-docker-compose
