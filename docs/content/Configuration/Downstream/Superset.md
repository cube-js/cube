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

<LoomVideo url="https://www.loom.com/embed/3e85b7fe3fef4c7bbb8b255ad3f2c675" />

## Enable Cube SQL API

<InfoBox>

Don't have a Cube project yet? [Learn how to get started
here][ref-getting-started].

</InfoBox>

### Cube Cloud

Click **How to connect** link on the Overview page, navigate to the SQL API tab
and enable it. Once enabled, you should see the screen like the one below with
your connection credentials:

<div style="text-align: center">
  <img
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/cube-sql-api-modal.png"
    style="border: none"
    width="80%"
  />
</div>

### Self-hosted Cube

You need to set the following environment variables to enable the Cube SQL API.
These credentials will be required to connect to Cube from Apache Superset
later.

```dotenv
CUBEJS_SQL_PORT=3306
CUBE_SQL_USERNAME=myusername
CUBE_SQL_PASSWORD=mypassword
```

## Connecting from Superset

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

Pick MySQL from the modal:

<div style="text-align: center">
  <img
    alt="Apache Superset: add new database modal"
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Configuration/Downstream/apache-superset-2.png"
    style="border: none"
    width="100%"
  />
</div>

Now enter the Cube SQL API credentials from earlier:

<div style="text-align: center">
  <img
    alt="Apache Superset: add database credentials"
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Configuration/Downstream/apache-superset-3.png"
    style="border: none"
    width="100%"
  />
</div>

Click 'Connect' and then 'Finish'. Now, we can create a dataset in Superset and
explore it. The Cube SQL API exposes cubes as tables where both measures and
dimensions are columns.

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

## Creating Charts

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

When querying measures in Cube, there is no need to apply aggregate functions to
them; for example, the following would be a valid query to Cube SQL API:

```sql
SELECT count FROM Orders;
```

But because many BI tools generate metrics with aggregate functions, Cube knows
how to re-write aggregates to measures. The `COUNT(*)` metric is automatically
generated for every new dataset in Superset. Cube will replace `COUNT(*)` with
measure of type count in the selected cube:

```sql
--- For our Orders cube
--- this query
SELECT COUNT(*) FROM Orders;

--- is similiar to this
SELECT count FROM Orders;

--- because Cube replaces COUNT(*) with measure of type count in the given cube
```

You can customize the list of metrics for a dataset in Settings and map cube
measures to metrics.

<div style="text-align: center">
  <img
    alt="Apache Superset: SQL Editor page with successful query"
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Configuration/Downstream/apache-superset-6.png"
    style="border: none"
    width="100%"
  />
</div>

[ref-getting-started]: /cloud/getting-started
[superset]: https://superset.apache.org/
[ref-cube-getting-started-docker]: https://cube.dev/docs/getting-started/docker
[superset-docs-installation-docker]:
  https://superset.apache.org/docs/installation/installing-superset-using-docker-compose
