---
title: Connecting from Power BI
permalink: /config/downstream/powerbi
---

<InfoBox>
  <b>The SQL API and Extended Support for BI Tools</b> workshop on June 22, 2022.<br/> 
  You'll have the opportunity to learn the latest on Cube's <a href="https://cube.dev/blog/expanded-bi-support/">SQL API.</a><br /> 
  Check out the agenda and resigter for the workshop today on the <a href="https://cube.dev/events/sql-api">event page</a> 👈
</InfoBox>

<WarningBox heading={`Power BI support is in preview`}>

Power BI support is in preview, not all features and requests may work at this
point.

</WarningBox>

You can connect to Cube from Power BI, interactive data visualization software product developed by Microsof, using the [Cube SQL
API][ref-sql-api].

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
CUBEJS_PG_SQL_PORT=5432
CUBE_SQL_USERNAME=myusername
CUBE_SQL_PASSWORD=mypassword
```

## Connecting from Power BI

Power BI connects to Cube as to a Postgres database.

## Querying data

Your cubes will be exposed as tables, where both your measures and dimensions are columns.


[ref-getting-started]: /cloud/getting-started
[ref-sql-api]: /backend/sql
