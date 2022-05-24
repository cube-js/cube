---
title: Connecting from Power BI
permalink: /config/downstream/powerbi
---

<WarningBox heading={`Power BI support is in preview`}>

Power BI support is in preview, not all features and requests may work on this
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

## Connecting from Power BI

Power BI connects to Cube as to a Postgres database.

## Querying data

Your cubes will be exposed as tables, where both your measures and dimensions are columns.


[ref-getting-started]: /cloud/getting-started
[ref-sql-api]: /backend/sql
