---
title: Connecting to Hex
permalink: /config/downstream/hex
---

<InfoBox>

The SQL API and Extended Support for BI Tools workshop is on June 22nd at 9-10:30 am PT! You'll have the opportunity to learn the latest on Cube's [SQL API](https://cube.dev/blog/expanded-bi-support/). 

You can register for the workshop at [the event page](https://cube.dev/events/sql-api). 👈

</InfoBox>

You can connect to Cube from Hex, a collaborative data platform, using the [Cube SQL
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
## Connecting from Hex

Hex connects to Cube as to a Postgres database.

## Querying data

Your cubes will be exposed as tables, where both your measures and dimensions are columns.

You can write SQL in Hex that will be executed in Cube. Learn more about Cube SQL
syntax on the [reference page][ref-sql-api].

<div style="text-align: center">
  <img
    src="https://descriptive-reply-0b7.notion.site/image/https%3A%2F%2Fs3-us-west-2.amazonaws.com%2Fsecure.notion-static.com%2F225f79fc-5150-47ce-9398-c54c97e7e143%2FUntitled.png?table=block&id=5a4515cf-8aa9-4885-88b5-42e0c54358b4&spaceId=73430d0e-c482-40f7-946b-8a7851a88586&width=2000&userId=&cache=v2"
    style="border: none"
    width="80%"
  />
</div>

[ref-getting-started]: /cloud/getting-started
[ref-sql-api]: /backend/sql
