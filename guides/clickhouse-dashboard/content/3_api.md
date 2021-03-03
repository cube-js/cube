---
order: 3
title: "How to Run an Analytical API"
---

Let's go step by step and figure out how we can work with ClickHouse in our own application of any kind.

**How to send queries to ClickHouse?** It provides two [interfaces](https://clickhouse.tech/docs/en/interfaces/), HTTP and Native TCP. However, rarely you want to work with low-level raw HTTP or binary TCP data, right?

**Are there any client libraries?** ClickHouse has a few officially supported [drivers](https://clickhouse.tech/docs/en/interfaces/) (e.g., for C++) and a variety of [libraries](https://clickhouse.tech/docs/en/interfaces/third-party/client-libraries/) for different languages. You can use them to send SQL queries and get the results.

**Is it possible to stay high-level, and even not bother to write and maintain SQL queries?** Sure. Here's when [Cube.js](https://cube.dev) comes to the stage. Cube.js is an open-source analytical API platform, and it allows you to create an API over any database, ClickHouse included. You can use Cube.js to take your high-level domain-specific queries (similar to "I want to know `average salary` for every `position`" or "Show me `count of purchases` for every `product category`"), efficiently execute them against your database (casually getting predictable, low-latency performance), and get the result which can be easily visualized, e.g., plotted on a dashboard. And you also get Cube.js Developer Playground, a visual tool which helps to build queries and put them on charts with ease. Let's try it.

**The first step is to create a new Cube.js project.** Here I assume that you already have [Node.js](https://nodejs.org/en/) installed on your machine. Note that you can also [use Docker](https://cube.dev/docs/getting-started-docker) to run Cube.js. Run in your console:

```bash
npx cubejs-cli create clickhouse-dashboard -d clickhouse
```

Now you have your new Cube.js project in the `clickhouse-dashboard` folder which contains a few files. Let's navigate to this folder.

**The second step is to add ClickHouse credentials to the `.env` file.** Cube.js will pick up its configuration options from this file. Let's put the credentials from ClickHouse Playground there. Make sure your `.env` file looks like this, or specify your own credentials:

```
# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables

CUBEJS_DB_TYPE=clickhouse
CUBEJS_DB_HOST=play-api.clickhouse.tech
CUBEJS_DB_PORT=8443
CUBEJS_DB_SSL=true
CUBEJS_DB_USER=playground
CUBEJS_DB_PASS=clickhouse
CUBEJS_DB_NAME=datasets
CUBEJS_DB_CLICKHOUSE_READONLY=true

CUBEJS_DEV_MODE=true
CUBEJS_WEB_SOCKETS=true
CUBEJS_API_SECRET=SECRET
```

Here's what all these options mean:
* Obviously, `CUBEJS_DB_TYPE` says we'll be connecting to ClickHouse.
* `CUBEJS_DB_HOST` and `CUBEJS_DB_PORT` specify where our ClickHouse instance is running, and `CUBEJS_DB_SSL` turns on secure communications over TLS.
* `CUBEJS_DB_USER` and `CUBEJS_DB_PASS` are used to authenticate the user to ClickHouse.
* `CUBEJS_DB_NAME` is the database (or "schema") name where all data tables are kept together.
* `CUBEJS_DB_CLICKHOUSE_READONLY` is an option that we need to provide specifically because we're connecting to ClickHouse Playground because it allows only read-only access. Usually you won't need to specify such an option for ClickHouse.
* The rest of options configure Cube.js and have nothing to do with ClickHouse.

**The third step is to start Cube.js.** Run in your console:

```bash
npm run dev
```

And that's it! Here's what you should see:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/yjneoih6qbkp5kmhyayf.png)

We've reached the cruising speed, so enjoy your flight! ✈️