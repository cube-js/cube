---
order: 2
title: "How to Create an Analytical API"
---

Pivot tables are useless without the data, and the API is where the data comes from in a real-world app. And the more data we have, the better it is.

So, what are we going to do? We'll use Cube.js.

[Cube.js](https://cube.dev?utm_source=dev-to&utm_medium=post&utm_campaign=react-pivot-table) is an open-source analytical API platform. It allows you to create an API over any database and use that API in any front-end app. In this tutorial, we'll connect Cube.js to a database and we'll use the API in our React app.

Cube.js provides an abstraction called a "semantic layer," or a "data schema," which encapsulates database-specific things, generates SQL queries for you, and lets you use high-level, domain-specific identifiers to work with data.

Also, Cube.js has a built-in caching layer that provides predictable, low-latency response query times. It means that, regardless of your data volume and database, an API built with Cube.js will serve data to your app in a performant way and help create a great user experience.

Let's try it in action.

**The first step is to create a new Cube.js project.** Here, I assume that you already have [Node.js](https://nodejs.org/en/) installed on your machine. Note that you can also [use Docker](https://cube.dev/docs/getting-started-docker?utm_source=dev-to&utm_medium=post&utm_campaign=react-pivot-table) to run Cube.js. Run in your console:

```bash
npx cubejs-cli create react-pivot-table -d postgres
```

Now you have your new Cube.js project in the `react-pivot-table` folder containing a few files. Let's navigate to this folder.

**The second step is to add database credentials to the `.env` file.** Cube.js will pick up its configuration options from this file. Let's put the credentials from a publicly available Postgres database there. Make sure your `.env` file looks like this, or specify your own credentials:

```ini
# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables

CUBEJS_DB_TYPE=postgres
CUBEJS_DB_HOST=demo-db.cube.dev
CUBEJS_DB_PORT=5432
CUBEJS_DB_SSL=true
CUBEJS_DB_USER=cube
CUBEJS_DB_PASS=12345
CUBEJS_DB_NAME=ecom

CUBEJS_DEV_MODE=true
CUBEJS_WEB_SOCKETS=true
CUBEJS_API_SECRET=SECRET
```

Here's what all these options mean:
* Obviously, `CUBEJS_DB_TYPE` says we'll be connecting to Postgres.
* `CUBEJS_DB_HOST` and `CUBEJS_DB_PORT` specify where our Postgres instance is running, and `CUBEJS_DB_SSL` turns on secure communications over TLS.
* `CUBEJS_DB_USER` and `CUBEJS_DB_PASS` are used to authenticate the user to Postgres.
* `CUBEJS_DB_NAME` is the database name where all data schemas and data tables are kept together.
* The rest of the options configure Cube.js and have nothing to do with the database.

**The third step is to start Cube.js.** Run in your console:

```bash
npm run dev
```

And that's it! Here's what you should see:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/txdf4jgqudcm5cv1t30s.png)

Great, the API is up and running. Let's move on! 🔀