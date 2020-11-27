---
order: 2
title: "Analytics API with Cube.js"
---

We're going to build the dashboard for an e-commerce company that wants to track its overall performance and orders' statuses. Let's assume that the company keeps its data in an SQL database. So, in order to display that data on a dashboard, we're going to create an analytical API.

For that, we'll use the Cube.js command-line utility (CLI).

Cube.js supports [all popular databases](https://cube.dev/docs/getting-started#2-connect-to-your-database), and the API will be pre-configured to work with a particular database type. We’ll use a [PostgreSQL](https://www.postgresql.org/) database. Please make sure you have PostgreSQL installed.

**To create the API, we run this command:**

```bash
$ npx cubejs-cli create material-ui-dashboard -d postgres
```

Now we can download and import a sample e-commerce dataset for PostgreSQL:

```
$ curl http://cube.dev/downloads/ecom-dump.sql > ecom-dump.sql
$ createdb ecom
$ psql --dbname ecom -f ecom-dump.sql
```

Once the database is ready, the API can be [configured to connect to the database](https://cube.dev/docs/connecting-to-the-database#configuring-connection-for-cube-js-cli-created-apps). To do so, we provide a few options via the `.env` file in the root of the Cube.js project folder (`material-ui-dashboard`):

```
CUBEJS_DB_NAME=ecom
CUBEJS_DB_TYPE=postgres
CUBEJS_API_SECRET=secret
CUBEJS_DEV_MODE=true
```

![](/images/start.gif)

Now we can run the API!

**In development mode, the API will also run the Cube.js Playground.** It's a time-saving web application that helps to create a data schema, test out the charts, and generate a React dashboard boilerplate. Run the following command in the Cube.js project folder:

`$ node index.js`

Next, open [http://localhost:4000](http://localhost:4000/) in your browser.

![](/images/demo.gif)

**We'll use the Cube.js Playground to create a data schema.** It's essentially a JavaScript code that declaratively describes the data, defines analytical entities like measures and dimensions, and maps them to SQL queries. Here is an example of the schema which can be used to describe users’ data.

```jsx
cube(`Users`, {
  sql: `SELECT * FROM users`,

  measures: {
    count: {
      sql: `id`,
      type: `count`
    },
  },

  dimensions: {
    city: {
      sql: `city`,
      type: `string`
    },

    signedUp: {
      sql: `created_at`,
      type: `time`
    },

    companyName: {
      sql: `company_name`,
      type: `string`
    },
  },
});
```

Cube.js can generate a simple data schema based on the database’s tables. If you already have a non-trivial set of tables in your database, consider using the data schema generation because it can save time.

For our API, we select the `line_items`, `orders`, `products`, and `users` tables and click “Generate Schema.” As the result, we'll have 4 generated files in the `schema` folder—one schema file per table.

![](/images/image-37.png)

**Once the schema is generated, we can build sample charts via web UI.** To do so, navigate to the “Build” tab and select some measures and dimensions from the schema.

The "Build" tab is a place where you can build sample charts using different visualization libraries and inspect every aspect of how that chart was created, starting from the generated SQL all the way up to the JavaScript code to render the chart. You can also inspect the Cube.js query encoded with JSON which is sent to Cube.js API.

![](/images/image-05.png)