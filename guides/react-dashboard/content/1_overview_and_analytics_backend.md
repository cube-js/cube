---
title: "Overview and Analytics API"
order: 1
---

Nowadays, we see analytics dashboards and reporting features in almost any application. In my career as a web developer, I’ve built dozens of different dashboards from internal tools to measure application performance to customer-facing portals with interactive report builders and dynamic dashboards.

And I cannot say I always enjoyed the process. Several years ago I was rendering all the HTML, including dashboards and charts, on the server and then was trying to make it dynamic with some jQuery and a lot of hacks. Backends were huge monolith applications, doing a ton of things, including analytics processing, which often ends up to be slow, inefficient, and hard to maintain. Thanks to microservices, containers, frontend frameworks, and a lot of great charting libraries it is easier and definitely more fun to build such analytics dashboards and report builders today.

In this React Dashboard tutorial, we’ll learn step by step how to build a full-stack analytics application, including a report builder and a dynamic dashboard. We’ll build our application in a microservice architecture with the frontend decoupled from the backend. We’ll rely on AWS services for some of the functionality, but that could be easily substituted by your own microservices, which we cover later in the tutorial.

You can check out the [final application we are going to build here.](https://react-dashboard-demo.cube.dev/) The diagram below shows the architecture of our app.

![](/images/1-schema.png)

Let’s go through the backend first -

We're going to store our data for the dashboard in [PostgreSQL](https://www.postgresql.org/), a free and open-source relational database. For those who don’t have Postgres or would like to use a different database, I’ll put some useful links on how to do the same setup for different databases, such as MongoDB, later in this tutorial.

Next, we’ll install [Cube.js](https://github.com/cube-js/cube.js) and connect it to the database. Cube.js is an open-source analytical API platform for building analytical applications. It creates an analytics API on top of the database and handles things like SQL generation, caching, security, authentication, and much more.

We’ll also use [AWS Cognito](https://aws.amazon.com/cognito/) for user registrations and sign-ins and [AWS AppSync](https://aws.amazon.com/appsync/) as a GraphQL backend. Optionally, you can use your own authentication service, as well as GraphQL backend. But to keep things simple, we’ll rely on AWS services for the purpose of this tutorial.

The frontend is a React application. We’re going to use Cube.js Playground to generate a React dashboard boilerplate with a report builder and a dashboard. It uses [Create React App](https://create-react-app.dev/) under the hood to create all the configuration and additionally wires together all the components to work with Cube.js API and a GraphQL backend. Finally, for the visualizations, we’ll use Recharts, a powerful and customizable React-based charting library.


## Setting up a Database and Cube.js

The first thing we need to have in place is a database. We’ll use a [PostgreSQL](https://www.postgresql.org/) database, but any other relational database should work fine as well. If you want to use MongoDB, you’d need to add a MongoDB Connector for BI. It allows you to execute SQL code on top of your MongoDB data. It can be easily downloaded from the MongoDB website.

One more thing to keep in mind is a replication. It is considered a bad practice to run analytics queries against your production database mostly because of the performance issues. Cube.js can dramatically reduce the amount of a database’s workload, but still, I’d recommend connecting to the replica.

If you don’t have any data for the dashboard, you can download our sample e-commerce Postgres dataset.

```
$ curl -L http://cube.dev/downloads/ecom-dump.sql > ecom-dump.sql
$ createdb ecom
$ psql --dbname ecom -f ecom-dump.sql
```

Now, let’s create an analytical API with Cube.js. Run the following command in your terminal:

```bash
$ npx cubejs-cli create react-dashboard -d postgres
```

We’ve just created a new Cube.js service, configured to work with the Postgres database. Cube.js uses environment variables starting with `CUBEJS_` for configuration. To configure the connection to our database, we need to specify the DB type and name. In the Cube.js project folder replace the contents of `.env` with the following:

```bash
CUBEJS_DB_TYPE=postgres
CUBEJS_DB_NAME=ecom
CUBEJS_API_SECRET=SECRET
CUBEJS_DEV_MODE=true
```

If you are using a different database, please refer to [this documentation](https://cube.dev/docs/connecting-to-the-database) on how to connect to a database of your choice.

Now, let’s run Cube.js Playground. It will help us to build a simple data schema, test out the charts, and then generate a React dashboard boilerplate. Run the following command in the Cube.js project folder:

```bash
$ npm run dev
```

Next, open http://localhost:4000 in your browser to create a Cube.js data schema.

Cube.js uses the data schema to generate an SQL code, which will be executed in your database. The data schema is not a replacement for SQL. It is designed to make SQL reusable and give it a structure while preserving all of its power. Basic elements of the data schema are **measures** and **dimensions**.

**Measure** is referred to as quantitative data, such as the number of units sold, number of unique visits, profit, and so on.

**Dimension** is referred to as categorical data, such as state, gender, product name, or units of time (e.g., day, week, month).

Data schema is a JavaScript code, which defines measures and dimensions and how they map to SQL queries. Here is an example of the schema, which can be used to describe users’ data.

```javascript
cube(`Users`, {
  sql: `SELECT * FROM users`,

  measures: {
    count: {
      sql: `id`,
      type: `count`
    }
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
    }
  }
});
```

Cube.js can generate a simple data schema based on the database’s tables. Let’s select the `orders`, `line_items`, `products`, and `product_categories` tables and click “Generate Schema.” It will generate 4 schema files, one per table.

![](/images/1-screenshot-1.png)

Once the schema is generated, we can navigate to the “Build” tab and select some measures and dimensions to test out the schema. The "Build" tab is a place where you can build sample charts with different visualization libraries and inspect how that chart was created, starting from the generated SQL all the way up to the JavaScript code to render the chart. You can also inspect the JSON query, which is sent to Cube.js backend.

![](/images/1-screenshot-3.png)

## Generating a Dashboard Template

The next step is to generate a template of our frontend application. Navigate to “Dashboard App,” select React and Recharts, and click on the “Create dashboard app” button.

![](/images/1-screenshot-2.png)

It could take a while to generate an app and install all the dependencies. Once it is done, you will have a `dashboard-app` folder inside your Cube.js project folder. To start a frontend application, either go to the “Dashboard App” tab in the playground and hit the “Start” button, or run the following command inside the `dashboard-app` folder:

```bash
$ npm start
```

Make sure the Cube.js backend process is up and running since our frontend application uses its API. The frontend application is running on http://localhost:3000. If you open it in your browser, you should be able to see an Explore tab with a query builder and an empty Dashboard tab. Feel free to play around to create some charts and save them to the dashboard.

`video: /videos/1-video.mp4`

Our generated application uses the Apollo GraphQL client to store dashboard items into local storage. In the next part, we will add persistent storage with AWS AppSync, as well as user authentication with AWS Cognito.
