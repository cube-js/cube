---
order: 2
title: "Setting up a Database and Cube.js"
---

The first thing we need to have in place is a database. We’ll use Postgres for this tutorial. However, you can use your favorite SQL (or Mongo) database. Please refer to the [Cube.js documentation on how to connect to different databases](https://cube.dev/docs/connecting-to-the-database).

If you don’t have any data for the dashboard, you can load our sample e-commerce Postgres dataset.

```bash
$ curl http://cube.dev/downloads/ecom-dump-d3-example.sql > ecom-dump.sql
$ createdb ecom
$ psql --dbname ecom -f ecom-dump.sql
```

Now, as we have data in the database, we’re ready to create the Cube.js Backend service. Run the following commands in your terminal:

```bash
$ npm install -g cubejs-cli
$ cubejs create d3-dashboard -d postgres
```

The commands above install Cube.js CLI and create a new service, configured to work with a Postgres database.

Cube.js uses environment variables for configuration. It uses environment variables starting with `CUBEJS_`. To configure the connection to our database, we need to specify the DB type and name. In the Cube.js project folder, replace the contents of .env with the following:

```bash
CUBEJS_API_SECRET=SECRET
CUBEJS_DB_TYPE=postgres
CUBEJS_DB_NAME=ecom
CUBEJS_WEB_SOCKETS=true
```

Now let’s start the server and open the developer playground at [http://localhost:4000](http://localhost:4000).

```bash
$ npm run dev
```

The next step is to create a [Cube.js data schema](https://cube.dev/docs/getting-started-cubejs-schema). Cube.js uses the data schema to generate an SQL code, which will be executed in your database. Cube.js Playground can generate simple schemas based on the database’s tables. Let’s navigate to the Schema page and generate the schemas we need for our dashboard. Select the `line_items`, `orders`, `products`, `product_categories`, and `users` tables and click **Generate Schema**.

![](/images/2-screenshot-1.png)

Let’s test our newly generated schema. Go to the Build page and select a measure in the dropdown. You should be able to see a simple line chart. You can choose D3 from the charting library dropdown to see an example of D3 visualization. Note that it is just an example and you can always customize and expand it.

![](/images/2-screenshot-2.png)

Now, let’s make some updates to our schema. The schema generation makes it easy to get started and test the dataset, but for real-world use cases, we almost always need to make manual changes. This is an optional step; feel free to skip to the [next chapter](/rendering-chart-with-d-3-js), where we’ll focus on rendering results with D3.

In the schema, we define measures and dimensions and how they map into SQL queries. You can find extensive documentation about [data schema here](https://cube.dev/docs/getting-started-cubejs-schema). We’re going to add a `priceRange` dimension to the Orders cube. It will indicate whether the total price of the order falls into one of the buckets: “$0 - $100”, “$100 - $200”, “$200+”.

To do this, we first need to define a `price` dimension for the order. In our database, `orders` don’t have a price column, but we can calculate it based on the total price of the `line_items` inside the order. Our schema has already automatically indicated and defined a relationship between the `Orders` and `LineTimes` cubes. You can read more about [joins here](https://cube.dev/docs/joins).

```javascript
// You can check the belongsTo join
// to the Orders cube inside the LineItems cube
joins: {
  Orders: {
    sql: `${CUBE}.order_id = ${Orders}.id`,
    relationship: `belongsTo`
  }
}
```

The `LineItems` cube has `price` measure with a `sum` type. We can reference this measure from the `Orders` cube as a dimension and it will give us the sum of all the line items that belong to that order. It’s called a `subQuery` dimension; you can [learn more about it here](https://cube.dev/docs/subquery).


```javascript
// Add the following dimension to the Orders cube
price: {
  sql: `${LineItems.price}`,
  subQuery: true,
  type: `number`,
  format: `currency`
}
```

Now, based on this dimension, we can create a `priceRange` dimension. We’ll use a [case statement](https://cube.dev/docs/dimensions#parameters-case) to define a conditional logic for our price buckets.

```javascript
// Add the following dimension to the Orders cube
priceRange: {
  type: `string`,
  case: {
    when: [
      { sql: `${price} < 101`, label: `$0 - $100` },
      { sql: `${price} < 201`, label: `$100 - $200` }
    ],
    else: {
      label: `$200+`
    }
  }
}
```

Let’s try our newly created dimension! Go to the Build page in the playground, select the Orders count measure with the Orders price range dimension. You can always check the generated SQL by clicking the **SQL** button on the control bar.

That’s it for the backend part. In the next chapter, we’ll look closer at how to render the results of our queries with D3.
