---
title: Getting Started
permalink: /schema/getting-started
category: Reference
menuOrder: 1
proofread: 06/18/2019
redirect_from:
  - /getting-started-cubejs-schema
---

A Cube.js Data Schema is used to model raw data into meaningful business
definitions and pre-aggregate data for optimal results. The data schema is
exposed through the [querying API][ref-backend-restapi] that allows end-users to
query a wide variety of analytical queries without modifying the schema itself.

Let’s use a users table with the following columns as an example:

| id  | paying | city          | company_name |
| --- | ------ | ------------- | ------------ |
| 1   | true   | San Francisco | Pied Piper   |
| 2   | true   | Palo Alto     | Raviga       |
| 3   | true   | Redwood       | Aviato       |
| 4   | false  | Mountain View | Bream-Hall   |
| 5   | false  | Santa Cruz    | Hooli        |

We can start with a set of simple questions about users we want to answer:

- How many users do we have?
- How many paying users?
- What is the percentage of paying users out of the total?
- How many users, paying or not, are from different cities and companies?

We don’t need to write SQL queries for every question, since the data schema
allows building well-organized and reusable SQL.

## 1. Creating a Cube

In Cube.js, [cubes][ref-schema-cube] are used to organize entities and
connections between entities. Usually one cube is created for each table in the
database, such as `users`, `orders`, `products`, etc. In the `sql` parameter of
the cube we define a base table for this cube. In our case, the base table is
simply our `users` table.

```javascript
cube(`Users`, {
  sql: `SELECT * FROM users`,
});
```

## 2. Adding Measures and Dimensions

Once the base table is defined, the next step is to add
[measures][ref-schema-measures] and [dimensions][ref-schema-dimensions] to the
cube.

<!-- prettier-ignore-start -->
[[info |]]
| **Measures** are referred to as quantitative data, such as number of units
| sold, number of unique visits, profit, and so on.
|
| **Dimensions** are referred to as categorical data, such as state, gender,
| product name, or units of time (e.g., day, week, month).
<!-- prettier-ignore-end -->

Let's go ahead and create our first measure and two dimensions:

```javascript
cube(`Users`, {
  sql: `SELECT * FROM users`,

  measures: {
    count: {
      sql: `id`,
      type: `count`,
    },
  },

  dimensions: {
    city: {
      sql: `city`,
      type: `string`,
    },

    companyName: {
      sql: `company_name`,
      type: `string`,
    },
  },
});
```

Let's break down the above code snippet piece-by-piece. After defining the base
table for the cube (with the `sql` property), we create a `count` measure in the
`measures` block. [The type][ref-schema-types-formats] `count` and sql `id`
means that when this measure will be requested via an API, Cube.js will generate
and execute the following SQL:

```sql
SELECT count(id) from users;
```

When we apply a city dimension to the measure to see "Where are users based?",
Cube.js will generate SQL with a `GROUP BY` clause:

```sql
SELECT city, count(id) from users GROUP BY 1;
```

You can add as many dimensions as you want to your query when you perform
grouping.

## 3. Adding Filters to Measures

Now let's answer the next question – "How many paying users do we have?". To
accomplish this, we will introduce **measure filters**:

```javascript
cube(`Users`, {
  measures: {
    count: {
      sql: `id`,
      type: `count`,
    },

    payingCount: {
      sql: `id`,
      type: `count`,
      filters: [{ sql: `${CUBE}.paying = 'true'` }],
    },
  },
});
```

<!-- prettier-ignore-start -->
[[info |]]
| It is best practice to prefix references to table columns with the name of
| the cube or with the `CUBE` constant when referencing the current cube's
| column.
<!-- prettier-ignore-end -->

That's it! Now we have the `payingCount` measure, which shows only our paying
users. When this measure is requested, Cube.js will generate the following SQL:

```sql
SELECT
  count(
    CASE WHEN (users.paying = 'true') THEN users.id END
  ) "users.paying_count"
FROM users
```

Since the `filters` property is an array, you can apply as many filters as
required. `payingCount` can be used with dimensions the same way as a simple
`count`. We can group `payingCount` by `city` and `companyName` simply by adding
these dimensions alongside measures in the requested query.

## 4. Using Calculated Measures

To answer "What is the percentage of paying users out of the total?", we need to
calculate the paying users ratio, which is basically `payingCount / count`.
Cube.js makes it extremely easy to perform this kind of calculation. Let's add a
new measure to our cube called `payingPercentage`:

```javascript
cube(`Users`, {
  measures: {
    count: {
      sql: `id`,
      type: `count`,
    },

    payingCount: {
      sql: `id`,
      type: `count`,
      filters: [{ sql: `${CUBE}.paying = 'true'` }],
    },

    payingPercentage: {
      sql: `100.0 * ${payingCount} / ${count}`,
      type: `number`,
      format: `percent`,
    },
  },
});
```

Here we defined a calculated measure `payingPercentage`, which divides
`payingCount` by `count`. This example shows how you can reference measures
inside other measure definitions. When you request the `payingPercentage`
measure via an API, the following SQL will be generated:

```sql
SELECT
  100.0 * count(
    CASE WHEN (users.paying = 'true') THEN users.id END
  ) / count(users.id) "users.paying_percentage"
FROM users
```

As with other measures, `payingPercentage` can be used with dimensions.

## 5. Next Steps

1. [Examples][ref-examples]
2. [Query format][ref-backend-query-format]
3. [REST API][ref-backend-restapi]
4. [Schema reference documentation][ref-schema-cube]

[ref-backend-restapi]: /rest-api
[ref-schema-cube]: /schema/reference/cube
[ref-schema-measures]: /schema/reference/measures
[ref-schema-dimensions]: /schema/reference/dimensions
[ref-schema-types-formats]: /schema/reference/types-and-formats
[ref-examples]: /examples
[ref-backend-query-format]: /query-format
