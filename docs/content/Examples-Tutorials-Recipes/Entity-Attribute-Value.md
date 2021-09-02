---
title: Entity-Attribute-Value Model
permalink: /recipes/entity-attribute-value
category: Examples & Tutorials
subCategory: Data schema
menuOrder: 4
---

## Use case

We want to create a cube for a dataset which uses the
[Entity-Attribute-Value](https://en.wikipedia.org/wiki/Entity–attribute–value_model)
model (EAV). It stores entities in a table that can be joined to another table
with numerous attribute-value pairs. Each entity is not guaranteed to have the
same set of associated attributes, thus making the entity-attribute-value
relation a sparse matrix. In the cube, we'd like every attribute to be modeled
as a dimension.

## Data schema

Let's explore the `Users` cube that contains the entities:

```javascript
cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  joins: {
    Orders: {
      relationship: 'hasMany',
      sql: `${Users}.id = ${Orders}.user_id`,
    },
  },

  dimensions: {
    name: {
      sql: `first_name || ' ' || last_name`,
      type: `string`,
    },
  },
});
```

The `Users` cube is joined with the `Orders` cube to reflect that there might be
many orders associated with a single user. The orders remain in various
statuses, as reflected by the `status` dimension, and their creation dates are
available via the `createdAt` dimension:

```javascript
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  dimensions: {
    status: {
      sql: `status`,
      type: `string`,
    },

    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
```

Currently, the dataset contains orders in the following statuses:

```javascript
[
  {
    'Orders.status': 'completed',
  },
  {
    'Orders.status': 'processing',
  },
  {
    'Orders.status': 'shipped',
  },
];
```

Let's say that we'd like to know, for each user, the earliest creation date for
their orders in any of these statuses. In terms of the EAV model:

- the users serve as _entities_ and they should be modeled with a _cube_
- order statuses serve as _attributes_ and they should be modeled as
  _dimensions_
- the earliest creation dates for each status serve as attribute _values_ and
  they will be modeled as _dimension values_

Let's explore some possible ways to model that.

### Static attributes

We already know that the following statuses are present in the dataset:
`completed`, `processing`, and `shipped`. Let's assume this set of statuses is
not going to change often.

Then, modeling the cube is as simple as defining a few joins (one join per
attribute):

```javascript
cube(`UsersStatuses_Joins`, {
  sql: `
    SELECT
      users.first_name,
      users.last_name,
      MIN(cOrders.created_at) AS cCreatedAt,
      MIN(pOrders.created_at) AS pCreatedAt,
      MIN(sOrders.created_at) AS sCreatedAt
    FROM public.users AS users
    LEFT JOIN public.orders AS cOrders
      ON users.id = cOrders.user_id AND cOrders.status = 'completed'
    LEFT JOIN public.orders AS pOrders
      ON users.id = pOrders.user_id AND pOrders.status = 'processing'
    LEFT JOIN public.orders AS sOrders
      ON users.id = sOrders.user_id AND sOrders.status = 'shipped'
    GROUP BY 1, 2
  `,

  dimensions: {
    name: {
      sql: `first_name || ' ' || last_name`,
      type: `string`,
    },

    completedCreatedAt: {
      sql: `cCreatedAt`,
      type: `time`,
    },

    processingCreatedAt: {
      sql: `pCreatedAt`,
      type: `time`,
    },

    shippedCreatedAt: {
      sql: `sCreatedAt`,
      type: `time`,
    },
  },
});
```

Querying the cube would yield data like this. As we can see, every user has
attributes that show the earliest creation date for their orders in all three
statuses. However, some attributes don't have values (meaning that a user
doesn't have orders in this status).

```javascript
[
  {
    'UsersStatuses_Joins.name': 'Ally Blanda',
    'UsersStatuses_Joins.completedCreatedAt': '2019-03-05T00:00:00.000',
    'UsersStatuses_Joins.processingCreatedAt': null,
    'UsersStatuses_Joins.shippedCreatedAt': '2019-04-06T00:00:00.000',
  },
  {
    'UsersStatuses_Joins.name': 'Cayla Mayert',
    'UsersStatuses_Joins.completedCreatedAt': '2019-06-14T00:00:00.000',
    'UsersStatuses_Joins.processingCreatedAt': '2021-05-20T00:00:00.000',
    'UsersStatuses_Joins.shippedCreatedAt': null,
  },
  {
    'UsersStatuses_Joins.name': 'Concepcion Maggio',
    'UsersStatuses_Joins.completedCreatedAt': null,
    'UsersStatuses_Joins.processingCreatedAt': '2020-07-14T00:00:00.000',
    'UsersStatuses_Joins.shippedCreatedAt': '2019-07-19T00:00:00.000',
  },
];
```

The drawback is that when the set of statuses changes, we'll need to amend the
cube definition in several places: update selected values and joins in SQL as
well as update the dimensions. Let's see how to work around that.

### Static attributes, DRY version

We can embrace the
[Don't Repeat Yourself](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself)
principle and eliminate the repetition by generating the cube definition
dynamically based on the list of statuses. Let's move all repeated code patterns
into handy functions and iterate over statuses in relevant parts of the cube's
code.

```javascript
const statuses = ['completed', 'processing', 'shipped'];

const createValue = (status, index) =>
  `MIN(orders_${index}.created_at) AS createdAt_${index}`;

const createJoin = (status, index) =>
  `LEFT JOIN public.orders AS orders_${index}
    ON users.id = orders_${index}.user_id
    AND orders_${index}.status = '${status}'`;

const createDimension = (status, index) => ({
  [`${status}CreatedAt`]: {
    sql: (CUBE) => `createdAt_${index}`,
    type: `time`,
  },
});

cube(`UsersStatuses_DRY`, {
  sql: `
    SELECT
      users.first_name,
      users.last_name,
      ${statuses.map(createValue).join(',')}
    FROM public.users AS users
    ${statuses.map(createJoin).join('')}
    GROUP BY 1, 2
  `,

  dimensions: Object.assign(
    {
      name: {
        sql: `first_name || ' ' || last_name`,
        type: `string`,
      },
    },
    statuses.reduce(
      (all, status, index) => ({
        ...all,
        ...createDimension(status, index),
      }),
      {}
    )
  ),
});
```

The new `UsersStatuses_DRY` cube is functionally identical to the
`UsersStatuses_Joins` cube above. Querying this new cube would yield the same
data. However, there's still a static list of statuses present in the cube's
source code. Let's work around that next.

### Dynamic attributes

We can eliminate the list of statuses from the cube's code by loading this list
from an external source, e.g., the data source. Here's the code from the
`fetch.js` file that defines the `fetchStatuses` function that would load the
statuses from the database. Note that it uses the `pg` package (Node.js client
for Postgres) and reuses the credentials from Cube. Let's keep this file outside
the `schema` folder:

```javascript
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.CUBEJS_DB_HOST,
  port: process.env.CUBEJS_DB_PORT,
  user: process.env.CUBEJS_DB_USER,
  password: process.env.CUBEJS_DB_PASS,
  database: process.env.CUBEJS_DB_NAME,
});

const statusesQuery = `
  SELECT DISTINCT status
  FROM public.orders
`;

exports.fetchStatuses = async () => {
  const client = await pool.connect();
  const result = await client.query(statusesQuery);
  client.release();

  return result.rows.map((row) => row.status);
};
```

In the cube file, we will use the `fetchStatuses` function to load the list of
statuses. We will also wrap the cube definition with the `asyncModule` built-in
function that allows the data schema to be created
[dynamically](https://cube.dev/docs/schema/advanced/dynamic-schema-creation).

```javascript
const fetchStatuses = require('../fetch').fetchStatuses;

asyncModule(async () => {
  const statuses = await fetchStatuses();

  const createValue = (status, index) =>
    `MIN(orders_${index}.created_at) AS createdAt_${index}`;

  const createJoin = (status, index) =>
    `LEFT JOIN public.orders AS orders_${index}
      ON users.id = orders_${index}.user_id
      AND orders_${index}.status = '${status}'`;

  const createDimension = (status, index) => ({
    [`${status}CreatedAt`]: {
      sql: (CUBE) => `createdAt_${index}`,
      type: `time`,
    },
  });

  cube(`UsersStatuses_Dynamic`, {
    sql: `
      SELECT
        users.first_name,
        users.last_name,
        ${statuses.map(createValue).join(',')}
      FROM public.users AS users
      ${statuses.map(createJoin).join('')}
      GROUP BY 1, 2
    `,

    dimensions: Object.assign(
      {
        name: {
          sql: `first_name || ' ' || last_name`,
          type: `string`,
        },
      },
      statuses.reduce(
        (all, status, index) => ({
          ...all,
          ...createDimension(status, index),
        }),
        {}
      )
    ),
  });
});
```

Again, the new `UsersStatuses_Dynamic` cube is functionally identical to the
previously created cubes. So, querying this new cube would yield the same data
too.

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/entity-attribute-value)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
