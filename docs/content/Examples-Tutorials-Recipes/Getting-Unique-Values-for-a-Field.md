---
title: Getting Unique Values for a Field
permalink: /recipes/getting-unique-values-for-a-field
category: Examples & Tutorials
subCategory: Data schema
menuOrder: 4
---

## Use case

We have an internal analytical dashboard with information about the users of our
online store. To filter users by city, we need to display all unique values in
the dropdown. In the recipe below, we'll learn how to get unique values from all
[dimensions](https://cube.dev/docs/schema/reference/dimensions).

## Data schema

To filter users by city, we need to define the appropriate dimension:

```javascript
cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true,
    },

    city: {
      sql: `city`,
      type: `string`,
    },

    state: {
      sql: `state`,
      type: `string`,
    },
  },
});
```

## Query

It is enough to include only a dimension in the query to get all unique values
of that dimension:

```javascript
{
  "dimensions": ["Users.city"]
}
```

## Result

We got the unique values of the `city` dimension, and now we can add them to the
dropdown on the dashboard.

```javascript
[
  {
    'Users.city': 'Austin',
  },
  {
    'Users.city': 'Chicago',
  },
  {
    'Users.city': 'Los Angeles',
  },
  {
    'Users.city': 'Mountain View',
  },
];
```

## Additional

What if we want to add dropdowns for other dimensions values without adding
these values manually? We can use the `/meta`
[endpoint](https://cube.dev/docs/rest-api#api-reference-v-1-meta) for this! Just
send a query to `/meta` and get back meta-information for the cube, including
dimension names.

```bash
curl http://localhost:4000/cubejs-api/v1/meta
```

```javascript
// meta-information including dimensions
{
  "cubes": [
    {
      "name": "Users",
      "title": "Users",
      "measures": [],
      "dimensions": [
        {
          "name": "Users.city",
          "title": "Users City",
          "type": "string",
          "shortTitle": "City",
          "suggestFilterValues": true
        },
        {
          "name": "Users.state",
          "title": "Users State",
          "type": "string",
          "shortTitle": "State",
          "suggestFilterValues": true
        }
      ],
      "segments": []
    }
  ]
}
```

Then we can iterate through dimension names and add them to a query to the
`/load` [endpoint](https://cube.dev/docs/rest-api#api-reference-v-1-load). This
action will allow us to add unique values to the dropdowns for every dimension.

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/getting-unique-values-for-a-field)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
