---
title: Getting Unique Values for a Field
permalink: /recipes/getting-unique-values-for-a-field
category: Examples & Tutorials
subCategory: Queries
menuOrder: 5
---

## Use case

We have a dashboard with information about the users, and we'd like to filter
them by city. To do so, we need to display all unique values for cities in the
dropdown. In the recipe below, we'll learn how to get unique values for
[dimensions](https://cube.dev/docs/schema/reference/dimensions).

## Data schema

To filter users by city, we need to define the appropriate dimension:

```javascript
cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  dimensions: {
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

We got the unique values of the `city` dimension, and now we can use them in the
dropdown on the dashboard:

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

## Choosing dimensions

In case we need to choose a dimension or render dropdowns for all dimensions, we
can fetch the list of dimensions for all cubes from the `/meta`
[endpoint](https://cube.dev/docs/backend/rest/reference/api#api-reference-v-1-meta):

```bash
curl http://localhost:4000/cubejs-api/v1/meta
```

```javascript
// Information about cubes, dimensions included
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

Then, we can iterate through dimension names and use any of them in a
[query](#query).

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/getting-unique-values-for-a-field)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
