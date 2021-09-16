---
title: Passing Dynamic Parameters in a Query
permalink: /recipes/passing-dynamic-parameters-in-a-query
category: Examples & Tutorials
subCategory: Data schema
menuOrder: 4
---

## Use case

We want to know the ratio between the number of people in a particular city and
the total number of women in the country. The user can specify the city for the
filter. The trick is to get the value of the city from the user and use it in
the calculation. In the recipe below, we can learn how to join the data table
with itself and reshape the dataset!

## Data schema

Let's explore the `Users` cube data that contains various information about
users, including city and gender:

| id  | city     | gender | name            |
| --- | -------- | ------ | --------------- |
| 1   | Seattle  | female | Wendell Hamill  |
| 2   | Chicago  | male   | Rahsaan Collins |
| 3   | New York | female | Megane O'Kon    |
| ... | ...      | ...    | ...             |

To calculate the ratio between the number of women in a particular city and the
total number of people in the country, we need to define three measures. One of
them can recieve the city value from the filter in a query. Cube will apply this
filter via the `WHERE` clause to the dataset. So, we need to reshape the dataset
so that applying this filter wouldn’t affect the calculations. In this use case,
we can join the data table with itself to multiply the `city` column — applying
the filter would remove the multiplication while still allowing to access the
filter value:

```javascript
cube(`Users`, {
  sql: `
    WITH data AS (
      SELECT 
        users.id AS id,
        users.city AS city,
        users.gender AS gender
      FROM public.users
    ),
    
    cities AS (
      SELECT city
      FROM data
    ),
    
    grouped AS (
      SELECT 
        cities.city AS city_filter,
        data.id AS id,
        data.city AS city,
        data.gender AS gender
      FROM cities, data
      GROUP BY 1, 2, 3, 4
    )
    
    SELECT *
    FROM grouped
  `,

  measures: {
    totalNumberOfWomen: {
      sql: 'id',
      type: 'count',
      filters: [{ sql: `${CUBE}.gender = 'female'` }],
    },

    numberOfPeopleOfAnyGenderInTheCity: {
      sql: 'id',
      type: 'count',
      filters: [{ sql: `${CUBE}.city = ${CUBE}.city_filter` }],
    },

    ratio: {
      title: 'Ratio Women in the City to Total Number of People',
      sql: `1.0 * ${CUBE.numberOfPeopleOfAnyGenderInTheCity} / ${CUBE.totalNumberOfWomen}`,
      type: `number`,
    },
  },

  dimensions: {
    cityFilter: {
      sql: `city_filter`,
      type: `string`,
    },
  }
});
```

## Query

To get the ratio result depending on the city, we need to pass the value via a
filter in the query:

```javascript
{
  "measures": [
    "Users.totalNumberOfWomen",
    "Users.numberOfPeopleOfAnyGenderInTheCity",
    "Users.ratio"
  ],
  "filters": [
    {
      "member": "Users.cityFilter",
      "operator": "equals",
      "values": ["Seattle"]
    }
  ]
}
```

## Result

By joining the data table with itself and using the dimensions defined above, we
can get the ratio we wanted to achieve:

```javascript
[
  {
    'Users.totalNumberOfWomen': '259',
    'Users.numberOfPeopleOfAnyGenderInTheCity': '99',
    'Users.ratio': '0.38223938223938223938',
  }
];
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/passing-dynamic-parameters-in-query)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
