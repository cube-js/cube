---
title: Daily, Weekly, Monthly active users
permalink: /recipes/dau-wau-mau-active-users
category: Examples, Tutorials & Recipes
menuOrder: 1
---

## Use case
You may be familiar with Active Users metric, which is commonly used to get a sense of your engagement. Daily, weekly, and monthly active users are commonly referred to as DAU, WAU, MAU. To get these metrics, we need to use a rolling time frame to calculate a daily count of how many users interacted with the product or website in the prior day, 7 days, or 30 days. Also, we can build other metrics on top of these basic metrics. For example, the WAU to MAU ratio. We can add it, using already defined `weeklyActiveUsers` and `monthlyActiveUsers`.

## Data schema

To calculate daily, weekly, or monthly active users we’re going to use the [rollingWindow](https://cube.dev/docs/schema/reference/measures#parameters-rolling-window) measure parameter. We’ll create a cube called `Users` with data from our template table.


```js
<GitHubCodeBlock
  href="https://github.com/rchkv/cubejs-active-users-recipe/blob/main/schema/Users.js"
  titleSuffixCount={2}
  lang="js"
/>

cube(`Users`, {
  sql: `SELECT * FROM public.users`,

  measures: {
    monthlyActiveUsers: {
      sql: `id`,
      type: `countDistinct`,
      rollingWindow: {
        trailing: `30 day`,
        offset: `start`,
      },
    },

    weeklyActiveUsers: {
      sql: `id`,
      type: `countDistinct`,
      rollingWindow: {
        trailing: `7 day`,
        offset: `start`,
      },
    },

    dailyActiveUsers: {
      sql: `id`,
      type: `countDistinct`,
      rollingWindow: {
        trailing: `1 day`,
        offset: `start`,
      },
    },
    
    wauToMau: {
      title: `WAU to MAU`,
      sql: `100.000 * ${weeklyActiveUsers} / NULLIF(${monthlyActiveUsers}, 0)`,
      type: `number`,
      format: `percent`,
    },
  },
  
  dimensions: {
    id: {
      sql: `id`,
      type: `number`,
      primaryKey: true
    },

    createdAt: {
      sql: `created_at`,
      type: `time`
    }
  },
});
```

## Query

We should set a `timeDimensions` with the `dateRange` in the query that will send to Cube.js API.

```js
{
  "measures": [
    "Users.monthlyActiveUsers",
    "Users.weeklyActiveUsers",
    "Users.dailyActiveUsers",
    "Users.wauToMau"
  ],
  "timeDimensions": [
    {
      "dimension": "Users.createdAt",
      "dateRange": [
        "2020-01-01",
        "2020-12-31"
      ]
    }
  ],
  "order": {},
  "limit": 5,
  "dimensions": [],
  "filters": []
}
```

## Result

We got the data with our daily, weekly, and monthly active users.

```js
<CubeQueryResultSet
  api="https://managing-aiea.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1"
  token="JWT_TOKEN"
  query={{
  "measures": [
    "Users.monthlyActiveUsers",
    "Users.weeklyActiveUsers",
    "Users.dailyActiveUsers",
    "Users.wauToMau"
  ],
  "timeDimensions": [
    {
      "dimension": "Users.createdAt",
      "dateRange": [
        "2020-01-01",
        "2020-12-31"
      ]
    }
  ],
  "order": {},
  "limit": 5,
  "dimensions": [],
  "filters": []
}}
/>

{
	"data": [
		{
			"Users.monthlyActiveUsers": "22",
			"Users.weeklyActiveUsers": "4",
			"Users.dailyActiveUsers": "0",
			"Users.wauToMau": "18.1818181818181818"
		}
	]
}
```

## Source

Please check out the full source code

```js
<GitHubFolderLink
  href="https://github.com/rchkv/cubejs-pagination-recipe"
/>
```