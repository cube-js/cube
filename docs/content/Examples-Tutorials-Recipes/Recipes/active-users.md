---
title: Daily, Weekly, Monthly Active Users (DAU, WAU, MAU)
permalink: /recipes/active-users
category: Examples & Tutorials
subCategory: Analytics
menuOrder: 1
redirect_from:
  - /active-users
---

## Use case

We want to know the customer engagement of our store. To do this, we need to use
an [Active Users metric](https://en.wikipedia.org/wiki/Active_users).

## Data schema

Daily, weekly, and monthly active users are commonly referred to as DAU, WAU,
MAU. To get these metrics, we need to use a rolling time frame to calculate a
daily count of how many users interacted with the product or website in the
prior day, 7 days, or 30 days. Also, we can build other metrics on top of these
basic metrics. For example, the WAU to MAU ratio, which we can add by using already
defined `weeklyActiveUsers` and `monthlyActiveUsers`.

To calculate daily, weekly, or monthly active users we’re going to use the
[`rollingWindow`](https://cube.dev/docs/schema/reference/measures#parameters-rolling-window)
measure parameter.

```javascript
cube(`ActiveUsers`, {
  sql: `SELECT user_id, created_at FROM public.orders`,

  measures: {
    monthlyActiveUsers: {
      sql: `user_id`,
      type: `countDistinct`,
      rollingWindow: {
        trailing: `30 day`,
        offset: `start`,
      },
    },

    weeklyActiveUsers: {
      sql: `user_id`,
      type: `countDistinct`,
      rollingWindow: {
        trailing: `7 day`,
        offset: `start`,
      },
    },

    dailyActiveUsers: {
      sql: `user_id`,
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
    createdAt: {
      sql: `created_at`,
      type: `time`,
    },
  },
});
```

## Query

We should set a `timeDimensions` with the `dateRange`.

```bash
curl cube:4000/cubejs-api/v1/load \
'query={
  "measures": [
    "ActiveUsers.monthlyActiveUsers",
    "ActiveUsers.weeklyActiveUsers",
    "ActiveUsers.dailyActiveUsers",
    "ActiveUsers.wauToMau"
  ],
  "timeDimensions": [
    {
      "dimension": "ActiveUsers.createdAt",
      "dateRange": [
        "2020-01-01",
        "2020-12-31"
      ]
    }
  ]
}'
```

## Result

We got the data with our daily, weekly, and monthly active users.

```javascript
{
  "data": [
    {
      "ActiveUsers.monthlyActiveUsers": "22",
      "ActiveUsers.weeklyActiveUsers": "4",
      "ActiveUsers.dailyActiveUsers": "0",
      "ActiveUsers.wauToMau": "18.1818181818181818"
    }
  ]
}
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/active-users)
or run it with the `docker-compose up` command. You'll see the result, including
queried data, in the console.
