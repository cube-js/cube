---
title: Daily, Weekly, Monthly Active Users
permalink: /active-users
category: Reference
subCategory: Tutorials
menuOrder: 14
---

<!-- prettier-ignore-start -->
[[info | ]]
| This content is being moved to the [Cube.js community forum](https://forum.cube.dev/).
| We encourage you to follow the content and discussions [in the new forum post](https://forum.cube.dev/t/daily-weekly-monthly-active-users).
<!-- prettier-ignore-end -->

You may be familiar with <b>Active Users metric</b>, which is commonly used to
get a sense of your engagement. Daily, weekly, and monthly active users are
commonly referred to as <b>DAU, WAU, MAU</b>. To get these metrics, we need to
use a <b>rolling time frame</b> to calculate a daily count of how many users
interacted with the product or website in the prior day, 7 days, or 30 days.

You need event data to build this analysis. You can use tools like Google
Analytics, Segment, Snowplow, or your custom event tracking system.

To calculate daily, weekly, or monthly active users we’re going to use the
`rollingWindow` measure parameter. `rollingWindow` accepts 3 parameters:
trailing, leading, and offset. You can read about what each of them does
[here](/schema/reference/measures#parameters-rolling-window).

For our purpose, we need only offset and trailing. We will set offset to
<b>start</b> and the trailing parameter to the number of days – 1, 7, or 30.

In the example below, we’ll create a cube called `ActiveUsers` with data from
our events table.

<div class="block help-block">Please note, we are using interval literal in the trailing parameter.
The example below should work in Redshift and BigQuery. The exact interval literal could be different, depending on your database.
</div>

```javascript
cube(`ActiveUsers`, {
  sql: `select id, user_id, timestamp from events`,

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
        trailing: `1 week`,
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
  },

  dimensions: {
    timestamp: {
      sql: `timestamp`,
      type: `time`,
    },
  },
});
```

Going further, we can build other metrics on top of these basic metrics. For
example, <b>the DAU to MAU ratio</b> is one of the most popular metrics used to
<b>measure the stickiness of the product</b>. We can easily add it, using
already defined `dailyActiveUsers` and `monthlyActiveUsers`.

```javascript
cube(`ActiveUsers`, {
  measures: {
    dauToMau: {
      title: `DAU to MAU`,
      sql: `100.000 * ${dailyActiveUsers} / NULLIF(${monthlyActiveUsers}, 0)`,
      type: `number`,
      format: `percent`,
    },
  },
});
```
