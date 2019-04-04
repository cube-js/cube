---
title: Funnels
permalink: /funnels
category: Funnels
menuOrder: 13
---

Funnels are representing a series of events that lead users towards a defined goal. It's commonly used in product, marketing and sales analytics.

Regardless of the domain, every funnel has the following traits:
* identity of the object moving through the funnel, ex. user or lead;
* set of steps, through object moves;
* date and time of each step;
* time to convert between steps;

Since funnels have pretty standard structure, they are good candidates for being extracted into reasable packages. Cube.js goes pre-packaged with a standard funnel package.

```javascript
// First step is to require the Funnel package
const Funnels = require(`Funnels`);

cube(`PurchaseFunnel`, {
  extends: Funnels.eventFunnel({
    userId: {
      sql: `user_id`
    },
    time: {
      sql: `timestamp`
    },
    steps: [{
      name: `view_product`,
      eventsView: {
        sql: `select * from events where event = 'view_product'`
      }
    }, {
      name: `purchase_product`,
      eventsView: {
        sql: `select * from events where event = 'purchase_product'`
      },
      timeToConvert: '1 day'
    }]
  })
});
```
Cube.js will generate an SQL for this funnel. Since funnel analysis in SQL is
not straightforward and easy, the SQL code itself is quite complicated,
even for such a small funnel.

<a href="#" class="accordion-trigger" id="show-sql-accordion"> Show Funnel's SQL </a>
<div class="accordion" id="show-sql-accordion-body">
  <pre class="language-sql">
    <code class="language-sql">
SELECT
  purchase_funnel.step "purchase_funnel.step",
  count(purchase_funnel.user_id) "purchase_funnel.conversions"
FROM
  (
    WITH joined_events AS (
      select
        view_product_events.user_id view_product_user_id,
        purchase_product_events.user_id purchase_product_user_id,
        view_product_events.t
      FROM
        (
          select
            user_id user_id,
            timestamp t
          from
            (
              select
                *
              from
                events
              where
                event = 'view_product'
            ) e
        ) view_product_events
        LEFT JOIN (
          select
            user_id user_id,
            timestamp t
          from
            (
              select
                *
              from
                events
              where
                event = 'purchase_product'
            ) e
        ) purchase_product_events ON view_product_events.user_id = purchase_product_events.user_id
        AND purchase_product_events.t >= view_product_events.t
        AND (
          purchase_product_events.t :: timestamptz AT TIME ZONE 'America/Los_Angeles'
        ) <= (
          view_product_events.t :: timestamptz AT TIME ZONE 'America/Los_Angeles'
        ) + interval '1 day'
    )
    select
      user_id,
      first_step_user_id,
      step,
      max(t) t
    from
      (
        SELECT
          view_product_user_id user_id,
          view_product_user_id first_step_user_id,
          t,
          'View Product' step
        FROM
          joined_events
        UNION ALL
        SELECT
          purchase_product_user_id user_id,
          view_product_user_id first_step_user_id,
          t,
          'Purchase Product' step
        FROM
          joined_events
      ) as event_steps
    GROUP BY
      1,
      2,
      3
  ) AS purchase_funnel
WHERE
  (
    purchase_funnel.t >= '2018-07-01T07:00:00Z' :: timestamptz
    AND purchase_funnel.t <= '2018-07-31T06:59:59Z' :: timestamptz
  )
GROUP BY
  1
ORDER BY
  2 DESC
LIMIT
  5000
    </code>
  </pre>
</div>

## Funnel parameters

### userId
A unique key to identify users, moving through the funnel.
```javascript
  userId: {
    sql: `user_id`
  }
```

### time
A timestamp of the event.
```javascript
  time: {
    sql: `timestamp`
  }
```

### steps
An array of steps. Each step has 2 required and 1 optional parameters:
 * __name__ *(required)* - Name of the step. It must be unique within a funnel.
 * __eventsView__ *(required)* - Events table for the step. It must contain userId and time fields. For example, if we have defined the userId as `user_id` and time as `timestamp`, we need to have these fields in a table we're selecting.
 * __timeToConvert__ *(optional)* - A time window for conversion to happen. Set it, depending on your funnel logic. If set to `1 day`, for instance, it means the funnel will include only users who made a purchase within 1 day after visiting the product page.

```javascript
  steps: [{
    name: `purchase_product`,
    eventsView: {
      sql: `select * from events where event = 'purchase_product'`
    },
    timeToConvert: '1 day'
  }]
```

## Joining funnels

In order to provide additional dimensions funnels can be joined with other cubes using user id at the first step of a funnel.
It'll be always `belongsTo` relationship and hence you should always join corresponding user cube.
Here by 'user' we understand any entity that can go through sequence of steps within funnel.
It can be real web user with some auto assigned id or specific email sent by some email automation that goes through typical flow of events like 'sent', 'opened', 'clicked'.
For example for our `PurchaseFunnel` we can add join as following:

```javascript
cube(`PurchaseFunnel`, {
  joins: {
    Users: {
      relationship: `belongsTo`,
      sql: `${CUBE}.first_step_user_id = ${Users}.id`
    }
  },

  extends: Funnels.eventFunnel({
    // ...
  })
});
```

## Using funnels

Cube.js is based on [multidimensional analysis](https://en.wikipedia.org/wiki/Multidimensional_analysis) and operates on the measures and dimensions level. Thus, all funnel data is represented via a set of measures and dimensions.

Funnel-based cubes have the following structure:

### Measures
* __conversions__ - Count of conversions in the funnel. The most useful when
  broken down by __steps__. It's the classic funnel view.
* __conversionsPercent__ - Percentage of conversions. It is useful when you
  want to inspect a specific step, or set of steps, and find out how a conversion was changing
over time.

### Dimensions
* __step__ - Describes funnels' steps. Use it to break down __conversions__ or
  __conversionsPercent__ by steps, or to filter for a specific step.
* __time__ - time dimension for the funnel. Use it to filter your analysis for
  specific dates or to analyze how conversion was changing over time.

In the following example, we use measure `conversions` with dimension `steps`
to display a classic bar chart showing the funnel's steps.

<iframe src="https://codesandbox.io/embed/nw87w1nnjm?fontsize=14" style="width:100%; height:500px; border:0; border-radius: 4px; overflow:hidden;" sandbox="allow-modals allow-forms allow-popups allow-scripts allow-same-origin"></iframe>
