---
title: Implementing Event Analytics
permalink: /recipes/event-analytics
category: Examples & Tutorials
subCategory: Analytics
menuOrder: 2
redirect_from:
  - /event-analytics
---

<InfoBox>

This content is being moved to the
[Cube.js community forum](https://forum.cube.dev/). We encourage you to follow
the content and discussions
[in the new forum post](https://forum.cube.dev/t/event-analytics-transforming-raw-event-data-into-sessions).

</InfoBox>

This tutorial walks through how to transform raw event data into sessions. Many
“out-of-box” web analytics solutions come already prepackaged with sessions, but
they work as a “black box.” It doesn’t give the user either insight into or
control how these sessions defined and work.

With Cube.js SQL-based sessions schema, you’ll have full control over how these
metrics are defined. It will give you great flexibility when designing sessions
and events to your unique business use case.

A few question we’ll answer with our sessions schema:

- How do we measure session duration?
- What is our bounce rate?
- What areas of the app are most used?
- Where are users spending most of their time?
- How do we filter sessions where a user performs a specific action?

We’ll explore the subject using the data from
[Segment.com](https://segment.com)’s analytics.js library. The same concept
could be applied for different data collection tools, such as
[Snowplow](https://snowplowanalytics.com).

## What is a session?

A session is defined as a group of interactions one user takes within a given
time frame on your app. Usually that time frame defaults to 30 minutes, meaning
that whatever a user does on your app (e.g. browses pages, downloads resources,
purchases products) before they leave equals one session.

<div style="text-align: center">
  <img
    src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Schema/session-schema.png"
    style="border: none"
    width="100%"
  />
</div>

## Unify events and page views into single cube

Segment stores page view data as a `pages` table and events data as a `tracks`
table. For sessions we want to rely not only on page views data, but on events
as well. Imagine you have a highly interactive app, a user loads a page and can
stay on this page interacting with the website for while. Hence, you want to
count events as part of the session as well.

To do that we need to combine page view data and event data into a single cube.
We’ll call the cube just events and assign a page views event type to
`pageview`. Also, we’re going to assign a unique event_id to every event to use
as primary key.

```javascript
// Create file Events.js with the following content
cube(`Events`, {
  sql: `
     SELECT
      t.id || '-e' as event_id
      , t.anonymous_id as anonymous_id
      , t.timestamp
      , t.event
      , t.context_page_path as page_path
      , NULL as referrer
    from javascript.tracks as t

    UNION ALL

    SELECT
      p.id as event_id
      , p.anonymous_id
      , p.timestamp
      , 'pageview' as event
      , p.context_page_path as page_path
      , p.referrer as referrer
    FROM javascript.pages as p
    `,
});
```

The above SQL creates base table for our events cube. Now we can add some
measures to calculate the number of events and number of page views only, using
a filter on `event` column.

```javascript
// Add this measures block to Events cube
measures: {
  count: {
    sql: `event_id`,
    type: `count`
  },

  pageViewsCount: {
    sql: `event_id`,
    type: `count`,
    filters: [
      { sql: `${CUBE}.event = 'pageview'` }
    ]
  }
}
```

Having this in place, we will already be able to calculate the total number of
events and pageviews. Next, we’re going to add dimensions to be able to filter
events in a specific time range and for specific types.

```javascript
// Add this dimensions block to the Events cube
dimensions: {
  timestamp: {
    sql: `timestamp`,
    type: `time`
  },

  eventId: {
    sql: `event_id`,
    type: `number`,
    primaryKey: true
  },

  event: {
    sql: `event`,
    type: `string`
  }
}
```

Now we have everything for Events cube and can move forward to grouping these
events into sessions.

## Creating Sessions

As a recap, a session is defined as a group of interactions one user takes
within a given time frame on your app. Usually that time frame defaults to 30
minutes. First, we’re going to use
[LAG() function](https://docs.aws.amazon.com/redshift/latest/dg/r_WF_LAG.html)
in Redshift to determine an inactivity_time between events.

```sql
select
  e.event_id AS event_id
  , e.anonymous_id AS anonymous_id
  , e.timestamp AS timestamp
  , DATEDIFF(minutes, LAG(e.timestamp) OVER(PARTITION BY e.anonymous_id ORDER BY e.timestamp), e.timestamp) AS inactivity_time
FROM events AS e
```

`inactivity_time` is the time in minutes between the current event and the
previous. We’re going to use `inactivity_time` to terminate a session based on
30 minutes of inactivity. This window could be changed to any value, based on
how users interact with your app. Now we’re ready to introduce our Sessions
cube.

```javascript
// Create new file Sessions.js with the following content
cube(`Sessions`, {
  sql: `
    SELECT
      row_number() over(partition by event.anonymous_id order by event.timestamp) || ' - '|| event.anonymous_id as session_id
      , event.anonymous_id
      , event.timestamp as session_start_at
      , row_number() over(partition by event.anonymous_id order by event.timestamp) as session_sequence
      , lead(timestamp) over(partition by event.anonymous_id order by event.timestamp) as next_session_start_at
    FROM
      (SELECT
        e.anonymous_id
        , e.timestamp
        , DATEDIFF(minutes, LAG(e.timestamp) OVER(PARTITION BY e.anonymous_id ORDER BY e.timestamp), e.timestamp) AS inactivity_time
       FROM ${Events.sql()} AS e
      ) as event
    WHERE (event.inactivity_time > 30 OR event.inactivity_time is null)
    `,
});
```

The SQL query above creates sessions, either where inactivity_time is NULL,
which means it is the first session for the user, or after 30 minutes of
inactivity.

As a primary key, we’re going to use `session_id`, which is the combination of
the `anonymous_id` and the session sequence, since it’s guaranteed to be unique
for each session. Having this in place, we can already count sessions and plot a
time series chart of sessions.

```javascript
// Add these two blocks for measures and dimensions to the Sessions cube
measures: {
    count: {
      sql: `session_id`,
      type: `count`
    }
  },

  dimensions: {
    startAt: {
      sql: `session_start_at`,
      type: `time`
    },

    sessionID: {
      sql: `session_id`,
      type: `number`,
      primaryKey: true
    }
  }
```

## Connecting Events to Sessions

The next step is to identify the events contained within the session and the
events ending the session. It’s required to get metrics such as session duration
and events per session, or to identify sessions where specific events occurred
(we’re going to use that for funnel analysis later on). We’re going to
[declare join](/schema/reference/joins), that Events `belongsTo` Sessions and a
specify condition, such as all users' events from session start (inclusive) till
the start of the next session (exclusive) belong to that session.

```javascript
// Add the joins block to the Events cube
joins: {
  Sessions: {
    relationship: `belongsTo`,
    sql: `
      ${Events}.anonymous_id = ${Sessions}.anonymous_id
      AND ${Events}.timestamp >= ${Sessions}.session_start_at
      AND (${Events}.timestamp < ${Sessions}.next_session_start_at or ${Sessions}.next_session_start_at is null)
    `
  }
}
```

To determine the end of the session, we’re going to use the
[`subQuery` feature](/schema/fundamentals/additional-concepts#subquery) in
Cube.js.

```javascript
// Add the lastEventTimestamp measure to the measures block in the Events cube
lastEventTimestamp: {
  sql: `timestamp`,
  type: `max`,
  shown: false
}

// Add the following dimensions to the dimensions block in the Sessions cube
endRaw: {
  sql: `${Events.lastEventTimestamp}`,
  type: `time`,
  subQuery: true,
  shown: false
},

endAt: {
  sql:
`CASE WHEN ${endRaw} + INTERVAL '1 minutes' > ${CUBE}.next_session_start_at
     THEN ${CUBE}.next_session_start_at
     ELSE ${endRaw} + INTERVAL '30 minutes'
     END`,
  type: `time`
},

durationMinutes: {
  sql: `datediff(minutes, ${CUBE}.session_start_at, ${endAt})`,
  type: `number`
}

// Add the following measure to the measures block in the Sessions cube
averageDurationMinutes: {
  type: `avg`,
  sql: `${durationMinutes}`
}
```

## Mapping Sessions to Users

Right now all our sessions are anonymous, so the final step in our modeling
would be to map sessions to users in case, they have signed up and have been
assigned a `user_id`. Segment keeps track of such assignments in a table called
identifies. Every time you identify a user with segment it will connect the
current `anonymous_id` to the identified user id.

We’re going to create an **Identifies** cube, which will not contain any visible
measures and dimensions for users to use in Insights, but instead will provide
us with a `user_id` to use in the **Sessions** cube. Also, Identifies could be
used later on to join **Sessions** to your **Users** cube, which could be a cube
built based on your internal database data for users.

```javascript
// Create a new file for the Identifies cube with following content
cube(`Identifies`, {
  sql: `select distinct user_id, anonymous_id from javascript.identifies`,

  measures: {},

  dimensions: {
    id: {
      primaryKey: true,
      sql: `user_id || '-' || anonymous_id`,
      type: `string`,
    },

    userId: {
      sql: `user_id`,
      type: `number`,
      format: `id`,
    },
  },
});
```

We need to declare a relationship between **Identifies** and **Sessions**, where
session belongs to identity.

```javascript
// Declare this joins block in the Sessions cube
joins: {
  Identifies: {
    relationship: `belongsTo`,
    sql: `${Identifies}.anonymous_id = ${Sessions}.anonymous_id`
  }
}
```

Once we have it, we can create a dimension `userId`, which will be either a
`user_id` from the identifies table or an `anonymous_id` in case we don’t have
the identity of a visitor, which means that this visitor never signed in.

```javascript
// Add a new dimension to the Sessions cube
userId: {
  sql: `coalesce(${Identifies.userId}, ${CUBE}.anonymous_id)`,
  type: `string`
}
```

Based on the just-created dimension, we can add two new metrics: the count of
users and the average sessions per user.

```javascript
// Add following measures to the Sessions cube
usersCount: {
  sql: `${userId}`,
  type: `countDistinct`
},

averageSessionsPerUser: {
  sql: `${count}::numeric / nullif(${usersCount}, 0)`,
  type: `number`
}
```

That was our final step in building a foundation for sessions schema.
Congratulations on making it here! Now we’re ready to add some advanced metrics
on top of it.

## More metrics for Sessions

### <--{"id" : "More metrics for Sessions"}--> Number of Events per Session

This one is super easy to add with a subQuery dimension. We just calculate count
of Events, which we already have as a measure in the Events cube, as a dimension
in the Sessions cube.

```javascript
numberEvents: {
  sql: `${Events.count}`,
  type: `number`,
  subQuery: true
}
```

### <--{"id" : "More metrics for Sessions"}--> Bounce Rate

A bounced session is usually defined as a session with only one event. Since
we’ve just defined the number of events per session, we can easily add a
dimension `isBounced` to identify bounced sessions to the Sessions cube. Using
this dimension, we can add two measures to the Sessions cube as well - a count
of bounced sessions and a bounce rate.

```javascript
dimensions: {
  isBounced: {
   type: `string`,
    case: {
      when: [ { sql: `${numberEvents} = 1`, label: `True` }],
      else: { label: `False` }
    }
  }
}

measures: {
  bouncedCount: {
    sql: `session_id`,
    type: `count`,
    filters:[{
      sql: `${isBounced} = 'True'`
    }]
  },

  bounceRate: {
    sql: `100.00 * ${bouncedCount} / NULLIF(${count}, 0)`,
    type: `number`,
    format: `percent`
  }
}
```

### <--{"id" : "More metrics for Sessions"}--> First Referrer

We already have this column in place in our base table. We’re just going to
define a dimension on top of this.

```javascript
firstReferrer: {
  type: `string`,
  sql: `first_referrer`
}
```

### <--{"id" : "More metrics for Sessions"}--> Sessions New vs Returning

Same as for the first referrer. We already have a `session_sequence` field in
the base table, which we can use for the `isFirst` dimension. If
`session_sequence` is 1 - then it belongs to the first session, otherwise - to a
repeated session.

```javascript
// Add this dimension to the Sessions cube
isFirst: {
  type: `string`,
  case: {
    when: [{ sql: `${CUBE}.session_sequence = 1`, label: `First`}],
    else: { label: `Repeat` }
  }
}

// Add following measures to Sessions cube
repeatCount: {
  description: `Repeat Sessions Count`,
  sql: `session_id`,
  type: `count`,
  filters: [
    { sql: `${isFirst} = 'Repeat'` }
  ]
},

repeatPercent: {
  description: `Percent of Repeat Sessions`,
  sql: `100.00 * ${repeatCount} / NULLIF(${count}, 0)`,
  type: `number`,
  format: `percent`
}
```

### <--{"id" : "More metrics for Sessions"}--> Filter Sessions, where user performs specific event

Often, you want to select specific sessions where a user performed some
important action. In the example below, we’ll filter out sessions where the
`form_submitted` event happened. To do that, we need to follow 3 steps:

Define a measure on the Events cube to count only `form_submitted` events.

```javascript
// Add this measure to the Events cube
formSubmittedCount: {
  sql: `event_id`,
  type: `count`,
  filters: [
    { sql: `${CUBE}.event = 'form_submitted'` }
  ]
}
```

Define a dimension `formSubmittedCount` on the Sessions using subQuery.

```javascript
// Add this dimension to the Sessions cube
formSubmittedCount: {
  sql: `${EventsWIP.formSubmittedCount}`,
  type: `number`,
  subQuery: true
}
```

Create a measure to count only sessions where `formSubmittedCount` is greater
than 0.

```javascript
// Add this measure to the Sessions cube
withFormSubmittedCount: {
  type: `count`,
  sql: `session_id`,
  filters: [
    { sql: `${formSubmittedCount} > 0` }
  ]
}
```

Now we can use the `withFormSubmittedCount` measure to get only sessions when
the `form_submitted`event occured.
