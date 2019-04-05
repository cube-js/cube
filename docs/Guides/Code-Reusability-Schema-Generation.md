---
title: 'Code Reusability: Schema Generation'
permalink: /schema-generation
scope: cubejs
category: Guides
menuOrder: 7
---

Cube.js Schema is a Javascript code, so full power of this language can be used to configure your schema definitions.
In this guide we generate several measure definitions based on a array of Javascript objects.

One example real world scenario is when you have single `events` table which has `event_type` and `user_id` columns in it and you want to create separate user count measure for each event.

It can be done as simple as

```javascript

const events = ['app_engagement', 'login', 'purchase'];

cube(`Events`, {
  sql: `select * from events`,

  measures: Object.assign({
    count: {
      type: `count`
    }
  }, events.map(e => ({
    [`${e}_userCount`]: {
      type: `countDistinct`,
      sql: `user_id`,
      filters: [{
        sql: `${CUBE}.event_type = '${e}'`
      }]
    }
  })).reduce((a, b) => Object.assign(a, b)))
});
```

In this case we use standard Javascript functions [Object.assign](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/assign), [Array.map](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/map) and [Array.reduce](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array/reduce) to add user count measure definitions based on `events` array.
This approach allows you to maintain list of events in very concise manner without boilerplate code.
This configuration can be reused using [export / import feature](export-import).
