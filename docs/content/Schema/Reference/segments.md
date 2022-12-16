---
title: Segments
permalink: /schema/reference/segments
scope: cubejs
category: Data Schema
subCategory: Reference
menuOrder: 12
proofread: 06/18/2019
redirect_from:
  - /segments
---

Segments are predefined filters. You can use segments to define complex
filtering logic in SQL. For example, users for one particular city can be
treated as a segment:

```javascript
cube(`Users`, {
  // ...

  segments: {
    sfUsers: {
      sql: `${CUBE}.location = 'San Francisco'`,
    },
  },
});
```

Or use segments to implement cross-column `OR` logic:

```javascript
cube(`Users`, {
  // ...

  segments: {
    sfUsers: {
      sql: `${CUBE}.location = 'San Francisco' or ${CUBE}.state = 'CA'`,
    },
  },
});
```

As with other cube member definitions segments can be
[generated][ref-schema-gen]:

```javascript
const userSegments = {
  sfUsers: ['San Francisco', 'CA'],
  nyUsers: ['New York City', 'NY'],
};

cube(`Users`, {
  // ...

  segments: {
    ...Object.keys(userSegments)
      .map((segment) => ({
        [segment]: {
          sql: `${CUBE}.location = '${userSegments[segment][0]}' or ${CUBE}.state = '${userSegments[segment][1]}'`,
        },
      }))
      .reduce((a, b) => ({ ...a, ...b })),
  },
});
```

After defining a segment, you can pass it in [query object][ref-backend-query]:

```json
{
  "measures": ["Users.count"],
  "segments": ["Users.sfUsers"]
}
```

## Segments vs Dimension Filters

As segments are simply predefined filters, it can be difficult to determine when
to use segments instead of dimension filters.

Let's consider an example:

```javascript
cube(`Users`, {
  // ...

  dimensions: {
    location: {
      sql: `location`,
      type: `string`,
    },
  },

  segments: {
    sfUsers: {
      sql: `${CUBE}.location = 'San Francisco'`,
    },
  },
});
```

In this case following queries are equivalent:

```json
{
  "measures": ["Users.count"],
  "filters": [
    {
      "member": "Users.location",
      "operator": "equals",
      "values": ["San Francisco"]
    }
  ]
}
```

and

```json
{
  "measures": ["Users.count"],
  "segments": ["Users.sfUsers"]
}
```

This case is a bad candidate for segment usage and dimension filter works better
here. `Users.location` filter value can change a lot for user queries and
`Users.sfUsers` segment won't be used much in this case.

A good candidate case for a segment is when you have a complex filtering
expression which can be reused for a lot of user queries. For example:

```javascript
cube(`Users`, {
  // ...

  segments: {
    sfNyUsers: {
      sql: `${CUBE}.location = 'San Francisco' OR ${CUBE}.location like '%New York%'`,
    },
  },
});
```

[ref-backend-query]: /backend/rest/reference/query-format
[ref-schema-gen]: /recipes/schema-generation
