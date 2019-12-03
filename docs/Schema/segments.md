---
title: Segments
permalink: /segments
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 6
proofread: 06/18/2019
---

Segments are predefined filters.

You can use segments to define complex filtering logic in SQL. 
For example, users for one particular city can be treated as a segment.

```javascript
cube(`Users`, {
  // ...
  
  segments: {
    sfUsers: {
      sql: `${CUBE}.location = 'San Francisco'`
    }
  }
});
```

Or use segments to implement cross-column `OR` logic:

```javascript
cube(`Users`, {
  // ...
  
  segments: {
    sfUsers: {
      sql: `${CUBE}.location = 'San Francisco' or ${CUBE}.state = 'CA'`
    }
  }
});
```

As with other cube member definitions segments can be [generated](/schema-generation):

```javascript
const userSegments = {
  sfUsers: ['San Francisco', 'CA'],
  nyUsers: ['New York City', 'NY']
}

cube(`Users`, {
  // ...
  
  segments: {
    ...(Object.keys(userSegments).map(segment => ({
      [segment]: {
        sql: `${CUBE}.location = '${userSegments[segment][0]}' or ${CUBE}.state = '${userSegments[segment][1]}'`
      }
    })).reduce((a, b) => ({ ...a, ...b })))
  }
});
```

After defining a segment, you can pass it in [query object](/query-format):

```javascript
{
  measures: ['Users.count'],
  segments: ['Users.sfUsers']
}
```
