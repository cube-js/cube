---
title: Segments
permalink: /segments
scope: cubejs
category: Reference
menuOrder: 6
---

`segments` parameter declares a block to specify some partitions.

A segment is a subset of your data. Usually items with similar properties are divided into segments. For example, users for one particular city can be treated as a segment.

```javascript
segments: {
  sfUsers: {
    sql: `location = 'San Francisco'`
  }
}
```
After creating segment you can choose it in segments section and receive partitioned data.
