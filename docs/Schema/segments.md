---
title: Segments
permalink: /segments
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 6
proofread: 06/18/2019
---

The `segments` parameter declares a block to specify some partitions.

A segment is a subset of your data. Usually items with similar properties are divided into segments. For example, users for one particular city can be treated as a segment.

```javascript
segments: {
  sfUsers: {
    sql: `location = 'San Francisco'`
  }
}
```

After creating a segment, you can choose it in the segments section and receive partitioned data.
