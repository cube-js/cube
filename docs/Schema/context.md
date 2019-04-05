---
title: Contexts
permalink: /context
scope: cubejs
category: Reference
subCategory: Reference
menuOrder: 9
---

A `context` is a combination of related cubes. Contexts are used to organize
related cubes into some domain, for example Marketing, Sales, Product Analytics.
It helps to provide end users with specific set of cubes they need and not to
overwhelm them with all possible cubes, measures and dimensions in your Data
Schema.

```javascript
context(`Marketing`, {
  contextMembers: [Sessions, Events, Ads]
});
```
## Parameters

### contextMembers

The `contextMembers` is an array of cubes, which will be included into the
context. Cubes could be members of multiple contexts.

```javascript
context(`Marketing`, {
  contextMembers: [Sessions, Events, Users]
});

context(`Sales`, {
  contextMembers: [Users, Deals, Meetings]
});
```
