---
title: 'Code Reusability: Export and Import'
permalink: /export-import
scope: cubejs
category: Guides
subCategory: Tutorials
menuOrder: 20
---

[comment]: # (PROOFREAD: DONE)

In Cube.js your data schema is a code, and the code is much easier to manage when it is in small chunks. 
It is best practice to **keep files small and containing only relevant and not duplicated code**. 
As your data schema grows, maintaining and debugging would be much easier with a well-organized code base.

Cube.js supports ES6 style [export](https://developer.mozilla.org/en-US/docs/web/javascript/reference/statements/export) and [import](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Statements/import) statements. 
It allows you to **write code in one file and share this code so it can be used by another file or files**.

There are several typical use cases in Cube.js where it is considered best practice to extract some variables or functions and then import it when needed. 

## Managing constants
Quite often, you may want to have an array of test user ids, for example, to exclude from your analysis. 
You can define it once and `export` like this:

```javascript
// in constants.js
export const TEST_USER_IDS = [1,2,3,4,5];
```

Later, you can `import` into the cube, wherever needed:

```javascript
// in Users.js
import { TEST_USER_IDS } from `./constants`;

cube(`Users`, {
 sql: `...`,
 measures: { /* ... */ },

 dimensions: { /* ... */ },

 segments: {
   excludeTestUsers: {
     sql: `${CUBE}.id NOT IN (${TEST_USER_IDS.join(", ")})`
   }
 }
});
```
## Helpers functions
You can assign some commonly used SQL snippets to javascript functions. 
The example below shows the parsing helper function, which can be used across various cubes to correctly parse stored date data if it was stored as a string. 

[You can read more about working with string time dimensions here](working-with-string-time-dimensions).

```javascript
// in helpers.js
export const parseDateWithTimeZone = (column) =>
  `PARSE_TIMESTAMP('%F %T %Ez', ${column})`;
```

```javascript
// in events.js
import { parseDateWithTimeZone } from './helpers';

cube(`Events`, {
  sql: `SELECT * FROM events`,

  // ...

  dimensions: {
    date: {
      sql: `${parseDateWithTimeZone('date')}`,
      type: `time`
    }
  }
});
```


