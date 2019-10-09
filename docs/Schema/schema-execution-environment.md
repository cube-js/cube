---
title: Execution Environment
permalink: /schema-execution-environment
scope: cubejs
category: Reference
menuOrder: 9
subCategory: Reference
---

Cube.js Schema Compiler uses [Node.js VM](https://nodejs.org/api/vm.html) to execute schema compiler code.
It gives required flexibility allowing to transpile schema files before they get executed, allow to store schema in external databases and execute untrusted code in safe manner.
Cube.js Schema JavaScript is standard JavaScript supported by Node.js starting version 8 with following exceptions.

## Require

Being executed in VM data schema JavaScript code doesn't have access to [Node.js require](https://nodejs.org/api/modules.html#modules_require_id) directly.
Instead `require()` is implemented by Schema Compiler to provide access to other data schema files and to regular Node.js modules.

## Import / Export

Data schema JavaScript files are transpiled to convert ES6 `import` and `export` expressions to corresponding Node.js calls.
In fact `import` is routed to [Require](#require) method.

`export` can be used to define named exports as well as default ones:

**constants.js:**
```javascript
export const TEST_USER_IDS = [1,2,3,4,5];
```

**usersSql.js:**
```javascript
export default (usersTable) => `select * form ${usersTable}`
```

Later, you can `import` into the cube, wherever needed:

**Users.js**:
```javascript
// in Users.js
import { TEST_USER_IDS } from './constants';
import usersSql from './usersSql'

cube(`Users`, {
 sql: usersSql(`users`),
 measures: { /* ... */ },

 dimensions: { /* ... */ },

 segments: {
   excludeTestUsers: {
     sql: `${CUBE}.id NOT IN (${TEST_USER_IDS.join(", ")})`
   }
 }
});
```

## asyncModule

If there's a need to generate schema based on values from external API or database `asyncModule` method can be used for such scenario.
`asyncModule` method allows to register async function to be executed at the end of data schema file compile phase so additional definitions can be added during this function call.

For example:

```javascript
const fetch = require('node-fetch');
const Funnels = require('Funnels');

asyncModule(async () => {
  const funnels = await (await fetch('http://your-api-endpoint/funnels')).json();

  class Funnel {
    constructor({ title, steps }) {
      this.title = title;
      this.steps = steps;
    }

    get transformedSteps() {
      return Object.keys(this.steps).map((key, index) => {
        const value = this.steps[key];
        let where = null
        if (value[0] === PAGE_VIEW_EVENT) {
          if (value.length === 1) {
            where = `event = '${value[0]}'`
          } else {
            where = `event = '${value[0]}' AND page_title = '${value[1]}'`
          }
        } else {
          where = `event = 'se' AND se_category = '${value[0]}' AND se_action = '${value[1]}'`
        }

        return {
          name: key,
          eventsView: {
            sql: () => `select * from (${eventsSQl}) WHERE ${where}`
          },
          timeToConvert: index > 0 ? '30 day' : null
        }
      });
    }

    get config() {
      return {
        userId: {
          sql: () => `user_id`
        },
        time: {
          sql: () => `time`
        },
        steps: this.transformedSteps
      }
    }
  }

  funnels.forEach((funnel) => {
    const funnelObject = new Funnel(funnel);
    cube(funnelObject.title, {
      extends: Funnels.eventFunnel(funnelObject.config),
      preAggregations: {
        main: {
          type: `originalSql`,
        }
      }
    });
  });
})
```

## Context symbols transpile

Cube.js uses custom transpiler to optimize boilerplate code around referencing cubes and cube members.
There're reserved property names inside `cube` definition that undergo reference resolve transpiling process:

-`sql` 
- `measureReferences`
- `dimensionReferences`
- `segmentReferences`
- `timeDimensionReference` 
- `drillMembers`
- `drillMemberReferences`
- `contextMembers`

Each of these properties inside `cube` and `context` definitions are transpiled to functions with resolved arguments.

For example:

```javascript
cube(`Users`, {
  // ...
  
  measures: {
    count: {
      type: `count`
    },
    
    ratio: {
      sql: `sum(${CUBE}.amount) / ${count}`,
      type: `number`
    }
  }
});
```

is transpiled to:

```javascript
cube(`Users`, {
  // ...
  
  measures: {
    count: {
      type: `count`
    },
    
    ratio: {
      sql: (CUBE, count) => `sum(${CUBE}.amount) / ${count}`,
      type: `number`
    }
  }
});
```
