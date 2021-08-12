---
title: Role Based Access
permalink: /recipes/role-based-access
category: Examples & Tutorials
subCategory: Access control
menuOrder: 1
---

## Use case

We might want to manage access to different data depending on the user's role.
In the recipe below, a user with the `operator` role can view only processing
orders from a shop, and a `manager` can view only shipped and completed orders.

## Data schema

To implement the role-based access, we will use
[JSON Web Token](https://cube.dev/docs/security) with role information in the
payload, and the
[queryRewrite](https://cube.dev/docs/security/context#using-query-rewrite) to
manage data access.

Let's add the role verify in the cube.js file.

```javascript
module.exports = {
  queryRewrite: (query, { securityContext }) => {
    if (!securityContext.role) {
      throw new Error('No role found in Security Context!');
    }

    if (securityContext.role == 'manager') {
      query.filters.push({
        member: 'Orders.status',
        operator: 'equals',
        values: ['shipped', 'completed'],
      });
    }

    if (securityContext.role == 'operator') {
      query.filters.push({
        member: 'Orders.status',
        operator: 'equals',
        values: ['processing'],
      });
    }

    return query;
  },
};
```

## Query

To get the number of orders as manager and operator, we will send two identical
requests with different authorization tokens.

```javascript
// manager
curl cube:4000/cubejs-api/v1/load \
-H "Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoibWFuYWdlciIsImlhdCI6MTYyODc0NTAxMSwiZXhwIjoxODAxNTQ1MDExfQ.1cOAjRHhrFKD7Tg3g57ppVm5nX4eI0zSk8JMbinfzTk" \
-G -s --data-urlencode "query={"dimensions": ["Orders.status"], "timeDimensions": [], "order": {"Orders.count": "desc"}, "measures": ["Orders.count"],"filters": []}"
```

```javascript
//operator
curl cube:4000/cubejs-api/v1/load \
-H "Authorization: eeyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjE2Mjg3NDUwNDUsImV4cCI6MTgwMTU0NTA0NX0.VErb2t7Bc43ryRwaOiEgXuU5KiolCT-69eI_i2pRq4o" \
-G -s --data-urlencode "query={"dimensions": ["Orders.status"], "timeDimensions": [], "order": {"Orders.count": "desc"}, "measures": ["Orders.count"],"filters": []}"
```

## Result

We have received different data depending on the user's role.

```javascript
// manager
[
  {
    'Orders.status': 'completed',
    'Orders.count': '3346',
  },
  {
    'Orders.status': 'shipped',
    'Orders.count': '3300',
  },
];
```

```javascript
// operator
[
  {
    'Orders.status': 'processing',
    'Orders.count': '3354',
  },
];
```

## Source

Please check out the full source code [here](https://github.com/rchkv/cubejs-rbac-recipe). It is
self-contained code, so if you execute `docker-compose up` command in the folder
with the source code, you'll see the query execution result's console log.
