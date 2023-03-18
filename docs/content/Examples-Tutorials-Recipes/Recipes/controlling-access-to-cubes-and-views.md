---
title: Controlling access to cubes and views
permalink: /recipes/controlling-access-to-cubes-and-views
category: Examples & Tutorials
subCategory: Access control
menuOrder: 1
---

## Use case

We want to manage user access to different cubes and/or views depending on some
sort of user property. In the recipe below, we will manage access to a view so
that only users with a `department` claim in their JWT can query it.

## Configuration

```javascript
module.exports = {
  contextToAppId: ({ securityContext }) => {
    return `CUBEJS_APP_${securityContext.company}`;
  },
  extendContext: (req) => {
    const { department } = jwtDecode(req.headers['authorization']);
    return {
      isFinance: department === 'finance',
    };
  },
};
```

## Data schema

```javascript
// Orders.js
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,
  shown: false,

  ...,
});

// Users.js
cube(`Users`, {
  sql: `SELECT * FROM public.users`,
  shown: false,

  ...,
});

// TotalRevenuePerCustomer.js
view('TotalRevenuePerCustomer', {
  description: `Total revenue per customer`,
  shown: COMPILE_CONTEXT.permissions.isFinance,

  includes: [
    Orders.totalRevenue,
    Users.company,
  ],
});
```

## Query

After generating a JWT with a `department` claim set to `finance`, we can send
it as part of a cURL command:

```bash{outputLines: 2-3}
curl \
  -H "Authorization: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkZXBhcnRtZW50IjoiZmluYW5jZSIsImV4cCI6MTY2NzMzNzI1MH0.njfL7GMDNlzKaJDZA0OQ_b2u2JhuSm-WjnS0yVfB8NA" \
  http://localhost:4000/cubejs-api/v1/meta
```

## Result

The `/meta` endpoint shows the available cubes and views:

```json
{
  "cubes": [
    {
      "name": "TotalRevenuePerCustomer",
      "title": "Total Revenue Per Customer",
      "description": "Total revenue per customer",
      "measures": [
        {
          "name": "TotalRevenuePerCustomer.totalRevenue",
          "title": "Total Revenue Per Customer Total Revenue",
          "shortTitle": "Total Revenue",
          "cumulativeTotal": false,
          "cumulative": false,
          "type": "number",
          "aggType": "number",
          "drillMembers": [],
          "drillMembersGrouped": {
            "measures": [],
            "dimensions": []
          },
          "isVisible": true
        }
      ],
      "dimensions": [
        {
          "name": "TotalRevenuePerCustomer.company",
          "title": "Total Revenue Per Customer Company",
          "type": "string",
          "shortTitle": "Company",
          "suggestFilterValues": true,
          "isVisible": true
        }
      ],
      "segments": []
    }
  ]
}
```

## Source code

Please feel free to check out the
[full source code](https://github.com/cube-js/cube.js/tree/master/examples/recipes/changing-visibility-of-cubes-or-views)
or run it with the `docker-compose up` command.
