---
title: Role-Based Access
permalink: /recipes/role-based-access
category: Examples & Tutorials
subCategory: Access control
menuOrder: 2
---

## Use case

We want to manage user access to different data depending on their role. In the
recipe below, a user with the `operator` role can only view processing orders
from a shop and a `manager` can only view shipped and completed orders.

## Configuration

To implement role-based access, we will use a
[JSON Web Token](https://cube.dev/docs/security) with role information in the
payload, and the
[`queryRewrite`](https://cube.dev/docs/security/context#using-query-rewrite)
extension point to manage data access.

Let's add the role verification in the `cube.js` file.

<GitHubCodeBlock
  href="https://github.com/cube-js/cube.js/blob/master/examples/recipes/role-based-access/cube.js"
  titleSuffixCount={2}
  part="productsRollup"
  lang="js"
/>

## Query

To get the number of orders as a manager or operator, we will send two identical
requests with different JWTs:

```javascript
{
  "iat": 1000000000,
  "exp": 5000000000,
  "role": "manager"
}
```

```javascript
{
  "iat": 1000000000,
  "exp": 5000000000,
  "role": "operator"
}
```

## Result

We have received different data depending on the user's role.

Manager's data: 

<CubeQueryResultSet
api="https://maroon-lemming.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1"
token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoibWFuYWdlciIsImlhdCI6MTAwMDAwMDAwMCwiZXhwIjo1MDAwMDAwMDAwfQ.3n17t_lTumC7Bc4uT7jrPjZMiGQ0rpfyy6fKil9WcC8"
query={{
    "dimensions": [
        "Orders.status"
    ],
    "order": {
        "Orders.count": "desc"
    },
    "measures": [
        "Orders.count"
    ]
}} />

Operator's data: 

<CubeQueryResultSet
api="https://maroon-lemming.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1"
token="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlIjoib3BlcmF0b3IiLCJpYXQiOjEwMDAwMDAwMDAsImV4cCI6NTAwMDAwMDAwMH0.8LH7yCpWZ8wnaetJLJVVR6OYQzIGf8B4jdaOpbO9WsM"
query={{
    "dimensions": [
        "Orders.status"
    ],
    "order": {
        "Orders.count": "desc"
    },
    "measures": [
        "Orders.count"
    ]
}} />

## Source code

Please feel free to check out the full source code or run it with the
`docker-compose up` command. You'll see the result, including queried data, in
the console.

<GitHubFolderLink
  href="https://github.com/cube-js/cube.js/blob/master/examples/recipes/role-based-access"
/>
