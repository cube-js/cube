---
order: 4
title: "Step 2. Authorization with JWTs"
---

As we already know, the essence of authorization is letting users perform certain actions based on who they are. How do we achieve that?

We can make decisions about actions that users are permitted to perform based on the additional information (or *claims*) in their JWTs. Do you remember that, while generating the JWT, we've supplied the payload of `role=admin`? We're going to make the API use that payload to permit or restrict users' actions.

Cube.js allows you to access the payload of JWTs through the [security context](https://cube.dev/docs/security/context?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics). You can use the security context to modify the [data schema](https://cube.dev/docs/getting-started-cubejs-schema?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) or support [multi-tenancy](https://cube.dev/docs/multitenancy-setup?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics).

**First, let's update the data schema.** In the `schema/Orders.js` file, you can find the following code:

```js
cube(`Orders`, {
  sql: `SELECT * FROM public.orders`,

  // ...
```

This SQL statement says that any query to this cube operates with all rows in the `public.orders` table. Let's say that we want to change it as follows:
* "admin" users can access all data
* "non-admin" users can access only a subset of all data, e.g., just 10 %

To achieve that, let's update the `schema/Orders.js` file as follows:

```js
cube(`Orders`, {
  sql: `SELECT * FROM public.orders ${SECURITY_CONTEXT.role.unsafeValue() !== 'admin' ? 'WHERE id % 10 = FLOOR(RANDOM() * 10)' : ''}`,

  // ...
```

What happens here? Let's break it down:
* `SECURITY_CONTEXT.role` allows us to access the value of the "role" field of the payload. With `SECURITY_CONTEXT.role.unsafeValue()` we can directly use the value in the JavaScript code and modify the SQL statement. In this snippet, we check that the role isn't equal to the "admin" value, meaning that a "non-admin" user sent a query.
* In this case, we're appending a new `WHERE` SQL statement where we compare the value of `id % 10` (which is the remainder of the numeric id of the row divided by 10) and the value of `FLOOR(RANDOM() * 10)` (which is a pseudo-random number in the range of `0..9`). Effectively, it means that a "non-admin" user will be able to query a 1/10 of all data, and as the value returned by `RANDOM()` changes, the subset will change as well.
* You can also directly check the values in the payload against columns in the table with `filter` and `requiredFilter`. See data schema [documentation](https://cube.dev/docs/cube?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics#context-variables-security-context) for details.

**Second, let's check how the updated schema restricts certain actions.** Guess what will happen if you update the schema, stop Cube.js (by pressing `CTRL+C`), run Cube.js again with `npm run dev`, then reload our web application.

Right, nothing! 🙀 We're still using the JWT with `role=admin` as the payload, so we can access all the data. So, how to test that the updated data schema works?

Let's generate a new token without the payload or with another role with `npx cubejs-cli token --secret="NEW_SECRET" --payload="role=foobar"`, update the `dashboard-app/src/App.js` file, and reload our web application once again. Wow, now it's something... certainly less than before:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/0lzf3algridqw3wurhdh.png)

**Third, let's check the same via the console.** As before, we can run the following command with an updated JWT:

```sh
curl http://localhost:4000/cubejs-api/v1/load \
  -H 'Authorization: eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJyb2xlIjoiZm9vYmFyIiwiaWF0IjoxNjE1MTk0MTIwLCJleHAiOjE2MTUxOTc3NjEsImp0aSI6ImMxYTk2NTY1LTUzNzEtNDNlOS05MDg0LTk0NWY3ZTI3ZDJlZSJ9.FSdEweetjeT9GJsqRqEebHLtoa5dVkIgWX4T03Y7Azg' \
  -G -s --data-urlencode 'query={"measures": ["Orders.count"], "dimensions": ["Orders.status"]}' \
  | jq '.data'
```

Works like a charm:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/k5dbxy79i268dxdcbwsk.png)

Cube.js also provides convenient extension points to use security context for [multi-tenancy support](https://cube.dev/docs/multitenancy-setup?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics). In the most frequent scenario, you'll use the `queryTransformer` to add mandatory [tenant-aware filters](https://cube.dev/docs/multitenancy-setup?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics#same-db-instance-with-per-tenant-row-level-security) to every query. However, you also can switch databases, their schemas, and cache configuration based on the security context.

‼️ **We were able to add authorization and use JWT claims to control the access to data.** Now the API is aware of users' roles. However, right now the only JWT is hardcoded into the web application and shared between all users.

To automate the way JWTs are issued for each user, we'll need to use an external authentication provider. Let's proceed to the next step and add identification 🤿