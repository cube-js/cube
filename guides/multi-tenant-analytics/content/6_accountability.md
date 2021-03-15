---
order: 6
title: "Step 4. Accountability with audit logs"
---

As we know, the essence of accountability is being able to understand what actions were performed by different users.

Usually, logs are used for that purpose. When and where to write the logs? Obviously, we should do that for every (critical) access to the data. Cube.js provides the [queryTransformer](https://cube.dev/docs/config?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics#options-reference-query-transformer), a great extension point for that purpose. The code in the `queryTransformer` runs for every query *before it's processed*. It means that you can not only write logs but also modify the queries, e.g., add filters and implement multi-tenant access control.

To write logs for every query, update the `cube.js` file as follows:

```js
// Cube.js configuration options: https://cube.dev/docs/config
module.exports = {
  queryTransformer: (query, { securityContext }) => {
    const { role, email } = securityContext;
    if (role === 'admin') {
      console.log(`User ${email} with role ${role} executed: ${JSON.stringify(query)}`);
    }
    return query;
  },
};
```

After that, stop Cube.js (by pressing `CTRL+C`), run it again with `npm run dev`, and refresh the web application. In the console, you'll see the output like this:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/7s5jw768n8jg2lqovlvg.png)

Surely you can use a more sophisticated logger, e.g., a cloud-based logging solution such as [Datadog](https://www.datadoghq.com).

‼️ **With minimal changes, we were able to add accountability to our app via a convenient Cube.js extension point.** Moreover, now we have everything from IAAA implemented in our app: identification, authentication, authorization, accountability. JSON Web Tokens are generated and passed to the API, role-based access control is implemented, and an external authentication provider controls how users sign in. With all these, multi-tenancy is only one line of code away and can be implemented in minutes.

**And that's all, friends!** 🤿 I hope you liked this guide 🤗

Here are just a few things you can do in the end:
* go to the [Cube.js repo](https://github.com/cube-js/cube.js/) on GitHub and give it a star ⭐️
* share a link to this guide on Twitter, Reddit, or with a friend 🙋‍♀️
* share your insights, feedback, and what you've learned about security, IAAA, Auth0, and Cube.js in the comments below ↓

P.S. I'd like to thank Aphyr for the [inspiration](https://youtu.be/eSaFVX4izsQ?t=20) for the fake "George Orwell" quote at the beginning of this guide.