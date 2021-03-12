---
order: 1
title: "Security... Why bother?"
---

*TL;DR: In this guide, we'll learn how to secure web applications with industry-standard and proven authentication mechanisms such as JSON Web Tokens, JSON Web Keys, OAuth 2.0 protocol.*

*We'll start with an openly accessible, insecure analytical app and walk through a series of steps to turn it into a secure, multi-tenant app with role-based access control and an external authentication provider. We'll use [Cube.js](https://cube.dev?utm_source=dev-to&utm_medium=post&utm_campaign=multi-tenant-analytics) to build an analytical app and Auth0 to authenticate users.*

"Why bother with security", that's a fair question! As a renowned security practitioner George Orwell coined, "All users are equal, but some users are more equal than others."

Usually, the need to secure an application is rooted in a premise that some users should be allowed to do more things than others: access an app, read or update data, invite other users, etc. To satisfy this need, an app should implement [IAAA](https://www.mayurpahwa.com/2018/06/identification-authentication.html), i.e., it should be able to perform:

* **Identification.** Ask users "Who are you?"
* **Authentication.** Check that users really are who they claim to be
* **Authorization.** Let users perform certain actions based on who they are
* **Accountability.** Keep records of users' actions for future review

In this guide, we'll go through a series of simple, comprehensible steps to secure a web app, implement IAAA, and user industry-standard mechanisms:

* **Step 0.** Bootstrap an openly accessible analytical app with Cube.js
* **Step 1.** Add *authentication* with signed and encrypted JSON Web Tokens
* **Step 2.** Add *authorization*, multi-tenancy, and role-based access control with security claims which are stored in JSON Web Tokens
* **Step 3.** Add *identification* via an external provider with Auth0 and use JSON Web Keys to validate JSON Web Tokens
* **Step 4.** Add *accountability* with audit logs
* **Step 5.** Feel great about building a secure app 😎

**Also, here's the [live demo](https://multi-tenant-analytics-demo.cube.dev) you can try right away.** It looks and feels exactly like the app we're going to build., i.e., it lets you authenticate with Auth0 and query an analytical API. And as you expected, the source code is on [GitHub](https://github.com/cube-js/cube.js/tree/master/examples/multi-tenant-analytics).

Okay, let's dive in — and don't forget to wear a mask! 🤿