---
title: Developer Playground
permalink: /dev-tools/dev-playground
category: Developer Tools
menuOrder: 3
---

Developer Playground is a web-based tool which helps to generate and view the
data schema, build and execute queries, plot the results, and generate dashboard
applications from a variety of templates for different frontend frameworks and
data visualization libraries.

Developer Playground is available on `http://localhost:4000` when Cube.js is run
in [development mode][link-dev-mode]

Here's an example of building a query and plotting the results in Developer
Playground:

![](https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/content/Developer-Tools/playground.gif)

[link-dev-mode]: /configuration/overview

## Running Playground in production

Developer playground is only enabled when `CUBEJS_DEV_MODE` is set to `true`. Since Playground exposes data schema and admin access to all the possible queries we do not recommend running it on production instance. You can use [Cube.js frontend SDKs](/frontend-introduction) to build your own query builder and use it to query your Cube.js API in a secure way.

You can also securely run Playground on top of the production Cube.js instance inside the Cube Cloud.

[[info |]]
| [Cube Cloud][link-cube-cloud] currently is in early access. If you don't have
| an account yet, you can [sign up to the waitlist here][link-cube-cloud].

![](https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/content/Developer-Tools/cube-cloud-playground.png)

[link-cube-cloud]: https://cube.dev/cloud
