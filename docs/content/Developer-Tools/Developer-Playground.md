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

![](https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Developer-Tools/playground.gif)

[link-dev-mode]: /configuration/overview

## Editing the Security Context

The Security Context used for requests can be modified by clicking the Edit
Security Context button at the top of the playground:

<div class="block-video" style="position: relative; padding-bottom: 56.25%; height: 0;">
  <iframe src="https://www.loom.com/embed/5307e973ad7e435094a31b7163f14f3d" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
</div>

You can paste in an existing JWT if desired; or create a brand-new one by
providing a JSON object that represents the decoded JWT.

## Running Playground in production

Developer playground is only enabled when `CUBEJS_DEV_MODE` is set to `true`.
Since Playground exposes data schema and admin access to all the possible
queries, we do not recommend running it on a production instance. You can use
[Cube.js frontend SDKs](/frontend-introduction) to build your own query builder
and use it to query your Cube.js API in a secure way.

You can also securely run Playground on top of the production Cube.js instance
inside the Cube Cloud.

<!-- prettier-ignore-start -->
[[info |]]
| [Cube Cloud][link-cube-cloud] currently is in early access. If you don't have
| an account yet, you can [sign up to the waitlist here][link-cube-cloud].
<!-- prettier-ignore-end -->

![](https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Developer-Tools/cube-cloud-playground.png)

[link-cube-cloud]: https://cube.dev/cloud
