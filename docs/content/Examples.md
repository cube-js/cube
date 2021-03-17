---
title: Examples & Tutorials
permalink: /examples
category: Examples & Tutorials
redirect_from:
  - /tutorials/
---

Below you can find examples and tutorials to help you get started with Cube.js.

<!-- prettier-ignore-start -->
[[info | ]]
| If you have any examples or tutorials that you'd like to contribute, we encourage you to create a new topic  
| in the [Cube.js community forum](https://forum.cube.dev/).
<!-- prettier-ignore-end -->

## Examples

| Demo                                            |                      Code                       | Description                                                                               |
| :---------------------------------------------- | :---------------------------------------------: | :---------------------------------------------------------------------------------------- |
| [Web Analytics][link-web-analytics]             |       [web-analytics][code-web-analytics]       | Web Analytics with AWS Athena, Snowplow, Cube.js backed by Cube Store                     |
| [Real-Time Dashboard][link-real-time-dashboard] | [real-time-dashboard][code-real-time-dashboard] | Real-Time Dashboard Demo using WebSockets transport                                       |
| [React Dashboard][link-react-dashboard]         |     [react-dashboard][code-react-dashboard]     | Dynamic dashboard with React, GraphQL, and Cube.js                                        |
| [D3 Dashboard][link-d3-dashboard]               |        [d3-dashboard][code-d3-dashboard]        | Dashboard with Cube.js, D3, and Material UI                                               |
| [Stripe Dashboard][link-stripe-dashboard]       |    [stripe-dashboard][code-stripe-dashboard]    | Stripe Demo Dashboard built with Cube.js and Recharts                                     |
| [Event Analytics][link-event-analytics]         |     [event-analytics][code-event-analytics]     | Mixpanel like Event Analytics App built with Cube.js and Snowplow                         |
| [External Rollups][link-external-rollups]       |    [external-rollups][code-external-rollups]    | Compare performance of direct BigQuery querying vs MySQL cached version for the same data |
| [AWS Web Analytics][link-aws-web-analytics]     |   [aws-web-analytics][code-aws-web-analytics]   | Web Analytics with AWS Lambda, Athena, Kinesis and Cube.js                                |
| Simple Dynamic Schema Creation                  | [async-module-simple][code-simple-asyncmodule]  | A simple example of using `asyncModule` to generate schemas                               |
| Auth0                                           |               [auth0][code-auth0]               | Cube.js deployment configured with Auth0 JWK/JWT integration                              |
| Cognito                                         |             [cognito][code-cognito]             | Cube.js deployment configured with AWS Cognito JWK/JWT integration                        |

[link-real-time-dashboard]: https://real-time-dashboard-demo.cube.dev/
[code-real-time-dashboard]:
  https://github.com/cube-js/cube.js/tree/master/examples/real-time-dashboard
[link-react-dashboard]: https://react-dashboard-demo.cube.dev/
[code-react-dashboard]:
  https://github.com/cube-js/cube.js/tree/master/guides/react-dashboard/demo
[link-d3-dashboard]: https://d3-dashboard-demo.cube.dev/
[code-d3-dashboard]:
  https://github.com/cube-js/cube.js/tree/master/examples/d3-dashboard
[link-stripe-dashboard]:
  http://cubejs-stripe-dashboard-example.s3-website-us-west-2.amazonaws.com/
[code-stripe-dashboard]:
  https://github.com/cube-js/cube.js/tree/master/examples/stripe-dashboard
[link-event-analytics]: https://d1ygcqhosay4lt.cloudfront.net/
[code-event-analytics]:
  https://github.com/cube-js/cube.js/tree/master/examples/event-analytics
[link-external-rollups]: https://external-rollups-demo.cube.dev/
[code-external-rollups]:
  https://github.com/cube-js/cube.js/tree/master/examples/external-rollups
[link-web-analytics]: https://web-analytics-demo.cube.dev/
[code-web-analytics]:
  https://github.com/cube-js/cube.js/tree/master/examples/web-analytics
[code-simple-asyncmodule]:
  https://github.com/cube-js/cube.js/tree/master/examples/async-module-simple
[code-auth0]: https://github.com/cube-js/cube.js/tree/master/examples/auth0
[code-cognito]: https://github.com/cube-js/cube.js/tree/master/examples/cognito

## Tutorials

### Getting Started Tutorials

These tutorials are a good place to start learning Cube.js.

- [Cube.js, the Open Source Dashboard Framework: Ultimate Guide ](https://cube.dev/blog/cubejs-open-source-dashboard-framework-ultimate-guide) -
  It is "**Cube.js 101**" style tutorial, which walks through the building a
  simple dashboard with React on the frontend.

- [Building MongoDB Dashboard using Node.js ](https://cube.dev/blog/building-mongodb-dashboard-using-node.js) -
  It is a great place to start if you are planning to use Cube.js with MongoDB.
  It covers the MongoDB Connector for BI and how to connect it to Cube.js.

- [Node Express Analytics Dashboard with Cube.js ](https://cube.dev/blog/node-express-analytics-dashboard-with-cubejs) -
  This tutorials shows how Cube.js could be embedded into an existing Express.js
  application.

### Advanced

- [Pre-Aggregations Tutorial](https://cube.dev/blog/high-performance-data-analytics-with-cubejs-pre-aggregations/) -
  Pre-Aggregations is one of the most powerful Cube.js features. By using it you
  can significantly speed up performance of your dashboards and reports. This
  tutorial is a good first step to master pre-aggregations.

- Building an Open Source Mixpanel Alternative - It's a series of tutorials on
  building a production ready application with Cube.js.

  - [Part 1: Collecting and Displaying Events](https://cube.dev/blog/building-an-open-source-mixpanel-alternative-1)
  - [Part 2: Conversion Funnels ](https://cube.dev/blog/building-open-source-mixpanel-alternative-2/)

- [React Query Builder with Cube.js ](https://cube.dev/blog/react-query-builder-with-cubejs) -
  It shows you how to build a dynamic query builder with Cube.js React Component.
