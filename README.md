<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Getting Started](https://cube.dev/docs/getting-started) • [Docs](https://cube.dev/docs) • [Examples](https://cube.dev/docs/examples) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_shield)
[![Codecov](https://codecov.io/gh/cube-js/cube.js/branch/master/graph/badge.svg)](https://codecov.io/gh/cube-js/cube.js)

__Cube.js is an open-source analytical API platform.__ It is primarily used to build internal business intelligence tools or add customer-facing analytics to existing applications.

Cube.js was designed to work with serverless data warehouses and query engines like Google BigQuery and AWS Athena. A multi-stage querying approach makes it suitable for handling
trillions of data points. Most modern RDBMS work with Cube.js as well and can be
further tuned for performance.

For more details, see the [Cube.js introduction](https://cube.dev/docs/cubejs-introduction) page in our documentation.

## Why Cube.js?

If you are building your own business intelligence tool or customer-facing analytics most probably you'll face the following problems:

1. __Performance.__ Most of effort time in modern analytics software development is spent to provide adequate time to insight. In the world where every company data is a big data writing just SQL query to get insight isn't enough anymore.
2. __SQL code organization.__ Modelling even a dozen of metrics with a dozen of dimensions using pure SQL queries sooner or later becomes a maintenance nightmare which ends up in building modelling framework.
3. __Infrastructure.__ Key components every production-ready analytics solution requires: analytic SQL generation, query results caching and execution orchestration, data pre-aggregation, security, API for query results fetch, and visualization.

Cube.js has necessary infrastructure for every analytic application that heavily relies on its caching and pre-aggregation layer to provide several minutes raw data to insight delay and sub second API response times on a trillion of data points scale.

![](https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/old-was-vs-cubejs-way.png)

## Getting Started 🚀

There're two ways to create a Cube.js application.

If you have Node.js installed, run this command and follow with the [Getting Started](https://cube.dev/docs/getting-started) guide.

```
$ npx cubejs-cli create hello-world
```

In case you don't have Node.js, or you'd like to run Cube.js with Docker, have a look at the [Getting started with Docker](https://cube.dev/docs/getting-started-docker) guide.

## Resources

- [Documentation](https://cube.dev/docs)
- [Getting Started](https://cube.dev/docs/getting-started)
- [Examples & Tutorials](https://cube.dev/docs/examples)
- [Architecture](https://cube.dev/docs/cubejs-introduction#architecture)

## Community

If you have any questions or need help - [please join our Slack community](https://slack.cube.dev) of amazing developers and contributors.

You are also welcome to join our **monthly community calls** where we discuss community news, Cube Dev team's plans, backlogs, use cases, etc. If you miss the call, the recordings will also be available after the meeting.
* When: Second Wednesday of each month at [9am Pacific Time](https://www.thetimezoneconverter.com/?t=09:00&tz=PT%20%28Pacific%20Time%29).
* Meeting link: https://us02web.zoom.us/j/86717042169?pwd=VlBEd2VVK01DNDVVbU1EUXd5ajhsdz09
* Meeting [agenda/notes](https://www.notion.so/Notes-from-monthly-community-meetings-f394e5c131cb4bd1bc64ed850b0186d8).
* Recordings will be posted on the [Community Playlist](https://www.youtube.com/playlist?list=PLtdXl_QTQjpb1dHZCM09qKTsgvgqjSvc9 ).

### Contributing

There are many ways you can contribute and help Cube.js. Here a few ones:

* Star this repo and follow us on [Twitter](https://twitter.com/thecubejs).
* Add Cube.js to your stack on [Stackshare](https://stackshare.io/cube-js).
* Upvote issues with 👍 reaction so we know what's the demand for particular issue to prioritize it within road map.
* Create issues every time you feel something is missing or goes wrong.
* Ask questions on [Stack Overflow with cube.js tag](https://stackoverflow.com/questions/tagged/cube.js) if others can have these questions as well.
* Provide pull requests for all open issues and especially for those with [help wanted](https://github.com/cube-js/cube.js/issues?q=is%3Aissue+is%3Aopen+label%3A"help+wanted") and [good first issue](https://github.com/cube-js/cube.js/issues?q=is%3Aissue+is%3Aopen+label%3A"good+first+issue") labels as those are out of Cube.js Core Team roadmap scope.

All sort of contributions are **welcome and extremely helpful** 🙌 Please refer to [the contribution guide](https://github.com/cube-js/cube.js/blob/master/CONTRIBUTING.md) for more information.

## License

Cube.js Client is [MIT licensed](./packages/cubejs-client-core/LICENSE).

Cube.js Backend is [Apache 2.0 licensed](./packages/cubejs-server/LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_large)
