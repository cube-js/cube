<p align="center">
  <a href="https://cube.dev"><img src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/cube-logo.png" alt="Cube — Headless Business Intelligence" width="300px"></a>
</p>

[Website](https://cube.dev) • [Getting Started](https://cube.dev/docs/getting-started) • [Docs](https://cube.dev/docs) • [Examples](https://cube.dev/docs/examples) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_shield)

__Cube is the headless business intelligence platform.__ It helps data engineers and application developers access data from modern data stores, organize it into consistent definitions, and deliver it to every application.

<img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/cube-scheme.png"
  style="border: none"
  width="100%"
/>

<p align="center">
  <i>Learn more about connecting Cube to <a href="https://cube.dev/docs/config/databases" target="_blank">data sources</a> and <a href="https://cube.dev/docs/config/downstream" target="_blank">analytics & visualization tools</a>.</i> 
</p>

Cube was designed to work with all SQL-enabled data sources, including cloud data warehouses like Snowflake or Google BigQuery, query engines like Presto or Amazon Athena, and application databases like Postgres. Cube has a built-in relational caching engine to provide sub-second latency and high concurrency for API requests.

For more details, see the [introduction](https://cube.dev/docs/cubejs-introduction) page in our documentation. 

## Why Cube?

If you are building a data application—such as a business intelligence tool or a customer-facing analytics feature—you’ll probably face the following problems:

1. __SQL code organization.__ Sooner or later, modeling even a dozen metrics with a dozen dimensions using pure SQL queries becomes a maintenance nightmare, which leads to building a modeling framework.
2. __Performance.__ Most of the time and effort in modern analytics software development is spent providing adequate time to insight. In a world where every company’s data is big data, writing just SQL queries to get insight isn’t enough anymore.
3. __Access Control.__ It is important to secure and govern access to data for all downstream data consuming applications.

Cube has the necessary infrastructure and features to implement efficient data modeling, access control, and performance optimizations so that every application—like embedded analytics, dashboarding and reporting tools, data notebooks, and other tools—can access consistent data via REST, SQL, and GraphQL APIs.

![](https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/old-was-vs-cubejs-way.png)

## Getting Started 🚀

The fastest way to try Cube locally is using [Docker](https://www.docker.com/). Once Docker is installed, in a new folder for your project, run the following command:

```bash
docker run -p 4000:4000 \
  -v ${PWD}:/cube/conf \
  -e CUBEJS_DEV_MODE=true \
  cubejs/cube
```


Then, open http://localhost:4000 in your browser to continue setup. You can learn more by following our [Getting started guide](https://cube.dev/docs/getting-started-docker).

Alternatively, if you have Node.js installed, you can run this command and follow the [Getting started guide](https://cube.dev/docs/getting-started).

```
$ npx cubejs-cli create hello-world
```

## Resources

- [Documentation](https://cube.dev/docs)
- [Getting Started](https://cube.dev/docs/getting-started)
- [Examples & Tutorials](https://cube.dev/docs/examples)
- [Architecture](https://cube.dev/docs/cubejs-introduction#architecture)

## Community

If you have any questions or need help - [please join our Slack community](https://slack.cube.dev) of amazing developers and data engineers.

You are also welcome to join our **monthly community calls** where we discuss community news, Cube Dev team's plans, backlogs, use cases, etc. If you miss the call, the recordings will also be available after the meeting. 
* When: Second Wednesday of each month at [9am Pacific Time](https://www.thetimezoneconverter.com/?t=09:00&tz=PT%20%28Pacific%20Time%29).  
* Meeting link: https://us02web.zoom.us/j/86717042169?pwd=VlBEd2VVK01DNDVVbU1EUXd5ajhsdz09
* Meeting [agenda/notes](https://www.notion.so/Notes-from-monthly-community-meetings-f394e5c131cb4bd1bc64ed850b0186d8). 
* Recordings will be posted on the [Community Playlist](https://www.youtube.com/playlist?list=PLtdXl_QTQjpb1dHZCM09qKTsgvgqjSvc9 ). 

### Contributing

There are many ways you can contribute to Cube! Here are a few possibilities:

* Star this repo and follow us on [Twitter](https://twitter.com/thecubejs).
* Add Cube to your stack on [Stackshare](https://stackshare.io/cube-js).
* Upvote issues with 👍 reaction so we know what's the demand for particular issue to prioritize it within road map.
* Create issues every time you feel something is missing or goes wrong.
* Ask questions on [Stack Overflow with cube.js tag](https://stackoverflow.com/questions/tagged/cube.js) if others can have these questions as well.
* Provide pull requests for all open issues and especially for those with [help wanted](https://github.com/cube-js/cube.js/issues?q=is%3Aissue+is%3Aopen+label%3A"help+wanted") and [good first issue](https://github.com/cube-js/cube.js/issues?q=is%3Aissue+is%3Aopen+label%3A"good+first+issue") labels.

All sort of contributions are **welcome and extremely helpful** 🙌 Please refer to [the contribution guide](https://github.com/cube-js/cube.js/blob/master/CONTRIBUTING.md) for more information.

## License

Cube Client is [MIT licensed](./packages/cubejs-client-core/LICENSE).

Cube Backend is [Apache 2.0 licensed](./packages/cubejs-server/LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_large)
