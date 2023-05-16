## Codefresh release life cycle

### Instructions
1. make sure to manually bump the version in package.json of all the packages that we are using otherwise it will fail to publish it during cd pipeline 

### CI
CI part will take place from github actions. and not from codefresh pipeline once a PR has been created

### CD
CD part will take place from codefresh pipeline and will only publish specific packages that were defined in the codefresh.yml

### how to add another new package in case we need
1. make change in the package.json of the specific required new package and rename the original name. take a look on how we did it for cubejs-bigquery-driver
2. add another step in the codefresh.yml so that the new package will be pushed to our npm

### What packages are we publishing atm
1. [server-core](https://github.com/codefresh-io/cube.js/blob/master/packages/cubejs-server-core/package.json), [@codefresh-io/cubejs-backend-server-core](https://www.npmjs.com/package/@codefresh-io/cubejs-backend-server-core)
2. [cbuejs-bigquery-driver](https://github.com/codefresh-io/cube.js/tree/master/packages/cubejs-bigquery-driver), [@codefresh-io/cubejs-backend-bigquery-driver](https://www.npmjs.com/package/@codefresh-io/cubejs-backend-bigquery-driver)
3. [cubejs-postgres-driver](https://github.com/codefresh-io/cube.js/tree/master/packages/cubejs-postgres-driver), [@codefresh-io/cubejs-backend-postgres-driver
   ](https://www.npmjs.com/package/@codefresh-io/cubejs-backend-postgres-driver)

### How to use packages 
1. We need to keep real names of packages when using the published packages, so since we are publishing with different name, other places in cube js code aren't aware of this name and this causing issues on runtime.
2. a workaround for that will be to add the dependency in the service which consume it(For example platform-analytics) - with this syntax:
`"@cubejs-backend/bigquery-driver": "npm:@codefresh-io/cubejs-backend-bigquery-driver@0.30.81"
`3. this will resolve in platform analytics from our fork but will resolve it correctly in cube-js code.
see examples in platform-analytics package.json file

<p align="center">
  <a href="https://cube.dev?ref=github-readme"><img src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/cube-logo.png" alt="Cube — Headless Business Intelligence" width="300px"></a>
</p>

[Website](https://cube.dev?ref=github-readme) • [Getting Started](https://cube.dev/docs/getting-started?ref=github-readme) • [Docs](https://cube.dev/docs?ref=github-readme) • [Examples](https://cube.dev/docs/examples?ref=github-readme) • [Blog](https://cube.dev/blog?ref=github-readme) • [Slack](https://slack.cube.dev?ref=github-readme) • [Discourse](https://forum.cube.dev/) • [Twitter](https://twitter.com/thecubejs)

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
  <i>Learn more about connecting Cube to <a href="https://cube.dev/docs/config/databases?ref=github-readme" target="_blank">data sources</a> and <a href="https://cube.dev/docs/config/downstream?ref=github-readme" target="_blank">analytics & visualization tools</a>.</i> 
</p>

Cube was designed to work with all SQL-enabled data sources, including cloud data warehouses like Snowflake or Google BigQuery, query engines like Presto or Amazon Athena, and application databases like Postgres. Cube has a built-in relational caching engine to provide sub-second latency and high concurrency for API requests.

For more details, see the [introduction](https://cube.dev/docs/cubejs-introduction?ref=github-readme) page in our documentation. 

## Why Cube?

If you are building a data application—such as a business intelligence tool or a customer-facing analytics feature—you’ll probably face the following problems:

1. __SQL code organization.__ Sooner or later, modeling even a dozen metrics with a dozen dimensions using pure SQL queries becomes a maintenance nightmare, which leads to building a modeling framework.
2. __Performance.__ Most of the time and effort in modern analytics software development is spent providing adequate time to insight. In a world where every company’s data is big data, writing just SQL queries to get insight isn’t enough anymore.
3. __Access Control.__ It is important to secure and govern access to data for all downstream data consuming applications.

Cube has the necessary infrastructure and features to implement efficient data modeling, access control, and performance optimizations so that every application—like embedded analytics, dashboarding and reporting tools, data notebooks, and other tools—can access consistent data via REST, SQL, and GraphQL APIs.

![](https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/old-was-vs-cubejs-way.png)

## Getting Started 🚀

### Cube Cloud

[Cube Cloud](https://cube.dev/cloud?ref=github-readme) is the fastest way to get started with Cube. It provides managed infrastructure as well as an instant and free access for development projects and proofs of concept.

<a href="https://cubecloud.dev/auth/signup?ref=github-readme"><img src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/f1f1eac0-0b44-4c47-936e-33b5c06eedf0.png" alt="Get started now" width="200px"></a>

For a step-by-step guide on Cube Cloud, [see the docs](https://cube.dev/docs/cloud/getting-started/create?ref=github-readme).

### Docker

Alternatively, you can get started with Cube locally or self-host it with [Docker](https://www.docker.com/).

Once Docker is installed, in a new folder for your project, run the following command:

```bash
docker run -p 4000:4000 \
  -v ${PWD}:/cube/conf \
  -e CUBEJS_DEV_MODE=true \
  cubejs/cube
```

Then, open http://localhost:4000 in your browser to continue setup.

For a step-by-step guide on Docker, [see the docs](https://cube.dev/docs/getting-started-docker?ref=github-readme).

## Resources

- [Documentation](https://cube.dev/docs?ref=github-readme)
- [Getting Started](https://cube.dev/docs/getting-started?ref=github-readme)
- [Examples & Tutorials](https://cube.dev/docs/examples?ref=github-readme)
- [Architecture](https://cube.dev/docs/cubejs-introduction?ref=github-readme#architecture)

## Community

If you have any questions or need help - [please join our Slack community](https://slack.cube.dev?ref=github-readme) of amazing developers and data engineers.

You are also welcome to join our **monthly community calls** where we discuss community news, Cube Dev team's plans, backlogs, use cases, etc. If you miss the call, the recordings will also be available after the meeting. 
* When: Second Wednesday of each month at [9am Pacific Time](https://www.thetimezoneconverter.com/?t=09:00&tz=PT%20%28Pacific%20Time%29).  
* Meeting link: https://us02web.zoom.us/j/86717042169?pwd=VlBEd2VVK01DNDVVbU1EUXd5ajhsdz09
* [Meeting page](https://cube.dev/community-call/). 
* Recordings will be posted on the [Community Call Playlist](https://www.youtube.com/playlist?list=PLtdXl_QTQjpb1dHZCM09qKTsgvgqjSvc9). 

### Our quarterly roadmap

We publish our open source roadmap every quarter and discuss them during our [monthly community calls](https://cube.dev/community-call/). You can find our roadmap under [projects in our Cube.js repository](https://github.com/cube-js/cube.js/projects?query=is%3Aopen+sort%3Aupdated-desc). 

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
