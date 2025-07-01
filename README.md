<p align="center">
  <a href="https://cube.dev?ref=github-readme"><img src="https://raw.githubusercontent.com/cube-js/cube/master/docs/content/cube-logo-with-bg.png" alt="Cube — Semantic Layer for Data Applications" width="300px"></a>
</p>
<br/>

[Website](https://cube.dev?ref=github-readme) • [Getting Started](https://cube.dev/docs/getting-started?ref=github-readme) • [Docs](https://cube.dev/docs?ref=github-readme) • [Examples](https://cube.dev/docs/examples?ref=github-readme) • [Blog](https://cube.dev/blog?ref=github-readme) • [Slack](https://slack.cube.dev?ref=github-readme) • [X](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube/workflows/Build/badge.svg)](https://github.com/cube-js/cube/actions?query=workflow%3ABuild+branch%3Amaster)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_shield)

__Cube is the universal semantic layer for modern data applications.__ Born in the cloud era, Cube represents the next evolution of OLAP technology, helping data engineers and application developers access data from modern data stores, organize it into consistent definitions, and deliver it to every application.

<img
  src="https://ucarecdn.com/8d945f29-e9eb-4e7f-9e9e-29ae7074e195/"
  style="border: none"
  width="100%"
/>

<p align="center">
  <i>Learn more about connecting Cube to <a href="https://cube.dev/docs/config/databases?ref=github-readme" target="_blank">data sources</a> and <a href="https://cube.dev/docs/config/downstream?ref=github-readme" target="_blank">analytics & visualization tools</a>.</i>
</p>

Cube was designed to work with all SQL-enabled data sources, including cloud data warehouses like Snowflake or Google BigQuery, query engines like Presto or Amazon Athena, and application databases like Postgres. Cube has a built-in relational caching engine to provide sub-second latency and high concurrency for API requests.

For more details, see the [introduction](https://cube.dev/docs/cubejs-introduction?ref=github-readme) page in our documentation.

## Why Cube?

As data infrastructure evolved from traditional relational databases to cloud data platforms, OLAP capabilities that once lived in specialized servers like SQL Server Analysis Services and Oracle Essbase were left behind. Today's organizations face several challenges:

1. __Analytics Modeling and Multidimensionality.__ Modern cloud data platforms excel at processing large volumes of data but lack native support for multidimensional analysis and modeling. Cube brings OLAP-style analytics to these platforms, enabling consistent metric definitions and multidimensional analysis.

2. __Performance Optimization.__ While cloud data warehouses have improved query performance through column-oriented storage and distributed processing, they still struggle with complex analytical workloads. Cube provides intelligent caching and pre-aggregation strategies that dramatically improve query response times.

3. __Access Control and Governance.__ Securing and governing access to data across all consuming applications remains critical. Cube offers robust access control to ensure consistent security across your entire data ecosystem.

4. __API Flexibility.__ Legacy OLAP tools were limited in how they exposed data. Cube provides modern REST, GraphQL, and SQL APIs along with support for traditional MDX and DAX interfaces, making it a truly universal semantic layer.

Cube is the missing OLAP engine for the cloud data platform era that provides the necessary infrastructure and features to implement efficient data modeling, access control, and performance optimizations without duplicating analytics modeling, data, or security permissions across different tools.

![](https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/old-was-vs-cubejs-way.png)

## Getting Started 🚀

### Cube Cloud

[Cube Cloud](https://cube.dev/cloud?ref=github-readme) is the fastest way to get started with Cube. It provides managed infrastructure as well as an instant and free access for development projects and proofs of concept.

<a href="https://cubecloud.dev/auth/signup?ref=github-readme"><img src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/f1f1eac0-0b44-4c47-936e-33b5c06eedf0.png" alt="Get started now" width="200px"></a>

For a step-by-step guide on Cube Cloud, [see the docs](https://cube.dev/docs/getting-started/cloud/overview?ref=github-readme).

### Docker

Alternatively, you can get started with Cube locally or self-host it with [Docker](https://www.docker.com/).

Once Docker is installed, in a new folder for your project, run the following command:

```bash
docker run -p 4000:4000 \
  -p 15432:15432 \
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
- [Architecture](https://cube.dev/docs/product/introduction#four-layers-of-semantic-layer)

## Contributing

There are many ways you can contribute to Cube! Here are a few possibilities:

* Star this repo and follow us on [X](https://twitter.com/the_cube_dev).
* Add Cube to your stack on [Stackshare](https://stackshare.io/cube-js).
* Upvote issues with 👍 reaction so we know what's the demand for particular issue to prioritize it within road map.
* Create issues every time you feel something is missing or goes wrong.
* Ask questions on [Stack Overflow with cube.js tag](https://stackoverflow.com/questions/tagged/cube.js) if others can have these questions as well.
* Provide pull requests for all open issues and especially for those with [help wanted](https://github.com/cube-js/cube/issues?q=is%3Aissue+is%3Aopen+label%3A"help+wanted") and [good first issue](https://github.com/cube-js/cube/issues?q=is%3Aissue+is%3Aopen+label%3A"good+first+issue") labels.

All sort of contributions are **welcome and extremely helpful** 🙌 Please refer to [the contribution guide](https://github.com/cube-js/cube/blob/master/CONTRIBUTING.md) for more information.

## License

Cube Client is [MIT licensed](./packages/cubejs-client-core/LICENSE).

Cube Backend is [Apache 2.0 licensed](./packages/cubejs-server/LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_large)
