![]()
<p align="center">
  <a href="https://cube.dev?ref=github-readme"><img src="https://raw.githubusercontent.com/cube-js/cube/master/docs/content/cube-core-logo.png" alt="Cube Core — Open-Source Semantic Layer" width="300px"></a>
</p>
<br/>

[Website](https://cube.dev?ref=github-readme) • [Docs](https://cube.dev/docs?ref=github-readme) • [Examples](https://cube.dev/docs/examples?ref=github-readme) • [Blog](https://cube.dev/blog?ref=github-readme) • [Slack](https://slack.cube.dev?ref=github-readme) • [X](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube/workflows/Build/badge.svg)](https://github.com/cube-js/cube/actions?query=workflow%3ABuild+branch%3Amaster)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_shield)

__Cube Core is an open-source semantic layer.__ Cube Core can be used to build embedded analytics in your applications or create your own business intelligence tool. Cube Core is headless and comes with multiple APIs for embedded analytics and BI: REST, GraphQL, and SQL.

If you are looking for a fully integrated platform, check out [Cube](https://cube.dev), a modern AI-first business intelligence platform. We use Cube Core to power it.

<img
  src="https://lgo0ecceic.ucarecd.net/418db1f9-7597-4e00-8c10-eba19fcac20f/"
  style="border: none"
  width="100%"
/>

<p align="center">
  <i>Learn more about connecting Cube to <a href="https://cube.dev/docs/config/databases?ref=github-readme" target="_blank">data sources</a> and <a href="https://cube.dev/docs/config/downstream?ref=github-readme" target="_blank">analytics & visualization tools</a>.</i>
</p>

Cube Core was designed to work with all SQL data sources, including cloud data warehouses like Snowflake, Databricks, and BigQuery; query engines like Presto and Amazon Athena; and application databases like Postgres. Cube Core has a built-in relational caching engine to provide sub-second latency and high concurrency for API requests.

For more details, see the [introduction](https://cube.dev/docs/cubejs-introduction?ref=github-readme) page in our documentation.

## Why Cube Core?

Every business intelligence tool relies on a semantic layer as its core engine—a critical component that defines metrics, dimensions, and business logic while abstracting the complexity of underlying data sources. However, most semantic layers are proprietary, tightly coupled to specific BI platforms, and cannot be reused across different applications.

Cube Core is an open-source project that aims to create an open, modern semantic layer that can be used to power any analytics application, including business intelligence tools and embedded analytics. By decoupling the semantic layer from specific tools and making it accessible through standard APIs, Cube Core enables organizations to define their metrics once and use them everywhere—from custom dashboards to embedded analytics, from data exploration tools to automated reporting systems.

## Getting Started 🚀

You can get started with Cube locally or self-host it with [Docker](https://www.docker.com/).

Once Docker is installed, in a new folder for your project, run the following command:

```bash
docker run -p 4000:4000 \
  -p 15432:15432 \
  -v ${PWD}:/cube/conf \
  -e CUBEJS_DEV_MODE=true \
  cubejs/cube
```

Then, open http://localhost:4000 in your browser to continue setup.

For a step-by-step guide, [see the docs](https://cube.dev/docs/getting-started-docker?ref=github-readme).

### Cube — Complete Modern BI Tool from Cube Core Creators

[Cube](https://cube.dev?ref=github-readme) is a complete modern agentic analytics platform built on Cube Core. It provides a fully integrated solution with a user-friendly interface, advanced analytics capabilities, and managed infrastructure.

<a href="https://cubecloud.dev/auth/signup?ref=github-readme"><img src="https://cubedev-blog-images.s3.us-east-2.amazonaws.com/f1f1eac0-0b44-4c47-936e-33b5c06eedf0.png" alt="Get started now" width="200px"></a>

## Resources

- [Documentation](https://cube.dev/docs?ref=github-readme)
- [Getting Started](https://cube.dev/docs/getting-started?ref=github-readme)
- [Examples & Tutorials](https://cube.dev/docs/examples?ref=github-readme)
- [Architecture](https://cube.dev/docs/product/introduction#four-layers-of-semantic-layer)

## Contributing

There are many ways you can contribute to Cube Core! Here are a few possibilities:

* Star this repo and follow us on [X](https://twitter.com/the_cube_dev).
* Add Cube to your stack on [Stackshare](https://stackshare.io/cube-js).
* Upvote issues with 👍 reaction so we know what the demand is for particular issues to prioritize them within the roadmap.
* Create issues every time you feel something is missing or goes wrong.
* Ask questions on [Stack Overflow with cube.js tag](https://stackoverflow.com/questions/tagged/cube.js) if others might have these questions as well.
* Provide pull requests for all open issues and especially for those with [help wanted](https://github.com/cube-js/cube/issues?q=is%3Aissue+is%3Aopen+label%3A"help+wanted") and [good first issue](https://github.com/cube-js/cube/issues?q=is%3Aissue+is%3Aopen+label%3A"good+first+issue") labels.

All sorts of contributions are **welcome and extremely helpful** 🙌 Please refer to [the contribution guide](https://github.com/cube-js/cube/blob/master/CONTRIBUTING.md) for more information.

## License

Cube Client is [MIT licensed](./packages/cubejs-client-core/LICENSE).

Cube Backend is [Apache 2.0 licensed](./packages/cubejs-server/LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_large)
