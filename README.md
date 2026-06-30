![]()
<p align="center">
  <a href="https://cube.dev?ref=github-readme"><img src="https://raw.githubusercontent.com/cube-js/cube/master/docs/content/cube-core-logo.png" alt="Cube Core — Open-Source Semantic Layer" width="300px"></a>
</p>
<br/>

[Website](https://cube.dev?ref=github-readme) • [Docs](https://cube.dev/docs?ref=github-readme) • [Examples](https://cube.dev/docs/examples?ref=github-readme) • [Blog](https://cube.dev/blog?ref=github-readme) • [Slack](https://slack.cube.dev?ref=github-readme) • [X](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube/workflows/Build/badge.svg)](https://github.com/cube-js/cube/actions?query=workflow%3ABuild+branch%3Amaster)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_shield)

__Cube Core is the open-source semantic layer.__ Define metrics, dimensions, joins, and access rules once in code, then expose them through SQL, REST, and GraphQL APIs to anything downstream — BI tools, custom applications, or AI agents. Cube Core is headless: it doesn't ship a UI, so you can build the analytics experience that fits your product.

Cube Core works with all SQL data sources, including cloud data warehouses like Snowflake, Databricks, and BigQuery; query engines like Presto and Amazon Athena; and application databases like Postgres. It has a built-in relational caching engine to provide sub-second latency and high concurrency for API requests.

<img
  src="https://lgo0ecceic.ucarecd.net/418db1f9-7597-4e00-8c10-eba19fcac20f/"
  style="border: none"
  width="100%"
/>

<p align="center">
  <i>Learn more about connecting Cube to <a href="https://cube.dev/cube-core/getting-started/create-a-project?ref=github-readme" target="_blank">data sources</a> and <a href="https://cube.dev/docs/integrations?ref=github-readme" target="_blank">analytics & visualization tools</a>.</i>
</p>

## Why Cube Core?

Every BI tool relies on a semantic layer as its core engine — the component that defines metrics, dimensions, and business logic and hides the complexity of the underlying data sources. Most semantic layers are proprietary, tightly coupled to a single BI platform, and can't be reused across other tools.

Cube Core is an open, standalone semantic layer that any analytics application or AI agent can consume through standard APIs. Define your metrics once and use them everywhere — internal BI, embedded analytics, AI agents — without re-implementing the model in each place.

## Getting Started

You can run Cube Core locally or self-host it with [Docker](https://www.docker.com/).

Once Docker is installed, in a new folder for your project, run:

```bash
docker run -p 4000:4000 \
  -p 15432:15432 \
  -v ${PWD}:/cube/conf \
  -e CUBEJS_DEV_MODE=true \
  cubejs/cube
```

Then open http://localhost:4000 in your browser to continue setup.

For a step-by-step guide, [see the docs](https://cube.dev/cube-core/getting-started/create-a-project?ref=github-readme).

## Cube Core vs. Cube

[Cube](https://cube.dev?ref=github-readme) is our commercial product — an agentic analytics platform built on Cube Core. Same semantic layer underneath, plus the rest of what makes it a full BI platform: Analytics Chat, workbooks and dashboards, embedded analytics surfaces, managed deployment, RBAC, multi-tenancy, and integrations with Tableau, Power BI, Excel, and Google Sheets.

The data model is fully compatible both ways: a model you build in Cube Core runs unchanged in Cube, and vice versa. Cube Core stays open-source and is what we run inside Cube ourselves.

- **Use Cube Core** when you want to own the stack — a custom BI experience, deeply integrated embedded analytics, or AI agents that need a governed semantic foundation.
- **Use Cube** when you want a managed, full-featured BI platform out of the box — internal analytics or customer-facing embedded analytics without building the surrounding platform yourself.

For more on how we think about the split, see [The Future of Cube Core and Cube](https://cube.dev/blog/cube-core-and-cube).

For a tour of what's in Cube today, watch the workshop:

<a href="https://www.youtube.com/watch?v=7ZQGGepDjUQ" target="_blank">
  <img src="https://img.youtube.com/vi/7ZQGGepDjUQ/maxresdefault.jpg" alt="Cube agentic analytics workshop on YouTube" width="600">
</a>

Or [try Cube for free](https://cubecloud.dev/auth/signup?ref=github-readme).

## Resources

- [Documentation](https://cube.dev/docs?ref=github-readme)
- [Getting Started](https://cube.dev/cube-core/getting-started?ref=github-readme)
- [Examples & Tutorials](https://cube.dev/recipes?ref=github-readme)
- [Architecture](https://cube.dev/docs/introduction)

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
