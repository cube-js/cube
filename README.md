# Gorgias Fork of Cube
This fork was made to be able to build our own cubestore image with StatsD metrics fixes for [this issue](https://github.com/cube-js/cube/issues/9920).

The CI will build and push the image for the `gorgisa` branch but only build the image for testing for other branches.

Locall, build and tag the image with:

```bash
docker build -t us-east1-docker.pkg.dev/gorgias-helpdesk-staging/container-images/cubestore:v0.35.80-gorgias -f rust/cubestore/gorgias.Dockerfile rust/cubestore

docker tag us-east1-docker.pkg.dev/gorgias-helpdesk-staging/container-images/cubestore:v0.35.80-gorgias \
  us-central1-docker.pkg.dev/gorgias-revenue-staging/container-images/cubestore:v0.35.80-gorgias
docker tag us-east1-docker.pkg.dev/gorgias-helpdesk-staging/container-images/cubestore:v0.35.80-gorgias \
  us-east1-docker.pkg.dev/gorgias-helpdesk-production/container-images/cubestore:v0.35.80-gorgias
docker tag us-east1-docker.pkg.dev/gorgias-helpdesk-staging/container-images/cubestore:v0.35.80-gorgias \
  us-central1-docker.pkg.dev/gorgias-revenue-production/container-images/cubestore:v0.35.80-gorgias
```

Then push the image to the registry with:

```
# Setup docker credentials if needed first
gcloud auth configure-docker us-east1-docker.pkg.dev --quiet
gcloud auth configure-docker us-central1-docker.pkg.dev --quiet

docker push us-east1-docker.pkg.dev/gorgias-helpdesk-staging/container-images/cubestore:v0.35.80-gorgias
docker push us-central1-docker.pkg.dev/gorgias-revenue-staging/container-images/cubestore:v0.35.80-gorgias
docker push us-east1-docker.pkg.dev/gorgias-helpdesk-production/container-images/cubestore:v0.35.80-gorgias
docker push us-central1-docker.pkg.dev/gorgias-revenue-production/container-images/cubestore:v0.35.80-gorgias

```

## How to upgrade versions and build the image

1. Update the gorgias branch to be at the tag of the cubestore version you want to use. This is our "master" branch on the fork.
2. With new new version checked out, use the `cube/rust/cubestore/Dockerfile` as reference, update `cube/rust/cubestore/gorgias.Dockerfile` such that they're identical.
3. Update the final target `FROM` to match the new tag version you wish to use, so that our image is based on the official image. You may wish to leave out package install steps since they'll already be in the image.
4. Also update the `TAG` variable in the `cube/.github/workflows/image-build.yaml` file to match the new tag version you wish to use.
5. Update the README version tags as well above.


<p align="center">
  <a href="https://cube.dev?ref=github-readme"><img src="https://raw.githubusercontent.com/cube-js/cube/master/docs/content/cube-logo-with-bg.png" alt="Cube — Semantic Layer for Data Applications" width="300px"></a>
</p>

<p align="center">
  <a href="https://cube.dev/events/cube-rollup-london?ref=github-readme">
    <img src="https://ucarecdn.com/15574460-3a9f-4fd2-aa86-0edae2d5bcaf/"/>
  </a>
</p>
<br/>

[Website](https://cube.dev?ref=github-readme) • [Getting Started](https://cube.dev/docs/getting-started?ref=github-readme) • [Docs](https://cube.dev/docs?ref=github-readme) • [Examples](https://cube.dev/docs/examples?ref=github-readme) • [Blog](https://cube.dev/blog?ref=github-readme) • [Slack](https://slack.cube.dev?ref=github-readme) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube/workflows/Build/badge.svg)](https://github.com/cube-js/cube/actions?query=workflow%3ABuild+branch%3Amaster)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fcube-js%2Fcube.js?ref=badge_shield)

__Cube is the semantic layer for building data applications.__ It helps data engineers and application developers access data from modern data stores, organize it into consistent definitions, and deliver it to every application.

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

* Star this repo and follow us on [Twitter](https://twitter.com/the_cube_dev).
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
