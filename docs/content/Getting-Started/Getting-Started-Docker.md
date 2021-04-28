---
title: Getting Started with Cube.js using Docker
permalink: /getting-started/docker
redirect_from:
  - /getting-started-docker
---

This guide will help you get Cube.js running using Docker.

<!-- prettier-ignore-start -->
[[info |]]
| Prefer using Docker Compose?
| [Check out this page instead](/getting-started/docker/compose).
<!-- prettier-ignore-end -->

## 1. Run Cube.js with Docker CLI

In a new folder for your project, run the following command:

```bash
docker run -p 4000:4000 \
  -v ${pwd}:/cube/conf \
  -e CUBEJS_DEV_MODE=true \
  cubejs/cube
```

## 2. Open Developer Playground

<!-- prettier-ignore-start -->
[[info |]]
| This step assumes you can connect to a database instance. If you're unable
| to connect to a remote instance, please use a Docker image to run one in
| your local development environment.
<!-- prettier-ignore-end -->

Head to [http://localhost:4000](http://localhost:4000) to open [Developer
Playground][ref-devtools-playground].

The [Developer Playground][ref-devtools-playground] has a database connection
wizard that loads when Cube.js is first started up and no `.env` file is found.
After database credentials have been set up, an `.env` file will automatically
be created and populated with the same credentials.

<div
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Getting-Started/connection-wizard-1.png"
  style="border: none"
  width="100%"
  />
</div>

Click on the type of database to connect to, and you'll be able to enter
credentials:

<div
  style="text-align: center"
>
  <img
  src="https://raw.githubusercontent.com/cube-js/cube.js/master/docs/content/Getting-Started/connection-wizard-2.png"
  style="border: none"
  width="100%"
  />
</div>

After clicking Apply, you should see tables available to you from the configured
database. Select one to generate a data schema. Once the schema is generated,
you can execute queries on the Build tab.

## Next Steps

Generating Data Schema files in the Developer Playground is a good first step to
start modelling your data. You can [learn more about Cube.js Data
Schema][ref-cubejs-schema] for complex data modelling techniques.

Learn how to [query Cube.js with REST API][ref-rest-api] or [use our Javascript
client library and integrations with frontend
frameworks][ref-frontend-introduction].

[ref-config]: /config
[ref-connecting-to-the-database]: /connecting-to-the-database
[ref-cubejs-schema]: /getting-started-cubejs-schema
[ref-devtools-playground]: /dev-tools/dev-playground
[ref-env-vars]: /reference/environment-variables
[ref-frontend-introduction]: /frontend-introduction
[ref-rest-api]: /rest-api
