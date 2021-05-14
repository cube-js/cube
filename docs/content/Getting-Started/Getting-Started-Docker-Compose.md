---
title: Docker Compose
permalink: /getting-started/docker/compose
---

This guide will help you get Cube.js running using Docker Compose.

<div class="block-video" style="position: relative; padding-bottom: 56.25%; height: 0;">
  <iframe src="https://www.loom.com/embed/268bc36e80a34f8790c805d33d6accf5" frameborder="0" webkitallowfullscreen mozallowfullscreen allowfullscreen style="position: absolute; top: 0; left: 0; width: 100%; height: 100%;"></iframe>
</div>

## 1. Create a Docker Compose file

Create a `docker-compose.yml` file with the following content:

<!-- prettier-ignore-start -->
[[warning |]]
| If you're using Linux as the Docker host OS, you'll also need to add
| `network_mode: 'host'` to your `docker-compose.yml`.
<!-- prettier-ignore-end -->

```yaml
version: '2.2'

services:
  cube:
    image: cubejs/cube:latest
    ports:
      # 4000 is the port for the Cube.js API
      - 4000:4000
      # 3000 is the port for the Developer Playground, and is available only in
      # development mode
      - 3000:3000
    environment:
      - CUBEJS_DEV_MODE=true
    volumes:
      - .:/cube/conf
```

## 2. Run Cube.js

<!-- prettier-ignore-start -->
[[info |]]
| Using Windows? Remember to use [PowerShell][link-powershell] or
| [WSL2][link-wsl2] to run the command below.
<!-- prettier-ignore-end -->

```bash
$ docker-compose up -d
```

Check if the container is running:

```bash
$ docker-compose ps
```

## 3. Open Developer Playground

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

[link-powershell]: https://docs.microsoft.com/en-us/powershell/scripting/overview?view=powershell-7.1
[link-wsl2]: https://docs.microsoft.com/en-us/windows/wsl/install-win10
[ref-config]: /config
[ref-connecting-to-the-database]: /connecting-to-the-database
[ref-cubejs-schema]: /getting-started-cubejs-schema
[ref-devtools-playground]: /dev-tools/dev-playground
[ref-env-vars]: /reference/environment-variables
[ref-frontend-introduction]: /frontend-introduction
[ref-rest-api]: /rest-api
