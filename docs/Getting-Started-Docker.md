---
title: Getting Started with Docker
permalink: /getting-started-docker
---

[link-connecting-to-the-database]: /connecting-to-the-database
[link-cubejs-schema]: /getting-started-cubejs-schema
[link-rest-api]: /rest-api
[link-frontend-introduction]: /frontend-introduction
[link-config]: /config
[link-env-vars]: /reference/environment-variables

This guide will help you get Cube.js running as a Docker container using Docker Compose.

## 1. Create a Docker Compose file

Create a `docker-compose.yml` file with the following content.

```yaml
version: '2.2'

services:
  cube:
    image: cubejs/cube:latest
    ports:
      # 4000 is a port for Cube.js API
      - 4000:4000
      # 3000 is a port for Playground web server
      # it is available only in dev mode
      - 3000:3000
    env_file: .env
    volumes:
      - ./schema:/cube/conf/schema
```

## 2. Configure Cube.js

There are two ways you can set configuration options for Cube.js; via a [configuration file][link-config], commonly known as the `cube.js` file, and [environment variables][link-env-vars].

We'll configure the database connection via environment variables. You can learn more about setting credentials for different databases in the [Connecting to the Database guide][link-connecting-to-the-database].

The example below is for a Postgres instance running locally. 

```bash
# Create an .env file with the following content
CUBEJS_DB_TYPE=postgres

# For Mac
CUBEJS_DB_HOST=host.docker.internal

# For Windows
CUBEJS_DB_HOST=docker.for.win.localhost

# For Linux
CUBEJS_DB_HOST=localhost

CUBEJS_DB_NAME=databasename
CUBEJS_DB_USER=databaseuser
CUBEJS_DB_PASS=secret
CUBEJS_WEB_SOCKETS=true
CUBEJS_DEV_MODE=true
CUBEJS_API_SECRET=SECRET
```

### Network config for Linux Users

For Linux, add the following line to your `docker-compose.yml` 

```yaml
network_mode: "host"
```

## 3. Run Cube.js

```bash
$ docker-compose up -d
```

Check if the container is running:

```bash
$ docker ps
```

## 4. Open Playground

Head to [http://localhost:4000](http://localhost:4000) to open the Playground.

You can generate Data Schema files using the Playground. Once schema files are
generated you can execute queries on the Build tab in the Playground.

## Next Steps

Generating Data Schema files in the Playground is a good first step to start modelling your data. You can [learn more about Cube.js Data Schema][link-cubejs-schema] for complex data
modelling techniques.

Learn how to [query Cube.js with REST API][link-rest-api] or [use Javascript client library and
integrations with frontend frameworks][link-frontend-introduction].

### Configuration with cube.js file

When using the `cube.js` file for configuration, you need to add it
to the `volumes` definition in your `docker-compose.yml`:

```yaml
  volumes:
    - ./schema:/cube/conf/schema
    - ./cube.js:/cube/conf/cube.js
```


