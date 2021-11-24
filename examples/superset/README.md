# Building a metrics dashboard with Superset and Cube

Please see the [blog post](https://cube.dev/blog/building-metrics-dashboard-with-superset/) for instructions.

## Running Superset locally

Please make sure you have [Docker](https://www.docker.com/get-started) installed on your machine.

You can explore the [instructions](https://hub.docker.com/r/apache/superset) to run Superset on Docker Hub. First, let's download and start a container:

```bash
docker run -d -p 8080:8088 --name superset apache/superset
```

Second, setup an admin account. By default, the username and password would be set to `admin`, but you can definitely adjust the credentials as you wish:

```bash
docker exec -it superset superset fab create-admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@superset.com \
  --username admin \
  --password admin
```

Then, initialize the database...

```bash
docker exec -it superset superset db upgrade
```

...and setup roles:

```bash
docker exec -it superset superset init
```

That's it! Now you should be able to navigate to [localhost:8080](http://localhost:8080/login/) and log into your Superset instance using the username and password from above.

## Running Cube locally

Note that all necessary files are already available in this folder. Here's how you can set up Cube and create them on your own:

Let's run Cube in Docker and see how it helps maintain a single source of truth for all business metrics.

Create a new folder for your Cube app and navigate to it:

```bash
mkdir superset-example
cd superset-example
```

Run this snippet to create a new `docker-compose.yml` file with the configuration. We'll also use environment variables from the `.env` file to instruct Cube how to connect to Postgres:

```bash
cat > docker-compose.yml << EOL
version: '2.2'
services:
  cube:
    image: cubejs/cube:latest
    ports:
      - 4000:4000
      - 3306:3306
    env_file: .env
    volumes:
      - .:/cube/conf
EOL
```

Then, run this snippet to create the `.env` file with database credentials. Cube will connect to a publicly available Postgres database that I've already set up. Check the [docs](https://cube.dev/docs/config/databases/postgres) to learn more about connecting Cube to Postgres or any other database.

```bash
cat > .env << EOL
CUBEJS_DB_TYPE=postgres
CUBEJS_DB_HOST=demo-db-examples.cube.dev
CUBEJS_DB_NAME=ecom
CUBEJS_DB_USER=cube
CUBEJS_DB_PASS=12345
CUBEJS_API_SECRET=SECRET
CUBEJS_SQL_PORT=3306
CUBEJS_DEV_MODE=true
CUBEJS_EXTERNAL_DEFAULT=true
EOL
```

That is all we need to let Cube connect to a database. Please note the `CUBEJS_SQL_PORT` environment variable and port `3306` exposed in `docker-compose.yml`. It enables [SQL API](https://cube.dev/docs/backend/sql) that we'll use later to connect Superset to Cube.

The last part of configuration is the [data schema](https://cube.dev/docs/schema/getting-started) which declaratively describes the metrics we'll be putting on the dashboard. Actually, Cube can generate it for us!

Navigate to [localhost:4000](http://localhost:4000) and, on the Schema tab, select the "public" schema with all tables, and generate data schema files. Now, you should be able to see files like `LineItems.js`, `Orders.js`, `Users.js`, etc. under the "schema" folder.