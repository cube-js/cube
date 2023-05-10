# Material UI Dashboard

Example: https://material-ui-dashboard-demo.cube.dev

Guide: https://material-ui-dashboard.cube.dev

React Material UI dashboard with Cube.js

## Run Project

### Setup a Demo Backend

if you already have Cube.js Backend up and running you can skip this step.

Let's start by setting up a database with some sample data. We'll use PostgresQL and our example e-commerce dataset for this tutorial. You can download and import it by running the following commands.

```
$ curl https://cube.dev/downloads/ecom-dump.sql > ecom-dump.sql
$ createdb ecom
$ psql --dbname ecom -f ecom-dump.sql
```

Cube.js uses environment variables for configuration. It uses environment variables starting with `CUBEJS_`. To configure the connection to our database, we need to specify the DB type and name. In the Cube.js project folder create the .env file with the following:

```
CUBEJS_DB_NAME=ecom
CUBEJS_DB_TYPE=postgres
CUBEJS_API_SECRET=SECRET
```

### Backend run
To start frontend application use this commands
```
$ yarn
$ yarn dev
```

### Frontend run
To start frontend application use this commands
```
$ cd dashboard-app
$ yarn
$ yarn start
```

