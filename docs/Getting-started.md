---
title: Getting Started
permalink: /getting-started
category: Getting Started
---

## 1. Install with NPM or Yarn
Cube.js CLI can be installed via NPM or Yarn.

### NPM
```bash
$ npm install -g cubejs-cli
```

### Yarn
```bash
$ yarn global add cubejs-cli
```

## 2. Connect to Your Database
Run the following command to get started with Cube.js

```bash
$ cubejs create <project name> -d <database type>
```

specifying the project name and your database using `-d` flag. Available options: 

* `postgres`
* `mysql` 
* `athena`

For example,

```bash
$ cubejs create hello-world -d postgres
```

Once run, the `create` command will create a new project directory that contains the scaffolding for your new Cube.js project. This includes all the files necessary to spin up the Cube.js backend, example frontend code for displaying the results of Cube.js queries in a React app, and some example schema files to highlight the format of the Cube.js Data Schema layer.

The `.env` file in this project directory contains placeholders for the relevant database credentials. For MySQL and PostgreSQL, you'll need to fill in the target host, database name, user and password. For Athena, you'll need to specify the AWS access and secret keys with the [access necessary to run Athena queries](https://docs.aws.amazon.com/athena/latest/ug/access.html), and the target AWS region and [S3 output location](https://docs.aws.amazon.com/athena/latest/ug/querying.html) where query results are stored.

[Learn more about connecting to different databases with Cube.js](connecting-to-the-database)

## 3. Define Your Data Schema

Cube.js uses Data Schema to generate and execute SQL.

It acts as an ORM for your database and it is flexible enough to model everything from simple counts to cohort retention and funnel analysis. [Read more about Cube.js Schema](getting-started-cubejs-schema).

You can generate schema files from your database tables using the `cubejs` CLI, or write them manually:

### Generating Data Schema files for MySQL, Postgres

Since you've defined the target database in the `CUBEJS_DB_NAME` environment variable, in the `.env` file above, you can simply specify a comma-separated list of tables for which you want to generate Data Schema files as the argument for the `-t` option:

```bash
$ cubejs generate -t orders,customers
```

### Generating Data Schema files for Athena

Generating Data Schema files for Athena requires you to pass the target database and table in the format `db.table`. For example:

```bash
$ cubejs generate -t my_db.orders
```

### Manually creating Data Schema files

You can also add schema files to the `schema` folder manually:

```javascript
// schema/users.js

cube(`Users`, {
   measures: {
     type: `count`
   },

   dimensions: {
     age: {
       type: `number`,
       sql: `age`
     },

     createdAt: {
       type: `time`,
       sql: `createdAt`
     },

     country: {
       type: `string`,
       sql: `country`
     }
   }
});
```

## 4. Visualize Results
The Cube.js client connects to the Cube.js Backend and lets you visualize your data. This section shows how to use Cube.js Javascript client.

The Cube.js backend requires a Redis server running on your local machine on the default port of `6379`. This default location can be changed by setting the `REDIS_URL` environment variable to your Redis server. Please make sure your Redis server is up before proceeding:

```bash
$ redis-cli ping
PONG
```

As a shortcut you can run your dev server first:

```
$ npm run dev
```

Then open `http://localhost:4000` to see visualization examples. This will open a [codesandbox.io](https://codesandbox.io) sample React app you can edit. You can change the metrics and dimensions of the example to use the schema you defined above, change the chart types, and more!

### Cube.js Client Installation

Vanilla JS:
```bash
$ npm i --save @cubejs-client/core
```

React:

```bash
$ npm i --save @cubejs-client/core
$ npm i --save @cubejs-client/react
```

### Example Usage

#### Vanilla Javascript
Instantiate Cube.js API and then use it to fetch data:

```js
import cubejs from '@cubejs-client/core';
import Chart from 'chart.js';
import chartjsConfig from './toChartjsData';

const cubejsApi = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');

const resultSet = await cubejsApi.load({
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
})
const context = document.getElementById("myChart");
new Chart(context, chartjsConfig(resultSet));
```

#### React
Import `cubejs` and `QueryRenderer` components, and use them to fetch the data.
In the example below we use Recharts to visualize data.

```jsx
import React from 'react';
import { LineChart, Line, XAxis, YAxis } from 'recharts';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';

const cubejsApi = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');

export default () => {
  return (
    <QueryRenderer
      query={{
        measures: ['Stories.count'],
        dimensions: ['Stories.time.month']
      }}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return 'Loading...';
        }

        return (
          <LineChart data={resultSet.rawData()}>
            <XAxis dataKey="Stories.time"/>
            <YAxis/>
            <Line type="monotone" dataKey="Stories.count" stroke="#8884d8"/>
          </LineChart>
        );
      }}
    />
  )
}
```
