
<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)

__Cube.js is an open source modular framework to build analytical web applications__. It is primarily used to build internal business intelligence tools or to add customer-facing analytics to an existing application.

Cube.js was designed to work with Serverless Query Engines like AWS Athena and Google BigQuery. Multi-stage querying approach makes it suitable for handling trillions of data points. Most modern RDBMS work with Cube.js as well and can be tuned for adequate performance.

Unlike others, it is not a monolith application, but a set of modules, which does one thing well. Cube.js provides modules to run transformations and modeling in data warehouse, querying and caching, managing API gateway and building UI on top of that.

### Cube.js Backend

- __Cube.js Schema.__ It acts as an ORM for analytics and allows to model everything from simple counts to cohort retention and funnel analysis.
- __Cube.js Query Orchestration and Cache.__ It optimizes query execution by breaking queries into small, fast, reusable and materialzed pieces.
- __Cube.js API Gateway.__ It provides idempotent long polling API which guarantees analytic query results delivery without request time frame limitations and tolerant to connectivity issues.

### Cube.js Frontend

- __Cube.js Javascript Client.__ Сore set of methods to access Cube.js API Gateway and to work with query result sets.
- __Cube.js React, Angular and Vue.__ Framework specific wrappers for Cube.js API.

## Why Cube.js?

If you are building your own business intelligence tool or customer-facing analytics most probably you'll face following problems:

1. __Performance.__ Most of effort time in modern analytics software development is spent to provide adequate time to insight. In the world where every company data is a big data writing just SQL query to get insight isn't enough anymore.
2. __SQL code organization.__ Modelling even a dozen of metrics with a dozen of dimensions using pure SQL queries sooner or later becomes a maintenance nightmare which ends up in building modelling framework.
3. __Infrastructure.__ Key components every production-ready analytics solution requires: analytic SQL generation, query results caching and execution orchestration, data pre-aggregation, security, API for query results fetch, and visualization.

Cube.js has necessary infrastructure for every analytic application that heavily relies on its caching and pre-aggregation layer to provide several minutes raw data to insight delay and sub second API response times on a trillion of data points scale.

![](https://raw.githubusercontent.com/statsbotco/cube.js/master/docs/old-was-vs-cubejs-way.png)


## Contents

- [Getting Started](#getting-started)
- [Examples](#examples)
- [Docs](https://cube.dev/docs)
- [Tutorials](#tutorials)
- [Community](#community)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

### 1. Install with NPM or Yarn
```bash
$ npm install -g cubejs-cli
# or
$ yarn global add cubejs-cli
```

### 2. Connect to Your Database
Run the following command to get started with Cube.js

```bash
$ cubejs create <project name> -d <database type>
```

specifying the project name and your database using `-d` flag. Available options: 

* `postgres`
* `mysql`
* `mongobi`
* `athena`
* `bigquery`
* `redshift`
* `mssql`
* `clickhouse`
* `snowflake`
* `presto`

For example,

```bash
$ cubejs create hello-world -d postgres
```

Once run, the `create` command will create a new project directory that contains the scaffolding for your new Cube.js project. This includes all the files necessary to spin up the Cube.js backend, example frontend code for displaying the results of Cube.js queries in a React app, and some example schema files to highlight the format of the Cube.js Data Schema layer.

The `.env` file in this project directory contains placeholders for the relevant database credentials. For MySQL and PostgreSQL, you'll need to fill in the target host, database name, user and password. For Athena, you'll need to specify the AWS access and secret keys with the [access necessary to run Athena queries](https://docs.aws.amazon.com/athena/latest/ug/access.html), and the target AWS region and [S3 output location](https://docs.aws.amazon.com/athena/latest/ug/querying.html) where query results are stored.

### 3. Define Your Data Schema

Cube.js uses Data Schema to generate and execute SQL.

It acts as an ORM for your database and it is flexible enough to model everything from simple counts to cohort retention and funnel analysis. [Read more about Cube.js Schema](https://cube.dev/docs/getting-started-cubejs-schema).

You can generate schema files using developer Playground.
To do so please start dev server from project directory

```bash
$ npm run dev
```

Then go to `http://localhost:4000` and use UI to generate schema files.

#### Manually creating Data Schema files

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

### 4. Visualize Results
The Cube.js client connects to the Cube.js Backend and lets you visualize your data. This section shows how to use Cube.js Javascript client.

As a shortcut you can run your dev server first:

```
$ npm run dev
```

Then open `http://localhost:4000` to see visualization examples. This will open a Developer Playground app. You can change the metrics and dimensions of the example to use the schema you defined above, change the chart types, generate sample code out of it and more!

#### Cube.js Client Installation

Vanilla JS:
```bash
$ npm i --save @cubejs-client/core
```

React:

```bash
$ npm i --save @cubejs-client/core
$ npm i --save @cubejs-client/react
```

#### Example Usage

##### Vanilla Javascript
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
const context = document.getElementById('myChart');
new Chart(context, chartjsConfig(resultSet));
```

##### React
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


## Examples

| Demo | Code | Description |
|:------|:----------:|:-------------|
|[Examples Gallery](https://statsbotco.github.io/cubejs-client/)|[examples-gallery](https://github.com/cube-js/cubejs-client/tree/master/examples/examples-gallery)|Examples Gallery with different visualizations libraries|
|[Stripe Dashboard](http://cubejs-stripe-dashboard-example.s3-website-us-west-2.amazonaws.com/)|[stripe-dashboard](https://github.com/cube-js/cubejs-client/tree/master/examples/stripe-dashboard)|Stripe Demo Dashboard built with Cube.js and Recharts|
|[AWS Web Analytics](https://statsbotco.github.io/cubejs-client/aws-web-analytics/)|[aws-web-analytics](https://github.com/cube-js/cubejs-client/tree/master/examples/aws-web-analytics)|Web Analytics with AWS Lambda, Athena, Kinesis and Cube.js|
|[Event Analytics](https://d1ygcqhosay4lt.cloudfront.net/)|[event-analytics](https://github.com/cube-js/cube.js/tree/master/examples/event-analytics)|Mixpanel like Event Analytics App built with Cube.js and Snowplow|
|[Node Express Dashboard](https://express-analytics-dashboard.herokuapp.com)|[node-express-dashboard](https://github.com/cube-js/cube.js/tree/master/examples/express-analytics-dashboard)|Analytics Dashboard with Node, Express, and Cube.js|

## Tutorials

### Getting Started Tutorials
- [Cube.js, the Open Source Dashboard Framework: Ultimate Guide ](https://cube.dev/blog/cubejs-open-source-dashboard-framework-ultimate-guide)
- [Building MongoDB Dashboard using Node.js](https://cube.dev/blog/building-mongodb-dashboard-using-node.js)
- [Node Express Analytics Dashboard with Cube.js](https://cube.dev/blog/node-express-analytics-dashboard-with-cubejs/)
- [Building a Serverless Stripe Analytics Dashboard](https://cube.dev/blog/building-serverless-stripe-analytics-dashboard/)
### Advanced
- [Optimize Cube.js Performance with Pre-Aggregations](https://cube.dev/blog/high-performance-data-analytics-with-cubejs-pre-aggregations/)
- [Building an Open Source Mixpanel Alternative. Part 1: Collecting and Displaying Events](https://cube.dev/blog/building-an-open-source-mixpanel-alternative-1/)
- [Building an Open Source Mixpanel Alternative. Part 2: Conversion Funnels](https://cube.dev/blog/building-open-source-mixpanel-alternative-2/)
- [Building Open Source Google Analytics from Scratch](https://cube.dev/blog/building-open-source-google-analytics-from-scratch/)
- [React Query Builder with Cube.js](https://cube.dev/blog/react-query-builder-with-cubejs/)


## Community

If you have any questions or need help - [please join our Slack community](https://slack.cube.dev) of amazing developers and contributors.

## Architecture
__Cube.js acts as an analytics backend__, translating business logic (metrics and dimensions) into SQL and handling database connection. 

The Cube.js javascript Client performs queries, expressed via dimensions, measures, and filters. The Server uses Cube.js Schema to generate a SQL code, which is executed by your database. The Server handles all the database connection, as well as pre-aggregations and caching layers. The result then sent back to the Client. The Client itself is visualization agnostic and works well with any chart library.

<p align="center"><img src="https://i.imgur.com/FluGFqo.png" alt="Cube.js" width="100%"></p>

## Contributing

Contributions are **welcome and extremely helpful** 🙌 Please refer to [the contribution guide](https://github.com/cube-js/cube.js/blob/master/CONTRIBUTING.md) for more information.

## License

Cube.js Client is [MIT licensed](./packages/cubejs-client-core/LICENSE).

Cube.js Backend is [Apache 2.0 licensed](./packages/cubejs-server/LICENSE).
