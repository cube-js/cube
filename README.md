
<p align="center"><a href="https://www.statsbot.co/cubejs"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://statsbot.co/cubejs) • [Blog](https://statsbot.co/blog) • [Slack](https://publicslack.com/slacks/cubejs/invites/new) • [Twitter](https://twitter.com/thecubejs)

[![npm version](https://badge.fury.io/js/%40cubejs-client%2Fcore.svg)](https://badge.fury.io/js/%40cubejs-client%2Fcore)

__Cube.js is an open source modular framework to build analytical web applications__. It is primarily used to build internal business intelligence tools or to add customer-facing analytics to an existing application.

Cube.js was designed to work with Serverless Query Engines like AWS Athena and Google BigQuery at the first place. Multi-stage querying approach makes it suitable for handling trillions of data points. Most of modern RDBMS work with Cube.js as well and can be tuned for adequate performance.

Unlike others, it is not a monolith application, but a set of modules, which does one thing well. Cube.js provides modules to run transformations and modeling in data warehouse, querying and caching, managing API gateway and building UI on top of that.

### Cube.js Backend

- __Cube.js Schema.__ It acts as an ORM for analytics and allows to model everything from simple counts to cohort retention and funnel analysis.
- __Cube.js Query Orchestration and Cache.__ It optimizes query execution by breaking queries into small, fast, reusable and materialzed pieces.
- __Cube.js API Gateway.__ It provides idempotent long polling API which guarantees analytic query results delivery without request time frame limitations and tolerant to connectivity issues.

### Cube.js Frontend

- __Cube.js Javascript Client.__ It provides idempotent long polling API which guarantees analytic query results delivery without request time frame limitations and tolerant to connectivity issues.
- __Cube.js React.__ React wrapper for Cube.js API.


## Contents

- [Examples](#examples)
- [Tutorials](#tutorials)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Security](#security)
- [API](#api)


## Examples

| Demo | Code | Description |
|:------|:----------:|:-------------|
|[Examples Gallery](https://statsbotco.github.io/cubejs-client/)|[examples-gallery](https://github.com/statsbotco/cubejs-client/tree/master/examples/examples-gallery)|Examples Gallery with different visualizations libraries|
|[Stripe Dashboard](http://cubejs-stripe-dashboard-example.s3-website-us-west-2.amazonaws.com/)|[stripe-dashboard](https://github.com/statsbotco/cubejs-client/tree/master/examples/stripe-dashboard)|Stripe Demo Dashboard built with Cube.js and Recharts|
|[AWS Web Analytics](https://statsbotco.github.io/cubejs-client/aws-web-analytics/)|[aws-web-analytics](https://github.com/statsbotco/cubejs-client/tree/master/examples/aws-web-analytics)|Web Analytics with AWS Lambda, Athena, Kinesis and Cube.js|

## Tutorials
- [Building a Serverless Stripe Analytics Dashboard](https://statsbot.co/blog/building-serverless-stripe-analytics-dashboard/)
- [Building E-commerce Analytics React Dashboard with Cube.js and Flatlogic](https://statsbot.co/blog/building-analytics-react-dashboard-with-cube.js)
- [Building Open Source Google Analytics from Scratch](https://statsbot.co/blog/building-open-source-google-analytics-from-scratch/)

## Architecture
__Cube.js acts as an analytics backend__, taking care of translating business  logic into SQL and handling database connection. 

The Cube.js javascript Client performs queries, expressed via dimensions, measures, and filters. The Server uses Cube.js Schema to generate a SQL code, which is executed by your database. The Server handles all the database connection, as well as pre-aggregations and caching layers. The result then sent back to the Client. The Client itself is visualization agnostic and works well with any chart library.

<p align="center"><img src="https://i.imgur.com/FluGFqo.png" alt="Cube.js" width="100%"></p>

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
$ cubejs create hello-world -d postgres
```
Specify your database using `-d` flag. Available options: `postgres`, `mysql`. Edit `.env` file in the generated project with your database credentials.

### 3. Define Your Data Schema
Cube.js uses Data Schema to generate and execute SQL.
It acts as an ORM for your analytics and it is flixible enough to model everything from simple counts to cohort retention and funnel analysis. [Read more about Cube.js Schema](https://statsbot.co/docs/getting-started-cubejs).

Generate schema files from your database tables:
```
$ cubejs generate -t orders,customers
```

Or put schema files into `schema` folder manually:


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
The Cube.js client connects to Cube.js Backend and lets you visualize your data. This section shows how to use Cube.js Javascript client.

As a shortcut you can run your dev server first:

```
$ npm run dev
```

Then open `http://localhost:4000` to see visualization examples.

#### Installation

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

##### Vanilla Javascript. 
Instantiate Cube.js API and then use it to fetch data:

```js
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

##### Using React
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

## Security

Cube.js auth tokens used to access an API are in fact [JWT tokens](https://jwt.io/).
You should use API Secret to generate your own client side auth tokens.
API Secret is generated on app creation and saved in `.env` file as `CUBEJS_API_SECRET` variable.

You can generate two types of tokens:
- Without security context. It implies same data access permissions for all users.
- With security context. User or role-based security models can be implemented using this approach.

Security context can be provided by passing `u` param for payload.
For example if you want to pass user id in security context you can create token with payload:
```json
{
  "u": { "id": 42 }
}
```

In this case `{ id: 42 }` object will be accessible as `USER_CONTEXT` in cube.js Data Schema.
Learn more: [Data Schema docs](https://statsbot.co/docs/cube#context-variables-user-context).

> *NOTE*: We strongly encourage you to use `exp` expiration claim to limit life time of your public tokens.
> Learn more: [JWT docs](https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim).

## API

### cubejs(apiKey)

Create instance of `CubejsApi`.

- `apiKey` - API key used to authorize requests and determine SQL database you're accessing.

### CubejsApi.load(query, options, callback)

Fetch data for passed `query`. Returns promise for `ResultSet` if `callback` isn't passed.

* `query` - analytic query. Learn more about it's format below.
* `options` - options object. Can be omitted.
    * `progressCallback(ProgressResult)` - pass function to receive real time query execution progress.
* `callback(err, ResultSet)` - result callback. If not passed `load()` will return promise.


### QueryRenderer

`<QueryRenderer />` React component takes a query, fetches the given query, and uses the render prop to render the resulting data.

Properties:

- `query`: analytic query. Learn more about it's format below.
- `cubejsApi`: `CubejsApi` instance to use.
- `render({ resultSet, error, loadingState })`: output of this function will be rendered by `QueryRenderer`.
  - `resultSet`: A `resultSet` is an object containing data obtained from the query.  If this object is not defined, it means that the data is still being fetched. `ResultSet` object provides a convient interface for data munipulation.
  - `error`: Error will be defined if an error has occurred while fetching the query.
  - `loadingState`: Provides information about the state of the query loading.
  

### ResultSet.rawData()

Returns query result raw data returned from server in format

```js
[
    { "Stories.time":"2015-01-01T00:00:00", "Stories.count": 27120 },
    { "Stories.time":"2015-02-01T00:00:00", "Stories.count": 25861 },
    { "Stories.time":"2015-03-01T00:00:00", "Stories.count": 29661 },
    //...
]
```

Format of this data may change over time.

### Query Format

Query is plain JavaScript object, describing an analytics query. The basic elements of query (query members) are `measures`, `dimensions`, and `segments`. You can [learn more about Cube.js Data Schema here.](https://statsbot.co/docs/getting-started-cubejs)
The query member format name is `CUBE_NAME.MEMBER_NAME`, for example dimension email in the Cube Users would have the following name `Users.email`.

Query has following properties:

- `measures`: An array of measures.
- `dimensions`: An array of dimensions.
- `filters`: An array of filters.
- `timeDimensions`: A convient way to specify a time dimension with a filter. It is an array of objects with following keys
  - `dimension`: Time dimension name.
  - `dateRange`: An array of dates with following format '2015-01-01', if only one date specified the filter would be set exactly to this date. 
  - `granularity`: A granularity for a time dimension, supports following values `day|week|month|year`.
- `segments`: An array of segments. Segment is a named filter, created in the Data Schema.
- `limit`: A row limit for your query. The hard limit is set to 5000 rows by default.

```js
{
  measures: ['Stories.count'],
  dimensions: ['Stories.category'],
  filters: [{
    dimension: 'Stories.dead',
    operator: 'equals',
    values: ['No']
  }],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }],
  limit: 100
}
```


### License

Cube.js Client is [MIT licensed](./packages/cubejs-client-core/LICENSE).

Cube.js Backend is [Apache 2.0 licensed](./packages/cubejs-server/LICENSE).
