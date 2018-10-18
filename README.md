
<p align="center"><a href="https://www.statsbot.co"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[![npm version](https://badge.fury.io/js/%40cubejs-client%2Fcore.svg)](https://badge.fury.io/js/%40cubejs-client%2Fcore)

__Cube.js is an analytics framework for modern applications.__ It supplies building blocks that let developers build custom and large-scale analytics frontends without spending the time on a complex analytics backend infrastructure. 

* __Cube.js is visualization agnostic.__ It means you can use your favorite chart library, like Chart.js, Recharts, C3.js or any other.
* __Cube.js Data Schema works as an ORM for your analytics.__ It allows to model everything from simple counts to cohort retention and funnel analysis.
* __It is designed to work on top of your database, so all your data stays with you.__ All major SQL databases are supported.

This repository contains Cube.js Javascript and React clients. The Cube.js Server itself is not yet open-sourced. We are working hard to make it happen. Before that, you can [request early access to our cloud version](https://statsbot.co/cubejs/).



## Examples

- [Examples Gallery](https://statsbotco.github.io/cubejs-client/)

## Getting Started

### 1. Create Free Statsbot Account
Cube.js Cloud is provided by Statsbot, you can sign up for a free account [here](https://statsbot.co/sign-up?cubejs=true).

### 2. Connect Your Database
All major SQL databases are supported. Here the guide on [how to connect your database to Statsbot](http://help.statsbot.co/how-to-connect-database-to-statsbot/how-to-connect-your-database-to-statsbot).

### 3.Define Your Data Schema
Cube.js uses Data Schema to generate and execute SQL. It acts as an ORM for yor analytics and it is flixible enough to model everything from simple counts to cohort retention and funnel analysis. [Read Cube.js Schema docs](https://statsbot.co/docs/getting-started-cubejs).

### 4. Visualize Results
Generate a Cube.js token within Statsbot UI and you are ready to use this library to add analytics features to your app.
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

Instantiate Cube.js API:

```js
const cubejsApi = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');
```

Please [request an early access](https://statsbot.co/cubejs/) to get an API key.

Use load API to fetch data:

```js
cubejsApi.load({
  measures: ['Stories.count'],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
})
  .then(resultSet => {
    const context = document.getElementById("myChart");
    new Chart(context, chartjsConfig(resultSet));
  });
```

Using React `QueryRenderer` component:

```jsx
  <QueryRenderer query={{
    measures: ['Stories.count'],
    timeDimensions: [{
      dimension: 'Stories.time',
      dateRange: ['2015-01-01', '2016-01-01'],
      granularity: 'month'
    }]
  }} cubejsApi={this.api} render={
    ({ resultSet }) => {
      return resultSet && (
        <LineChart width={600} height={300} data={resultSet.rawData()}
                         margin={{top: 5, right: 30, left: 20, bottom: 5}}>
          <XAxis dataKey="Stories.time"
                 tickFormatter={(v) => moment(v).format('MMM YY')}
          />
          <YAxis/>
          <CartesianGrid strokeDasharray="3 3"/>
          <Tooltip/>
          <Legend />
          <Line type="monotone" dataKey="Stories.count" stroke="#8884d8"/>
        </LineChart>
      ) || 'Loading...'
    }
  }
  />
```

## Cube.js API tokens

You're provided with two types of security credentials:
- *Cube.js Global Token*. Has format like `eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw`. Can be passed to `cubejs()`.
- *Cube.js Secret*. Has format like `cjs_38594_sPEWwPkVtTEEjTs9AkpicdUcw26R58ueo2G4rRZ-Wyc`. Should be used to sign JWT tokens passed to `cubejs()`.

Cube.js tokens used to access an API are in fact [JWT tokens](https://jwt.io/).
*Cube.js Global Token* is not an exception and generated for your convenience.
*Cube.js Global Token* is JWT token signed with *Cube.js Secret* that has minimal payload like `{ i: 38594 }`.
*Cube.js Global Token* provides no security context so it has all possible rights for querying.
Besides *Cube.js Global Token* you can use *Cube.js Secret* to generate customer API tokens to restrict data available for querying on your server side and embed it in web page that renders query results.
For example to generate customer API tokens with `cjs_38594_sPEWwPkVtTEEjTs9AkpicdUcw26R58ueo2G4rRZ-Wyc` secret you should provide minimal payload `{ i: 38594 }` which is your key identifier required for token verification.
Security context can be provided by passing `u` param for payload.
For example if you want to pass user id in security context you can create token with payload:
```json
{
  "i": 38594,
  "u": { "id": 42 }
}
```

In this case `{ id: 42 }` object will be accessible as `USER_CONTEXT` in cube.js Data Schema.
Learn more: [Data Schema docs](https://statsbot.co/docs/cube#context-variables-user-context).

> *NOTE*: We strongly encourage you to use `exp` expiration claim to limit life time of your public tokens.
> Learn more: [JWT docs](https://github.com/auth0/node-jsonwebtoken#token-expiration-exp-claim).

Please [request an early access](https://statsbot.co/cubejs/) to get an API key.

## API

### cubejs(apiKey)

Create instance of `CubejsApi`.

- `apiKey` - API key used to authorize requests and determine SQL database you're accessing. Please email info@statsbot.co to obtain API key.

### CubejsApi.load(query, options, callback)

Fetch data for passed `query`. Returns promise for `ResultSet` if `callback` isn't passed.

* `query` - analytic query. Learn more about it's format below.
* `options` - options object. Can be omitted.
    * `progressCallback(ProgressResult)` - pass function to receive real time query execution progress.
* `callback(err, ResultSet)` - result callback. If not passed `load()` will return promise.

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

### <QueryRenderer />

React component for rendering query results.

Properties:

- `query` - analytic query. Learn more about it's format below.
- `cubejsApi` - `CubejsApi` instance to use.
- `render({ resultSet, error, loadingState })` - output of this function will be rendered by `QueryRenderer`.

### Query Format

Query is plain JavaScript object with the following format -

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
  }]
}
```
