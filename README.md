
<p align="center"><a href="https://www.statsbot.co"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

## Examples

- [Examples Gallery](https://statsbotco.github.io/cubejs-client/)

## Installation

Vanilla JS:

```bash
npm i --save @cubejs-client/core
```

React:

```bash
npm i --save @cubejs-client/core
npm i --save @cubejs-client/react
```

## Getting Started

Instantiate Cube.js API:

```js
const cubejsApi = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');
```

Please email info@statsbot.co to obtain API key.

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

## Securing customer facing applications

Cube.js tokens are in fact [JWT tokens](https://jwt.io/).
Besides public API key you can obtain secret key to generate public customer API keys on your server side and embed it in web page that renders query results.
Secret key looks like `cjs_38594_sPEWwPkVtTEEjTs9AkpicdUcw26R58ueo2G4rRZ-Wyc`.
To generate public key with this secret you should provide minimal payload `{ i: 38594 }` which is your key identifier required for token verification.
Such key is called global and provides no security context so it has all possible rights for querying.
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

Please email info@statsbot.co to obtain your secret key.

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