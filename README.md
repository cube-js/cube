
<p align="center"><a href="https://www.statsbot.co"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

## Examples

- [Examples Gallery](https://statsbotco.github.io/cubejs-client/)

## Installation

Vanilla JS:

```
npm i --save @cubejs-client/core
```

React:

```
npm i --save @cubejs-client/core
npm i --save @cubejs-client/react
```

## Getting Started

Instantiate Cube.js API:

```
const cubejsApi = cubejs('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpIjozODU5NH0.5wEbQo-VG2DEjR2nBpRpoJeIcE_oJqnrm78yUo9lasw');
```

Please email info@statsbot.co to obtain API key.

Use load API to fetch data:

```
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

```
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

```
[
    { "Stories.time":"2015-01-01T00:00:00", "Stories.count": 27120 },
    { "Stories.time":"2015-02-01T00:00:00", "Stories.count": 25861 },
    { "Stories.time":"2015-03-01T00:00:00", "Stories.count": 29661 },
    ...
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

```
{
  measures: ['Stories.count'],
  dimensions: ['Stories.category'],
  filters: [{
    dimension: 'Stories.dead',
    operator: 'equals',
    params: ['No']
  }],
  timeDimensions: [{
    dimension: 'Stories.time',
    dateRange: ['2015-01-01', '2015-12-31'],
    granularity: 'month'
  }]
}
```
