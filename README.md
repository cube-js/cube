
<p align="center"><a href="https://www.statsbot.co"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="340px"></a></p>

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
  .then(r => {
    const context = document.getElementById("myChart");
    new Chart(context, chartjsConfig(r));
  });
```

## API

### cubejs(apiKey)

Create instance of `CubejsApi`.

- `apiKey` - API key used to authorize requests and determine SQL database you're accessing. Please email info@statsbot.co to obtain API key.

### CubejsApi.load(query, options, callback)

Fetch data for passed `query`.

- `query` - analytic query. Learn more about it's format below.
- `options` - optional.
-- `progressCallback(ProgressResult)` - pass function to receive real time query execution progress.
- `callback(err, ResultSet)` - result callback. If not passed `load()` will return promise.

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

### Query Format

Query is plain JavaScript object with following format

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

```