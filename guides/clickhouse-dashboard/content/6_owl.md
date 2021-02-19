---
order: 6
title: "How to Draw the Rest of the Owl"
---

Honestly, it's quite easy to transform this generic dashboard into stock market data visualization in just a few quick steps.

**First, let's connect to another datasource.** It will still be ClickHouse: behind the scenes and for our convenience, I've set up a dedicated ClickHouse instance in Google Cloud. It holds a fresh version of this [stock market dataset](https://www.kaggle.com/jacksoncrow/stock-market-dataset) which was updated on Feb 17, 2021.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/gzqscto0t2gpyzklokk1.png)

The dataset contains nearly 3 GB and just under 9000 tickers with daily volumes and prices: low, high, open, and close price values. So, it's 28.2 million rows in total which is not much but a fairly decent data volume.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/nn03lt9wbrh2tet4mnmy.png)

To use this dataset, update your `.env` file with these contents:

```
# Cube.js environment variables: https://cube.dev/docs/reference/environment-variables

CUBEJS_DB_TYPE=clickhouse
CUBEJS_DB_HOST=demo-db-clickhouse.cube.dev
CUBEJS_DB_PORT=8123
CUBEJS_DB_USER=default
CUBEJS_DB_PASS=
CUBEJS_DB_NAME=default
CUBEJS_DB_CLICKHOUSE_READONLY=true

CUBEJS_DEV_MODE=true
CUBEJS_WEB_SOCKETS=true
CUBEJS_API_SECRET=SECRET
```

**Second, let's compose a data schema.** We need to describe our data in terms of [measures](https://cube.dev/docs/measures) and [dimensions](https://cube.dev/docs/dimensions) or, in simpler words, in terms of "what we want to know" about the data (i.e., measures) and "how we can decompose" the data (i.e., dimensions). In our case, stock prices have two obvious dimensions: stock ticker (i.e., company identifier) and date.

However, measures are not that straightforward because we'll need to use different [aggregation functions](https://cube.dev/docs/types-and-formats) (i.e., ways to calculate needed values). For example, daily low prices should be aggregated with the `min` type because the weekly low price is the lowest price of all days, right? Then, obviously, daily high prices should use the `max` type. For open and close prices we'll use the `avg` type, and we'll also employ the `count` type to calculate the total number of data entries.

Now, make sure that the only file in your `schema` folder is named `Stocks.js` and has the following contents:

```js
cube(`Stocks`, {
  sql: `SELECT * FROM default.stocks`,

  measures: {
    count: { sql: `${CUBE}.Date`, type: `count` },
    open: { sql: `${CUBE}.Open`, type: `avg`, format: `currency` },
    close: { sql: `${CUBE}.Close`, type: `avg`, format: `currency` },
    high: { sql: `${CUBE}.High`, type: `max`, format: `currency` },
    low: { sql: `${CUBE}.Low`, type: `min`, format: `currency` },
    volume: { sql: `${CUBE}.Volume`, type: `sum`, format: `currency` },
    firstTraded: { sql: `${CUBE}.Date`, type: `min` },
  },
  
  dimensions: {
    ticker: { sql: `${CUBE}.Ticker`, type: `string` },
    date: { sql: `${CUBE}.Date`, type: `time` },
  },
});
```

With these changes you should be all set to restart your Cube.js instance and use Developer Playground for data exploration. Look how easy it is to find the companies we have the most amount of data about — obviously, because they are publicly traded on the stock exchange since who knows when.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/gmfgi93rg8iu3qi399p8.png)

Here we can see Coca-Cola (`KO`), Hewlett-Packard (`HPQ`), Johnson & Johnson (`JNJ`), Caterpillar (`CAT`), Walt Disney (`DIS`), etc. Actually, you can easily find out since when they are traded by adding the `Stocks.firstTraded` measure to your query. Oops! Now you know that we only have the data since the [Unix epoch](https://en.wikipedia.org/wiki/Unix_time) but it's not a big deal, right?

**Third, let's build a lightweight but nicely looking front-end app.** Developer Playground is great but why not to write some code as we routinely do? It will help us focus and explore the stocks that were popular on the [WallStreetBets](https://www.reddit.com/r/wallstreetbets/) subreddit.

As stock market gurus, we should obviously use the [candlestick chart](https://en.wikipedia.org/wiki/Candlestick_chart) for data visualization. Though it sounds complicated, a candlestick chart is a powerful way to display pricing data because it allows to combine four values (open, close, low, and high prices) in a single geometric figure. You can dig deeper into [Investopedia](https://www.investopedia.com/trading/candlestick-charting-what-is-it/) on the topic.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/uumpfpvk7x7szeviimdx.png)

After that, make sure to go to the `dashboard-app` folder and install a few npm packages for [ApexCharts](https://apexcharts.com). We'll use a readily available candlestick chart component so we don't have to build it ourselves. Run in the console:

```bash
npm install --save apexcharts react-apexcharts
```

Then, create a new file at the `src/components/GameStock.js` location with the following contents. Basically, it uses Cube.js API to query the dataset, ApexCharts to visualize it, and a few [Ant Design](https://ant.design) components to control what is shown. It's not very lengthy and you can flick though it later:

```js
import React, { useState, useEffect } from 'react';
import { Row, Col, Button, Select, Space, Card, Layout } from 'antd';
import { useCubeQuery } from '@cubejs-client/react';
import Chart from 'react-apexcharts';

const GameStock = () => {
  const { resultSet } = useCubeQuery({
    dimensions: [ 'Stocks.ticker' ],
  });

  const [ tickers, setTickers ] = useState([]);
  const [ selectedTicker, setSelectedTicker ] = useState('GME');

  useEffect(() => {
    if (resultSet) {
      setTickers(resultSet.tablePivot().map(x => x['Stocks.ticker']).map(x => ({ label: x, value: x })));
    }
  }, [ resultSet ]);

  const [ dateRange, setDateRange ] = useState(dateRange2021);

  return (
    <Layout>
      <Layout.Header style={{ backgroundColor: '#43436B' }}>
        <Space size='large'>
          <a href='https://cube.dev' target='_blank'>
            <img src='https://cubejs.s3-us-west-2.amazonaws.com/downloads/logo-full.svg' alt='Cube.js' />
          </a>
          <Space>
            <Button href='https://github.com/cube-js/cube.js' target='_blank' ghost>GitHub</Button>
            <Button href='https://slack.cube.dev' target='_blank' ghost>Slack</Button>
          </Space>
        </Space>
      </Layout.Header>
      <div style={{ padding: 50 }}>
        <Row gutter={[ 50, 50 ]}>
          <Col span={24}>
            <Space>
              Ticker
              <Select
                style={{ width: 100 }}
                showSearch
                options={tickers}
                value={selectedTicker}
                loading={!selectedTicker}
                onChange={setSelectedTicker}
                filterOption={(input, option) =>
                  option.value.toLowerCase().indexOf(input.toLowerCase()) === 0
                }
              />
              or
              {prominentTickers.map(t => (
                <Button
                  key={t}
                  size='small'
                  type={t === selectedTicker ? 'primary' : 'default'}
                  onClick={() => setSelectedTicker(t)}
                >{t}</Button>
              ))}
            </Space>
          </Col>
        </Row>
        <Row gutter={[ 50, 50 ]}>
          <Col span={24}>
            <Space>
              Time frame
              {dateRanges.map(([ label, range ]) => (
                <Button
                  key={label}
                  size='small'
                  value={range}
                  onClick={() => setDateRange(range)}
                  type={range === dateRange ? 'primary' : 'default'}
                >{label}</Button>
              ))}
            </Space>
          </Col>
        </Row>
        <Row gutter={[ 50, 50 ]}>
          <Col span={24}>
            <Card style={{ maxWidth: dateRange === dateRange2021 ? '900px' : '100%' }}>
              {selectedTicker && (
                <CandlestickChart ticker={selectedTicker} dateRange={dateRange} />
              )}
            </Card>
          </Col>
        </Row>
      </div>
    </Layout>
  );
};

const CandlestickChart = ({ ticker, dateRange }) => {
  const granularity = dateRange !== undefined ? 'day' : 'month';

  const { resultSet } = useCubeQuery({
    measures: [ 'Stocks.open', 'Stocks.close', 'Stocks.high', 'Stocks.low' ],
    timeDimensions: [ {
      dimension: 'Stocks.date',
      granularity,
      dateRange,
    } ],
    filters: [ {
      dimension: 'Stocks.ticker',
      operator: 'equals',
      values: [ ticker ],
    } ],
  });

  const pivotConfig = {
    x: [ `Stocks.date.${granularity}` ],
    y: [ 'measures' ],
    joinDateRange: false,
    fillMissingDates: false,
  };

  const data = resultSet === null ? [] : resultSet.chartPivot(pivotConfig).map(row => {
    const max = Math.max(row['Stocks.open'], row['Stocks.high'], row['Stocks.low'], row['Stocks.close']);
    const precision = max >= 100 ? 0 : max >= 10 ? 1 : 2;

    return {
      x: new Date(row.x),
      y: [
        row['Stocks.open'].toFixed(precision),
        row['Stocks.high'].toFixed(precision),
        row['Stocks.low'].toFixed(precision),
        row['Stocks.close'].toFixed(precision),
      ],
    };
  });

  const options = {
    title: { text: '', align: 'left' },
    chart: { animations: { enabled: false }, toolbar: { show: false } },
    xaxis: { type: 'datetime' },
    yaxis: { labels: { formatter: v => Math.round(v) }, tooltip: { enabled: true } },
  };

  return <Chart
    options={options}
    series={[ { data } ]}
    type='candlestick'
    height={300} />;
};

const prominentTickers = [ 'BYND', 'GME', 'IRM', 'MAC', 'NOK', 'SPCE' ];

const dateRange202x = [ '2020-01-01', '2021-03-01' ];
const dateRange2021 = [ '2021-01-01', '2021-03-01' ];

const dateRanges = [
  [ '2021', dateRange2021 ],
  [ '2020 – 2021', dateRange202x ],
  [ 'All time', undefined ],
];

export default GameStock;
```

To make everything work, now go to `src/App.js` and change a few lines there to add this new `GameStock` component to the view:

```diff
+ import GameStock from './components/GameStock';
  import './body.css';
  import 'antd/dist/antd.css';

  // ...

  const AppLayout = ({
    children
  }) => <Layout style={{
    height: '100%'
  }}>
-   <Header />
-   <Layout.Content>{children}</Layout.Content>
+   <GameStock />
  </Layout>;

  // ...
```

**Believe it or not, we're all set! 🎉** Feel free to start your `dashboard-app` again with `npm run start` and prepare to be amused.

Not only we can see what happened on Jan 28, 2021 when GameStop (`GME`) stock price were as volatile as one can't imagine with the low at US $ 112 and high at US $ 483. Definitely have a look at `IRM`, `MAC`, or `NOK` as they were also affected by this movement.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/66m8fphcieqxh8ormv1o.png)

Now we can explore the prices of basically every public company or ETF. Just type in a ticker and choose the desired time frame. Maybe you want to have a look at Google (`GOOG` and `GOOGL`) stock prices since 2005? Here they are:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/a05i9ntjdm0biqqqpw4a.png)

I strongly encourage you to [spend some time](https://clickhouse-dashboard-demo.cube.dev) with this ClickHouse dashboard we've just created. Pay attention to how responsive the API is: all the data is served from the back-end by Cube.js and queried from ClickHouse in real-time. Works smoothly, right?

**Thank you for following this tutorial, learning more about ClickHouse, building an analytical dashboard, exploring the power of [Cube.js](https://cube.dev), investigating the stock prices, etc. I sincerely hope that you liked it 😇**

Please don't hesitate to like and bookmark this post, write a short comment, and give a star to [Cube.js](https://github.com/cube-js/cube.js) or [ClickHouse](https://github.com/ClickHouse/ClickHouse) on GitHub. And I hope that you'll give Cube.js and ClickHouse a shot in your next fun pet project or your next important production thing. Cheers!