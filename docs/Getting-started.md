---
title: Getting Started
permalink: /getting-started
category: Getting Started
---

## 1. Install with NPM or Yarn
Cube.js CLI is used for various Cube.js workflows. It can be installed via NPM or Yarn.

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
* `mssql`
* `athena`
* `mongobi`
* `bigquery`
* `redshift`
* `clickhouse`
* `hive`
* `snowflake`
* `prestodb`
* `oracle`

For example,

```bash
$ cubejs create hello-world -d postgres
```

Once run, the `create` command will create a new project directory that contains the scaffolding for your new Cube.js project. This includes all the files necessary to spin up the Cube.js backend, example frontend code for displaying the results of Cube.js queries in a React app, and some example schema files to highlight the format of the Cube.js Data Schema layer.

The `.env` file in this project directory contains placeholders for the relevant database credentials. Setting credentials is covered in [Connecting to the Database](/connecting-to-the-database) section.

## 3. Define Your Data Schema

Cube.js uses Data Schema to generate and execute SQL.

It acts as an ORM for your database and it is flexible enough to model everything from simple counts to cohort retention and funnel analysis. [Read more about Cube.js Schema](https://cube.dev/docs/getting-started-cubejs-schema).

You can generate schema files using developer Playground.
To do so please start dev server from project directory

```bash
$ npm run dev
```

Then go to `http://localhost:4000` and use UI to generate schema files.

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
The Cube.js client provides set of methods to access Cube.js API and to work with query result.  The client itself doesn't provide any visualizations and is designed to work with existing chart libraries.

As a shortcut you can run your dev server first:

```bash
$ npm run dev
```

Then open `http://localhost:4000` to see visualization examples. This will open a Developer Playground app. You can change the metrics and dimensions of the example to use the schema you defined above, change the chart types, generate sample code out of it and more!

Cube.js Backend also provides [REST API](/rest-api) for accessing your data.

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

Vue:

```bash
$ npm i --save @cubejs-client/core
$ npm i --save @cubejs-client/vue
```

### Example Usage

#### Vanilla Javascript
Instantiate Cube.js API and then use it to fetch data. `CubejsApi.load` accepts
query, which is a plain Javascript object. [Learn more about query format
here.](query-format)

```js
import cubejs from '@cubejs-client/core';
import Chart from 'chart.js';
import chartjsConfig from './toChartjsData';

const cubejsApi = cubejs(
  'YOUR-CUBEJS-API-TOKEN',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' },
);

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

#### React
Import `cubejs` and `QueryRenderer` components, and use them to fetch the data.
In the example below we use Recharts to visualize data.

```jsx
import React from 'react';
import { LineChart, Line, XAxis, YAxis } from 'recharts';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';

const cubejsApi = cubejs(
  'YOUR-CUBEJS-API-TOKEN',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' },
);

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

#### Vue
Import `cubejs` and `QueryRenderer` components, and use them to fetch the data.
In the example below we use Vue-Chartkick to visualize data.

```javascript
<template>
  <div class="hello">
    <query-renderer :cubejs-api="cubejsApi" :query="query">
      <template v-slot="{ measures, resultSet, loading }">
        <line-chart :data="transformData(resultSet)"></line-chart>
      </template>
    </query-renderer>
  </div>
</template>

<script>
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/vue';
import Vue from 'vue';
import VueChartkick from 'vue-chartkick';
import Chart from 'chart.js';

Vue.use(VueChartkick, { adapter: Chart });

const cubejsApi = cubejs(
  'YOUR-CUBEJS-API-TOKEN',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' },
);

export default {
  name: 'HelloWorld',
  components: {
    QueryRenderer,
  },
  props: {
    msg: String,
  },
  data() {
    const query = {
      measures: ['LineItems.count', 'LineItems.quantity', 'Orders.count'],
      timeDimensions: [
        {
          dimension: 'LineItems.createdAt',
          granularity: 'month',
        },
      ],
    };

    return {
      cubejsApi,
      query,
    };
  },
  methods: {
    transformData(resultSet) {
      const seriesNames = resultSet.seriesNames();
      const pivot = resultSet.chartPivot();
      const series = [];
      seriesNames.forEach((e) => {
        const data = pivot.map(p => [p.x, p[e.key]]);
        series.push({ name: e.key, data });
      });
      return series;
    },
  },
};
</script>
```
