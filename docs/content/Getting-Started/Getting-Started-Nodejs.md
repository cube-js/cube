---
title: Getting Started with Cube using Node.js
permalink: /getting-started/nodejs
---

This guide walks you through getting started with Cube and requires
[Node.js][link-nodejs] to be installed.

## 1. Scaffold the project

Run the following command to get started with Cube, specifying the project name
and optionally your database using the `-d` flag.

```bash
$ npx cubejs-cli create <project name> -d <database type>
```

You can find [all available databases here][ref-connecting-to-the-database]. For
example, to create a new project with the Postgres database, use the following:

```bash
$ npx cubejs-cli create hello-world -d postgres
```

Once run, the `create` command will create a new project directory that contains
the scaffolding for your new Cube project. This includes all the files necessary
to spin up the Cube backend and some example schema files to highlight the
format of the Cube Data Schema layer.

The `.env` file in this project directory contains placeholders for the relevant
database credentials. Setting credentials is covered in the [Connecting to the
Database][ref-connecting-to-the-database] section.

## 2. Define Your Data Schema

Cube uses [Data Schema][ref-cubejs-schema] to generate and execute SQL.

It acts as an ORM for your database and it is flexible enough to model
everything from simple counts to cohort retention and funnel analysis. [Read
more about Cube Schema][ref-cubejs-schema].

You can generate schema files using [Developer Playground][ref-dev-playground].
To do so, you can start the dev server from project directory like this:

```bash
$ npm run dev
```

Then go to `http://localhost:4000` and use Developer Playground to generate
schema files.

### <--{"id" : "2. Define Your Data Schema"}--> Manually creating Data Schema files

You can also add schema files to the `schema` folder manually:

```javascript
// schema/users.js

cube(`Users`, {
  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    age: {
      type: `number`,
      sql: `age`,
    },

    createdAt: {
      type: `time`,
      sql: `createdAt`,
    },

    country: {
      type: `string`,
      sql: `country`,
    },
  },
});
```

## 3. Visualize Results

The Cube client provides set of methods to access Cube API and to work with
query result. The client itself doesn't provide any visualizations and is
designed to work with existing chart libraries. You can find more information
about [the Cube client as well as our frontend integrations
here][ref-frontend-intro].

As a shortcut you can run your dev server first:

```bash
$ npm run dev
```

Then open `http://localhost:4000` in a browser to see visualization examples.
This will open a Developer Playground app. You can change the metrics and
dimensions of the example to use the schema you defined earlier, change the
chart types, generate sample code and more!

Cube also provides a [REST API](/backend/rest/reference/api) for accessing your
data programmatically.

### <--{"id" : "3. Visualize Results"}--> Cube Client Installation

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

Angular:

```bash
$ npm i --save @cubejs-client/core
$ npm i --save @cubejs-client/ngx
```

### <--{"id" : "3. Visualize Results"}--> Example Usage

#### Vanilla Javascript

Instantiate the Cube API and then use it to fetch data. `CubejsApi.load()`
accepts a query, which is a plain Javascript object.
[Learn more about the query format here](query-format).

```javascript
import cubejs from '@cubejs-client/core';
import Chart from 'chart.js';
import chartjsConfig from './toChartjsData';

const cubejsApi = cubejs('YOUR-CUBEJS-API-TOKEN', {
  apiUrl: 'http://localhost:4000/cubejs-api/v1',
});

const resultSet = await cubejsApi.load({
  measures: ['Stories.count'],
  timeDimensions: [
    {
      dimension: 'Stories.time',
      dateRange: ['2015-01-01', '2015-12-31'],
      granularity: 'month',
    },
  ],
});
const context = document.getElementById('myChart');
new Chart(context, chartjsConfig(resultSet));
```

#### React

Import `cubejs` and `QueryRenderer` components, and use them to fetch the data.
In the example below, we use `recharts` to visualize data.

```jsx
import React from 'react';
import { LineChart, Line, XAxis, YAxis } from 'recharts';
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';

const cubejsApi = cubejs('YOUR-CUBEJS-API-TOKEN', {
  apiUrl: 'http://localhost:4000/cubejs-api/v1',
});

export default () => {
  return (
    <QueryRenderer
      query={{
        measures: ['Stories.count'],
        dimensions: ['Stories.time.month'],
      }}
      cubejsApi={cubejsApi}
      render={({ resultSet }) => {
        if (!resultSet) {
          return 'Loading...';
        }

        return (
          <LineChart data={resultSet.rawData()}>
            <XAxis dataKey="Stories.time" />
            <YAxis />
            <Line type="monotone" dataKey="Stories.count" stroke="#8884d8" />
          </LineChart>
        );
      }}
    />
  );
};
```

#### Vue

Import `cubejs` and `QueryRenderer` components, and use them to fetch the data.
In the example below, we use `vue-chartkick` to visualize data.

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

const cubejsApi = cubejs('YOUR-CUBEJS-API-TOKEN', {
  apiUrl: 'http://localhost:4000/cubejs-api/v1',
});

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
        const data = pivot.map((p) => [p.x, p[e.key]]);
        series.push({ name: e.key, data });
      });
      return series;
    },
  },
};
</script>
```

#### Angular

Add CubejsClientModule to your `app.module.ts` file:

```typescript
import { CubejsClientModule } from '@cubejs-client/ngx';
import { environment } from '../../environments/environment';

const cubejsOptions = {
  token: environment.CUBEJS_API_TOKEN,
  options: { apiUrl: environment.CUBEJS_API_URL }
};

@NgModule({
  declarations: [
    ...
  ],
  imports: [
    ...,
    CubejsClientModule.forRoot(cubejsOptions)
  ],
  providers: [...],
  bootstrap: [...]
})
export class AppModule { }
```

Then you can inject `CubejsClient` into your components or services:

```typescript
import { CubejsClient } from '@cubejs-client/ngx';

export class AppComponent {
  constructor(private cubejs: CubejsClient) {}

  ngOnInit() {
    this.cubejs
      .load({
        measures: ['some_measure'],
      })
      .subscribe(
        (resultSet) => {
          this.data = resultSet.chartPivot();
        },
        (err) => console.log('HTTP Error', err)
      );
  }
}
```

## 4. Deploy to Production

Cube has first-class deployment support for [Docker][link-docker]:

```bash
$ docker run --rm \
  --name cubejs-docker-demo \
  -e CUBEJS_API_SECRET=<YOUR-API-SECRET> \
  -e CUBEJS_DB_HOST=<YOUR-DB-HOST-HERE> \
  -e CUBEJS_DB_NAME=<YOUR-DB-NAME-HERE> \
  -e CUBEJS_DB_USER=<YOUR-DB-USER-HERE> \
  -e CUBEJS_DB_PASS=<YOUR-DB-PASS-HERE> \
  -e CUBEJS_DB_TYPE=postgres \
  --volume "$(pwd):/cube/conf" \
  <YOUR-USERNAME>/cubejs-docker-demo
```

For more information on deploying our official Docker image, please consult the
[Deployment Guide][ref-docker-deployment-guide].

[link-docker]: https://www.docker.com/
[link-nodejs]: https://nodejs.org/en/
[ref-dev-playground]: /dev-tools/dev-playground
[ref-frontend-intro]: /frontend-introduction
[ref-docker-deployment-guide]: /deployment/platforms/docker
[ref-connecting-to-the-database]: /connecting-to-the-database
[ref-cubejs-schema]: /schema/getting-started
