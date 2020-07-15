# Vue Query Builder with Cube.js

## Introduction

It often happens that we need to do a dashboard with filters. And we need to control our queries because their complexity increases exponentially. To solve this problem we can use a query builder.

The QueryBuilder provides a convenient, fluent interface to creating and running database queries. It can be used to perform most database operations. It is designed to help developers build interactive analytics query builders.

Our <QueryBuilder /> abstracts state management and API calls to Cube.js Backend. It uses render prop and doesn’t render anything itself, but calls the render function instead. This way it gives maximum flexibility to building a custom-tailored UI with a minimal API. It uses a scoped slot props technique.

This tutorial we will build query builder with Cube.js.

Below you can see the demo of the query builder we're going to build today.

GIF

link to demo & link to source code.

## Setup a Demo Backend

if you already have Cube.js Backend up and running you can skip this step.

First, let’s install Cube.js CLI and create a new application with a Postgres database.

```
$ npm install -g cubejs-cli
$ cubejs create -d postgres vue-query-builder
```

We host a dump with sample data for tutorials. It is a simple “E-commerce database” with orders, products, product categories, and users tables.

```
$ curl http://cube.dev/downloads/ecom-dump.sql > ecom-dump.sql
$ createdb ecom
$ psql --dbname ecom -f ecom-dump.sql
```

Once you have data in your database, change the content of the .env file inside your Cube.js directory to the following. It sets the credentials to access the database, as well as a secret to generate auth tokens.

```
UBEJS_DB_NAME=ecom
CUBEJS_DB_TYPE=postgres
CUBEJS_API_SECRET=SECRET
```

Now that we have everything configured, the last step is to generate a Cube.js schema based on some of our tables and start the dev server.

![https://s3-us-west-2.amazonaws.com/secure.notion-static.com/a484a2db-1658-4243-b501-1392763ba5f8/Cube.js_Demo.gif](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/a484a2db-1658-4243-b501-1392763ba5f8/Cube.js_Demo.gif)

If you open http://localhost:4000 in your browser you will access Cube.js Playground. It is a development environment, which generates the Cube.js schema, creates scaffolding for charts, and more. It has its own query builder, which lets you generate charts with different charting libraries.

Now, let’s move on to building our own query building.

## Generating template

Now let's generate frontend template. For this we will use [Vue CLI](https://cli.vuejs.org/) and [Vuetify](https://vuetifyjs.com/).

[To install Vue CLI](https://cli.vuejs.org/guide/installation.html) run this command:

```
npm install -g @vue/cli
```

Then [we can generate a project and add vuetify](https://vuetifyjs.com/en/getting-started/quick-start/#quick-start).

```
$ vue create dashboard-app
// navigate to new project directory
$ cd dashboard-app
```

Now that you have an instantiated project, you can add the [Vuetify Vue CLI package](https://github.com/vuetifyjs/vue-cli-plugins) using the cli.

```
$ vue add vuetify
```

Then start the project

```
$ yarn serve
```

Congratulations! We create template🎉

Let's remove unnecessary code. And prepare layout. Go to `src/App.vue` and use this code:

```jsx
<template>
  <v-app>
    <v-app-bar
      app
      color="#43436B"
      dark
    >
      <div class="d-flex align-center">
        <v-img
          alt="Vuetify Logo"
          class="shrink mr-2"
          contain
          src="https://material-ui-dashboard.cubecloudapp.dev/images/logo/logo.svg"
          transition="scale-transition"
        />
      </div>
    </v-app-bar>

    <v-main>
      <HelloWorld/>
    </v-main>
  </v-app>
</template>

<script>
import HelloWorld from './components/HelloWorld'

export default {
  name: 'App',

  components: {
    HelloWorld
  },
}
</script>
```

And clear `src/HelloWorld.vue`

```jsx
<template>
  <v-container>
  </v-container>
</template>

<script>
export default {
  name: 'HelloWorld',
  data: () => ({
  })
}
</script>
```

We received an empty template

![https://s3-us-west-2.amazonaws.com/secure.notion-static.com/93affa0d-62f8-4d9d-a729-8bc6ae8bedff/Screenshot_2020-07-09_at_18.25.52.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/93affa0d-62f8-4d9d-a729-8bc6ae8bedff/Screenshot_2020-07-09_at_18.25.52.png)

Now we can start 🎉

## Building a Query Builder

The <QueryBuilder /> component is used to build interactive analytics query builders. It abstracts state management and API calls to Cube.js Backend. It uses a scoped slot props technique.

Besides render, the required props are [cubejsApi](https://cube.dev/docs/@cubejs-client-core#cubejs-api) and [query](https://cube.dev/docs/query-format). 

We can import QueryBuilder in @cubejs-client/vue dependency. To install this dependency run the command: 

```jsx
npm install --save @cubejs-client/vue
```

```jsx
<template>
  <v-container>
    <v-row class="text-center">
      <query-builder :cubejs-api="cubejsApi" :query="query">
        <template v-slot="{ resultSet }">
          <v-col cols="12">
            <!-- render something-->
          </v-col>
        </template>
      </query-builder>
    </v-row>
  </v-container>
</template>

<script>
import cubejs from '@cubejs-client/core'
import { QueryBuilder } from '@cubejs-client/vue'

const cubejsApi = cubejs(
  'CUBEJS_TOKEN',
  { apiUrl: 'CUBEJS_BACKEND_URL' }
)
export default {
  name: 'HelloWorld',

  components: {
    QueryBuilder
  },
  data: () => {
    const query = {
      // some query
    }

    return {
      cubejsApi,
      query
    }
  }
}
</script>
```

The properties of `queryBuilder` can be split into categories based on what element they are referred to.

For example, to render and update measures, you can to use `measures`, `availableMeasures`, `addMeasures`, `removeMeasures`, `setMeasures`, `updateMeasures`. 

`measures` is an array of already selected measures. It is usually empty in the beginning (unless you passed a default `query` prop). 

`availableMeasures` is an array of all measures loaded via API from your Cube.js data schema. Both `measures` and `availableMeasures` are arrays of objects with `name`, `title`, `shortTitle`, and `type` keys.

`addMeasures` is a function to control the adding of new members to query builder.

`removeMeasures` is a  function to control the removing of members to query builder.

`setMeasures` is a  function to control the setting of members to query builder.

`updateMeasures` is a  function to control the updating of members to query builder.

Just like measures we can use Dimensions, Segments, and TimeDimensions. [You can find the full list of properties in the documentation.](https://cube.dev/docs/@cubejs-client-vue#query-builder-slots)

Now, using these properties, we can render a UI to manage measures and render a simple line chart, which will dynamically change the content based on the state of the query builder.

To render chart we need chart.

Let's install chart. Open main.js file and add:

```diff
import Vue from 'vue'
import App from './App.vue'
import vuetify from './plugins/vuetify'
+ import Chart from 'chart.js'
+ import VueChartkick from 'vue-chartkick'

Vue.config.productionTip = false
+ Vue.use(VueChartkick, { adapter: Chart })

new Vue({
  vuetify,
  render: h => h(App)
}).$mount('#app')
```

Then install dependencies, run the commands:

```diff
$ npm install --save chart.js
$ npm install --save vue-chartkick
```

Now we can add select, and chart. Edit `src/components/HelloWord.vue` file with the following content.

```jsx
<template xmlns:v-slot="http://www.w3.org/1999/XSL/Transform">
  <v-container>
    <v-row class="text-center">
      <query-builder :cubejs-api="cubejsApi" :query="query" style="width: 100%">
        <template v-slot:builder="{measures,setMeasures,availableMeasures}"
        >
          <v-row>
            <v-col cols="3">
              <v-select
                multiple
                :value="measures.map(i => (i.name))"
                @change="setMeasures"
                :items="availableMeasures.map(i => (i.name))"
              />
            </v-col>
          </v-row>

        </template>

        <template v-slot="{ resultSet }">
          <v-col cols="12" v-if="resultSet">
            <line-chart :data="series(resultSet)"></line-chart>
          </v-col>
        </template>
      </query-builder>
    </v-row>
  </v-container>
</template>

<script>
import cubejs from '@cubejs-client/core'
import { QueryBuilder } from '@cubejs-client/vue'

const API_URL = process.env.NODE_ENV === 'production' ? '' : 'http://localhost:4000'
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTE4NjM4MDEsImV4cCI6MTU5NDQ1NTgwMX0.NW6yiMgiZz_LCnkRn-dunzyqTRO9K7L-k5FpNn2-iCA'
const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`
})

export default {
  name: 'HelloWorld',

  components: {
    QueryBuilder
  },
  data: () => {
    const query = {
      limit: 100,
      measures: [
        'Orders.count'
      ],
      timeDimensions: [
        {
          dimension: 'LineItems.createdAt',
          granularity: 'month'
        }
      ]
    }

    return {
      cubejsApi,
      innerMeasures: [],
      query
    }
  },
  methods: {
    series (resultSet) {
      const seriesNames = resultSet.seriesNames()
      const pivot = resultSet.chartPivot()
      const series = []
      seriesNames.forEach((e) => {
        const data = pivot.map(p => [p.x, p[e.key]])
        series.push({ name: e.key, data })
      })
      return series
    }
  }
}
</script>
```

Congratulations! Now we can dynamically select measure and build query to our database. 

![https://s3-us-west-2.amazonaws.com/secure.notion-static.com/cff8c108-4690-48fe-871c-1538e2c78525/Screenshot_2020-07-09_at_21.56.30.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/cff8c108-4690-48fe-871c-1538e2c78525/Screenshot_2020-07-09_at_21.56.30.png)

Okay, we can get Orders to count. But it is not enough. What if I want to see the number of completed orders? Let's do it! We need to add `Dimensions` control select.

In `HelloWorld.vue` add this code:

```diff
<template>
  <v-container>
    <v-row class="text-center">
      <query-builder :cubejs-api="cubejsApi" :query="query" style="width: 100%">
-        <template v-slot:builder="{measures,setMeasures,availableMeasures}"
+        <template v-slot:builder="{measures,setMeasures,availableMeasures, dimensions, setDimensions, availableDimensions}"
        >
          <v-row>
            <v-col cols="3">
              <v-select
                multiple
                :value="measures.map(i => (i.name))"
                @change="setMeasures"
                :items="availableMeasures.map(i => (i.name))"
              />
            </v-col>
+            <v-col cols="3">
+              <v-select
+                multiple
+                :value="dimensions.map(i => (i.name))"
+                @change="setDimensions"
+                :items="availableDimensions.map(i => (i.name))"
+              />
            </v-col>
          </v-row>

        </template>

// ....

```

Now we can do more interesting queries!🎉 

![https://s3-us-west-2.amazonaws.com/secure.notion-static.com/855fc053-d9b1-468a-9a84-3862f716350a/Screenshot_2020-07-10_at_17.37.52.png](https://s3-us-west-2.amazonaws.com/secure.notion-static.com/855fc053-d9b1-468a-9a84-3862f716350a/Screenshot_2020-07-10_at_17.37.52.png)

Let's find only the number of completed orders that cost more than 100. To resolve this request we need to add filters. And if use QueryBuilder it is done very easily.

Also, it is worth checking the source code of a more complicated query builder from Cube.js Playground. [You can find it on Github here](https://github.com/cube-js/cube.js/tree/master/packages/cubejs-playground).
