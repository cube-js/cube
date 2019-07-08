---
title: '@cubejs-client/vue'
permalink: /@cubejs-client-vue
category: Cube.js Frontend
subCategory: Reference
menuOrder: 4
---

`@cubejs-client/vue` provides Vue Components for easy integration Cube.js
into Vue.js app.

## QueryRenderer

`<QueryRenderer />` Vue component takes a query, fetches the given query, and uses the slot scoped props to render the resulting data.

### Props

- `query`: analytic query. [Learn more about it's format](query-format).
- `cubejsApi`: `CubejsApi` instance to use.

### Slots

#### Default Slot

##### Slot Props

- `resultSet`: A `resultSet` is an object containing data obtained from the query. [ResultSet](@cubejs-client-core#result-set) object provides a convient interface for data munipulation.

#### Empty Slot

This slot functions as a empty/loading state in which if the query is loading or empty you can show
something in the meantime

#### Error Slot

##### Slot Props

- `error`: will show the details from error.
- `sqlQuery`: will show tried query

### Example
```js
<template>
  <div class="hello">
    <query-renderer :cubejs-api="cubejs" :query="query" v-if="cubejs">
      <template v-slot="{ resultSet }">
        <component :is="type" :result="resultSet"/>
      </template>

      <template v-slot:empty>
        <div class="loading-container">
          <loading-ring class="loading"/>
        </div>
      </template>
    </query-renderer>
  </div>
</template>

<script>
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/vue';
import ChartRenderer from "./ChartRenderer.vue";

const cubejsApi = cubejs(
  'YOUR-CUBEJS-API-TOKEN',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' },
);

export default {
  name: "HelloWorld",
  components: {
    QueryRenderer,
    ChartRenderer
  },
  data() {
    const query = {
      measures: ["LineItems.count", "LineItems.quantity", "Orders.count"],
      timeDimensions: [
        {
          dimension: "LineItems.createdAt",
          granularity: "month"
        }
      ]
    };

    return {
      cubejsApi,
      query
    };
  }
};
</script>
```

## QueryBuilder
`<QueryBuilder />` is used to  build interactive analytics query builders. It abstracts state management and API calls to Cube.js Backend. It uses scoped slot props technique.

### Props

- `query`: default query.
- `cubejsApi`: `CubejsApi` instance to use. Required.
- `defaultChartType`: default value of chart type. Default: 'line'.

### Slots

#### Default Slot

##### Slot Props

- `resultSet`: A `resultSet` is an object containing data obtained from the query. [ResultSet](@cubejs-client-core#result-set) object provides a convient interface for data munipulation.

#### Empty Slot

This slot functions as a empty/loading state in which if the query is loading or empty you can show
something in the meantime

#### Error Slot

##### Slot Props

- `error`: will show the details from error.
- `sqlQuery`: will show tried query

#### Builder Slot

- `measures`, `dimensions`, `segments`, `timeDimensions`, `filters` - arrays of
selected query builder members.
- `availableMeasures`, `availableDimensions`, `availableTimeDimensions`,
`availableSegments` - arrays of available to select members. They are loaded via
API from Cube.js Backend.
- `addMeasures`, `addDimensions`, `addSegments`, `addTimeDimensions` - function to control the adding of new members to query builder
- `removeMeasures`, `removeDimensions`, `removeSegments`, `removeTimeDimensions` - function to control the removing of member to query builder
- `setMeasures`, `setDimensions`, `setSegments`, `setTimeDimensions` - function to control the set of members to query builder
- `updateMeasures`, `updateDimensions`, `updateSegments`, `updateTimeDimensions` - function to control the update of member to query builder
- `chartType` - string, containing currently selected chart type.
- `updateChartType` - function-setter for chart type.
- `isQueryPresent` - Bool indicating whether is query ready to be displayed or
    not.
- `query` - current query, based on selected members.

### Example
[Open in CodeSandbox](https://codesandbox.io/s/3rlxjkv2p)
```js
<template>
  <div class="hello">
    <query-builder :cubejs-api="cubejsApi" :query="query">
      <template v-slot:builder="scope">
        <div class="report-details-parameters">
          <dropdown
            placeholder="Chart"
            :options="dropdown"/>
          <dropdown
            :options="scope.availableMeasures"
            placeholder="Measure"/>
          <dropdown
            :options="scope.availableDimensions"
            placeholder="Dimensions"/>
        </div>
      </template>
      
      <template v-slot="{ resultSet }">
        <component :is="type" :result="resultSet" class="chart"/>
      </template>
      
      <template v-slot:empty>
        <div class="loading-container">
          <loading-ring class="loading"/>
        </div>
      </template>
    </query-builder>
  </div>
</template>

<script>
import cubejs from '@cubejs-client/core';
import { QueryBuilder } from '@cubejs-client/vue';
import ChartRenderer from "./ChartRenderer.vue";

const cubejsApi = cubejs(
  'YOUR-CUBEJS-API-TOKEN',
  { apiUrl: 'http://localhost:4000/cubejs-api/v1' },
);

export default {
  name: "HelloWorld",
  components: {
    QueryBuilder,
    ChartRenderer
  },
  data() {
    return {
      cubejsApi,
    };
  }
};
</script>
```
