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

### Scoped Slot Props

- `resultSet`: A `resultSet` is an object containing data obtained from the query.  If this object is not defined, it means that the data is still being fetched. [ResultSet](@cubejs-client-core#result-set) object provides a convient interface for data munipulation.
- `error`: Error will be defined if an error has occurred while fetching the query.
- `loadingState`: Provides information about the state of the query loading.

## QueryBuilder
`<QueryBuilder />` is used to  build interactive analytics query builders. It abstracts state management and API calls to Cube.js Backend. It uses scoped slot props technique.

### Props

- `query`: default query.
- `cubejsApi`: `CubejsApi` instance to use. Required.
- `defaultChartType`: default value of chart type. Default: 'line'.

### Scoped Slot Props

- `measurers`, `dimensions`, `segments`, `timeDimensions`, `filters` - arrays of
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
- `resultSet`, `error`, `loadingState` - same as `<QueryRenderer />` [Scoped slot params.](#query-scoped-slot-props)

### Example
[Open in CodeSandbox](https://codesandbox.io/s/3rlxjkv2p)
```js
<template>
  <div class="hello">
    <query-builder :cubejs-api="cubejsApi" :query="query">
      <template v-slot="{ resultSet }">
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
