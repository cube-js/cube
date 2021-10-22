---
title: '@cubejs-client/vue'
permalink: /@cubejs-client-vue
category: Cube.js Frontend
subCategory: Reference
menuOrder: 4
---

`@cubejs-client/vue` provides Vue Components to easily integrate Cube.js
within a Vue.js app.

## QueryBuilder
`<QueryBuilder />` is used to build interactive analytics query builders. It abstracts state management and API calls to Cube.js Backend. It uses scoped slot props technique.

### <--{"id" : "QueryBuilder"}--> Props

Name | Type | Description |
------ | ------ | ------ |
cubejsApi | [CubejsApi](@cubejs-client-core#cubejs-api) | `CubejsApi` instance to use |
initialChartType? | [ChartType](#types-chart-type) | The type of chart to display initially. Default is `line`. |
disableHeuristics? | boolean | Defaults to `false`. This means that the default heuristics will be applied. For example: when the query is empty and you select a measure that has a default time dimension it will be pushed to the query. |
query? | Query | Query parameters ([learn more about the format](/query-format)). This property is reactive - if you change the object here, the internal query values will be overwritten. This is not two-way. |
stateChangeHeuristics? |  (**state**: [QueryBuilderState](#types-query-builder-query-builder-state)) => *[QueryBuilderState](#types-query-builder-query-builder-state)* | A function that accepts the `newState` just before it's applied. You can use it to override the **defaultHeuristics** or to tweak the query or the vizState in any way. |
initialVizState? | [VizState](#types-viz-state) | - |

### <--{"id" : "QueryBuilder"}--> Slots

#### Default Slot

##### Slot Props

- `resultSet`: A `resultSet` is an object containing data obtained from the query. [ResultSet](@cubejs-client-core#result-set) object provides a convenient interface for data manipulation.

#### Empty Slot

This slot functions as an empty/loading state; when the query is loading or empty, you can show something in the meantime.

#### Error Slot

This slot will be rendered if any error happens while the query is loading or rendering.

##### Slot Props

- `error`: the error.
- `sqlQuery`: the attempted query.

#### Builder Slot

- `measures`, `dimensions`, `segments`, `timeDimensions`, `filters` - arrays containing the
selected query builder members.
- `availableMeasures`, `availableDimensions`, `availableTimeDimensions`,
`availableSegments` - arrays containing available members to select. They are loaded via
API from Cube.js Backend.
- `addMeasures`, `addDimensions`, `addSegments`, `addTimeDimensions` - functions to control the adding of new members to query builder.
- `removeMeasures`, `removeDimensions`, `removeSegments`, `removeTimeDimensions` - functions to control the removing of members to query builder.
- `setMeasures`, `setDimensions`, `setSegments`, `setTimeDimensions` - functions to control the setting of members to query builder.
- `updateMeasures`, `updateDimensions`, `updateSegments`, `updateTimeDimensions` - functions to control the updating of members to query builder.
- `chartType` - string containing currently selected chart type.
- `updateChartType` - function-setter for chart type.
- `isQueryPresent` - bool indicating whether is query ready to be displayed or not.
- `query` - current query, based on selected members.
- `setLimit`, `removeLimit` - functions to control the number of results returned.
- `setOffset`, `removeOffset` - functions to control the number of rows skipped before results returned. Use with limit to control pagination.

### <--{"id" : "QueryBuilder"}--> Example
<a href="https://codesandbox.io/s/vuejs-query-builder-with-vuejs-forked-5fn8z" target="_blank">Open in CodeSandbox</a>

```html
<template>
  <query-builder :cubejs-api="cubejsApi" :query="query">
    <template #builder="{ measures, setMeasures, availableMeasures }">
      <multiselect
        placeholder="Please Select"
        label="Title"
        track-by="name"
        multiple
        :customLabel="customLabel"
        :value="measures"
        :options="availableMeasures"
        @input="(...args) => set(setMeasures, ...args)"
      />
    </template>

    <template #default="{ resultSet }">
      <chart-renderer v-if="resultSet" :result-set="resultSet" />
    </template>

    <template #empty>Loading...</template>
  </query-builder>
</template>

<script>
import cubejs from '@cubejs-client/core';
import Multiselect from 'vue-multiselect';
import { QueryBuilder } from '@cubejs-client/vue';
import ChartRenderer from './ChartRenderer.vue';

const API_URL = 'https://awesome-ecom.gcp-us-central1.cubecloudapp.dev/cubejs-api/v1';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE1OTQ2NjY4OTR9.0fdi5cuDZ2t3OSrPOMoc3B1_pwhnWj4ZmM3FHEX7Aus';

const cubejsApi = cubejs(CUBEJS_TOKEN, { apiUrl: API_URL });

export default {
  name: 'QueryBuilderExample',
  components: {
    Multiselect,
    QueryBuilder,
    ChartRenderer,
  },
  data() {
    const query = {
      measures: [],
      timeDimensions: [
        {
          dimension: 'LineItems.createdAt',
          granularity: 'month',
        },
      ],
    };

    return {
      cubejsApi,
      selected: undefined,
      query,
    };
  },
  methods: {
    customLabel(a) {
      return a.title;
    },
    set(setMeasures, value) {
      setMeasures(value.map((e) => e.name));
    },
  },
};
</script>
```

## QueryRenderer

`<QueryRenderer />` Vue component takes a query, fetches the given query, and uses the slot scoped props to render the resulting data.

### <--{"id" : "QueryRenderer"}--> Props

Name | Type | Description |
------ | ------ | ------ |
cubejsApi | CubejsApi | `CubejsApi` instance to use |
loadSql? | "only" &#124; boolean | Indicates whether the generated by `Cube.js` SQL Code should be requested. See [rest-api#sql](rest-api#v-1-sql). When set to `only` then only the request to [/v1/sql](rest-api#v-1-sql) will be performed. When set to `true` the sql request will be performed along with the query request. Will not be performed if set to `false` |
queries? | object | - |
query | Query | Analytic query. [Learn more about it's format](query-format)

### <--{"id" : "QueryRenderer"}--> Slots

#### Default Slot

##### Slot Props

- `resultSet`: A `resultSet` is an object containing data obtained from the query. [ResultSet](@cubejs-client-core#result-set) object provides a convenient interface for data manipulation.

#### Empty Slot

This slot functions as an empty/loading state; when the query is loading or empty, you can show something in the meantime.

#### Error Slot

This slot will be rendered if any error happens while the query is loading or rendering.

##### Slot Props

- `error`: the error.
- `sqlQuery`: the attempted query.

### <--{"id" : "QueryRenderer"}--> Example
```html
<template>
  <query-renderer :cubejs-api="cubejsApi" :query="query" v-if="cubejsApi">
    <template #default="{ resultSet }">
      <!--      render a chart here using the `resultSet`-->
    </template>

    <template #empty> Loading... </template>
  </query-renderer>
</template>

<script>
import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/vue';

const cubejsApi = cubejs('YOUR-CUBEJS-API-TOKEN', {
  apiUrl: 'http://localhost:4000/cubejs-api/v1',
});

export default {
  name: 'QueryRendererExample',
  components: {
    QueryRenderer,
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
};
</script>
<style src="vue-multiselect/dist/vue-multiselect.min.css"></style>

<style scoped>
h3 {
  margin: 40px 0 0;
}
ul {
  list-style-type: none;
  padding: 0;
}
li {
  display: inline-block;
  margin: 0 10px;
}
a {
  color: #42b983;
}
</style>

```

## Types

### <--{"id" : "Types"}--> ChartType

> **ChartType**: *"line" | "bar" | "table" | "area" | "number" | "pie"*

### <--{"id" : "Types"}--> QueryBuilderState

Name | Type |
------ | ------ |
query | [Query](@cubejs-client-core#query) |
chartType? | [ChartType](#types-chart-type) |

### <--{"id" : "Types"}--> VizState

Name | Type |
------ | ------ |
chartType? | [ChartType](#types-chart-type) |
pivotConfig? | [PivotConfig](@cubejs-client-core#pivot-config) |
shouldApplyHeuristicOrder? | boolean |
