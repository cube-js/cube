---
title: '@cubejs-client/react'
permalink: /@cubejs-client-react
category: Cube.js Frontend
subCategory: Reference
menuOrder: 3
---

`@cubejs-client/react` provides React Components for easy Cube.js integration in a React app.

## useCubeQuery

▸  **useCubeQuery**‹**TData**›(**query**: Query, **options?**: [UseCubeQueryOptions](#use-cube-query-options)) => *[UseCubeQueryResult](#use-cube-query-result)‹TData›*

A React hook for executing Cube.js queries
```js
import React from 'react';
import { Table } from 'antd';
import { useCubeQuery }  from '@cubejs-client/react';

export default function App() {
  const { resultSet, isLoading, error } = useCubeQuery({
    measures: ['Orders.count'],
    dimensions: ['Orders.createdAt.month'],
  });

  if (isLoading) {
    return <div>Loading...</div>;
  }

  if (error) {
    return <div>{error.toString()}</div>;
  }

  if (!resultSet) {
    return null;
  }

  const dataSource = resultSet.tablePivot();
  const columns = resultSet.tableColumns();

  return <Table columns={columns} dataSource={dataSource} />;
}

```

**Type parameters:**

- **TData**

**Parameters:**

Name | Type |
------ | ------ |
query | Query |
options? | [UseCubeQueryOptions](#use-cube-query-options) |

**Returns:** *[UseCubeQueryResult](#use-cube-query-result)‹TData›*

## isQueryPresent

▸  **isQueryPresent**(**query**: Query) => *boolean*

Checks whether the query is ready

**Parameters:**

Name | Type |
------ | ------ |
query | Query |

**Returns:** *boolean*

## QueryBuilder

### • **QueryBuilder** extends **React.Component** ‹[QueryBuilderProps](#query-builder-props), [QueryBuilderState](#query-builder-state)›:

`<QueryBuilder />` is used to build interactive analytics query builders. It abstracts state management and API calls to Cube.js Backend. It uses render prop technique and doesn’t render anything itself, but calls the render function instead.

**Example**
[Open in CodeSandbox](https://codesandbox.io/s/z6r7qj8wm)
```js
import React from 'react';
import ReactDOM from 'react-dom';
import { Layout, Divider, Empty, Select } from 'antd';
import { QueryBuilder } from '@cubejs-client/react';
import cubejs from '@cubejs-client/core';
import 'antd/dist/antd.css';

import ChartRenderer from './ChartRenderer';

const cubejsApi = cubejs('YOUR-CUBEJS-API-TOKEN', {
  apiUrl: 'http://localhost:4000/cubejs-api/v1',
});

const App = () => (
  <QueryBuilder
    query={{
      timeDimensions: [
        {
          dimension: 'LineItems.createdAt',
          granularity: 'month',
        },
      ],
    }}
    cubejsApi={cubejsApi}
    render={({ resultSet, measures, availableMeasures, updateMeasures }) => (
      <Layout.Content style={{ padding: '20px' }}>
        <Select
          mode="multiple"
          style={{ width: '100%' }}
          placeholder="Please select"
          onSelect={(measure) => updateMeasures.add(measure)}
          onDeselect={(measure) => updateMeasures.remove(measure)}
        >
          {availableMeasures.map((measure) => (
            <Select.Option key={measure.name} value={measure}>
              {measure.title}
            </Select.Option>
          ))}
        </Select>
        <Divider />
        {measures.length > 0 ? (
          <ChartRenderer resultSet={resultSet} />
        ) : (
          <Empty description="Select measure or dimension to get started" />
        )}
      </Layout.Content>
    )}
  />
);

const rootElement = document.getElementById("root");
ReactDOM.render(<App />, rootElement);
```

## QueryRenderer

### • **QueryRenderer** extends **React.Component** ‹[QueryRendererProps](#query-renderer-props)›:

`<QueryRenderer />` a react component that accepts a query, fetches the given query, and uses the render prop to render the resulting data

## CubeProvider

### • **CubeProvider**: *React.FC‹[CubeProviderVariables](#cube-provider-variables)›*

Cube.js context provider
```js
import React from 'react';
import cubejs from '@cubejs-client/core';
import { CubeProvider } from '@cubejs-client/react';

const API_URL = 'https://react-dashboard.cubecloudapp.dev';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.* eyJpYXQiOjE1OTE3MDcxNDgsImV4cCI6MTU5NDI5OTE0OH0.* n5jGLQJ14igg6_Hri_Autx9qOIzVqp4oYxmX27V-4T4';

const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
});

export default function App() {
  return (
    <CubeProvider cubejsApi={cubejsApi}>
      //...
    </CubeProvider>
  )
}
```

## ChartType

Ƭ **ChartType**: *"line" | "bar" | "table" | "area"*

## CubeProviderVariables

Name | Type |
------ | ------ |
children | React.ReactNode |
cubejsApi | CubejsApi |

## MemberUpdater

Name | Type |
------ | ------ |
add |  (**member**: [TMember](#t-member)) => *void* |
remove |  (**member**: [TMember](#t-member)) => *void* |
update |  (**member**: [TMember](#t-member), **updateWith**: [TMember](#t-member)) => *void* |

## QueryBuilderProps

Name | Type | Description |
------ | ------ | ------ |
cubejsApi | CubejsApi | `CubejsApi` instance to use |
defaultChartType? | [ChartType](#chart-type) | - |
disableHeuristics? | boolean | Defaults to `false`. This means that the default heuristics will be applied. For example: when the query is empty and you select a measure that has a default time dimension it will be pushed to the query. |
query? | Query | Default query |
render |  (**renderProps**: [QueryBuilderRenderProps](#query-builder-render-props)) => *React.ReactNode* | - |
setQuery? |  (**query**: Query) => *void* | Called by the `QueryBuilder` when the query state has changed. Use it when state is maintained outside of the `QueryBuilder` component. |
setVizState? |  (**vizState**: [VizState](#viz-state)) => *void* | - |
stateChangeHeuristics? |  (**state**: [QueryBuilderState](#query-builder-state)) => *[QueryBuilderState](#query-builder-state)* | A function that accepts the `newState` just before it's applied. You can use it to override the **defaultHeuristics** or to tweak the query or the vizState in any way. |
vizState? | [VizState](#viz-state) | - |
wrapWithQueryRenderer? | boolean | - |

## QueryBuilderRenderProps

Name | Type | Description |
------ | ------ | ------ |
availableDimensions | [TAvailableDimension](#t-available-dimension)[] | An array of available dimensions to select. They are loaded via the API from Cube.js Backend. |
availableMeasures | [TAvailableMeasure](#t-available-measure)[] | An array of available measures to select. They are loaded via the API from Cube.js Backend. |
availableSegments | [TMember](#t-member)[] | An array of available segments to select. They are loaded via the API from Cube.js Backend. |
availableTimeDimensions | [TAvailableDimension](#t-available-dimension)[] | An array of available time dimensions to select. They are loaded via the API from Cube.js Backend. |
dimensions | string[] | - |
isQueryPresent | boolean | Indicates whether the query is ready to be displayed or not |
measures | string[] | - |
segments | string[] | - |
timeDimensions | Filter[] | - |
updateDimensions | [MemberUpdater](#member-updater) | - |
updateMeasures | [MemberUpdater](#member-updater) | - |
updateQuery |  (**query**: Query) => *void* | Used for partial of full query update |
updateSegments | [MemberUpdater](#member-updater) | - |
updateTimeDimensions | [MemberUpdater](#member-updater) | - |

## QueryBuilderState

Ƭ **QueryBuilderState**: *[VizState](#viz-state) & object*

## QueryRendererProps

Name | Type | Description |
------ | ------ | ------ |
cubejsApi | CubejsApi | `CubejsApi` instance to use |
loadSql? | "only" &#124; boolean | Indicates whether the generated by `Cube.js` SQL Code should be requested. See [rest-api#sql](rest-api#api-reference-v-1-sql). When set to `only` then only the request to [/v1/sql](/docs/rest-api#api-reference-v-1-sql) will be performed. When set to `true` the sql request will be performed along with the query request. Will not be performed if set to `false` |
queries? | object | - |
query | Query | Analytic query. [Learn more about it's format](query-format) |
render |  (**QueryRendererRenderProp**: any) => *void* | Output of this function will be rendered by the `QueryRenderer` |
resetResultSetOnChange? | boolean | When `true` the **resultSet** will be reset to `null` first on every state change |
updateOnlyOnStateChange? | boolean | - |

## QueryRendererRenderProp

Name | Type |
------ | ------ |
error | Error &#124; null |
loadingState | object |
resultSet | ResultSet &#124; null |

## TAvailableDimension

Ƭ **TAvailableDimension**: *[TMember](#t-member) & object*

## TAvailableMeasure

Ƭ **TAvailableMeasure**: *[TMember](#t-member) & object*

## TMember

Name | Type |
------ | ------ |
name | string |
shortTitle | string |
title | string |

## TMemberType

Ƭ **TMemberType**: *"time" | "number" | "string" | "boolean"*

## UseCubeQueryOptions

Name | Type | Description |
------ | ------ | ------ |
cubejsApi? | CubejsApi | A `CubejsApi` instance to use. Taken from the context if the param is not passed |
resetResultSetOnChange? | boolean | - |
skip? | boolean | Query execution will be skipped when `skip` is set to `true`. You can use this flag to avoid sending incomplete queries. |
subscribe? | boolean | When `true` the resultSet will be reset to `null` first |

## UseCubeQueryResult

Name | Type |
------ | ------ |
error | Error &#124; null |
isLoading | boolean |
resultSet | ResultSet‹TData› &#124; null |

## VizState

Name | Type |
------ | ------ |
chartType? | [ChartType](#chart-type) |
pivotConfig? | PivotConfig |
shouldApplyHeuristicOrder? | boolean |
