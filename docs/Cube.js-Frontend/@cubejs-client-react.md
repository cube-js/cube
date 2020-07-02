---
---

## QueryBuilder

### • **QueryBuilder** :

`<QueryBuilder />` is used to build interactive analytics query builders. It abstracts state management and API calls to Cube.js Backend. It uses render prop technique and doesn’t render anything itself, but calls the render function instead.

Example: **orderMembers**
```js
// Ex: `orderMembers`
// [
//   {
//     id: 'Users.country',
//     title: 'Users Country',
//     order: 'desc'
//   },
//   //...
// ]

import React from 'react';
import ReactDOM from 'react-dom';
import { Button, Layout, Divider, Empty, Select, Row, Col } from 'antd';
import { QueryBuilder } from '@cubejs-client/react';
import cubejs from '@cubejs-client/core';
import 'antd/dist/antd.css';

import ChartRenderer from './ChartRenderer';

const API_URL = 'https://react-dashboard.cubecloudapp.dev';
const CUBEJS_TOKEN =
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9. * eyJpYXQiOjE1OTE3MDcxNDgsImV4cCI6MTU5NDI5OTE0OH0. * n5jGLQJ14igg6_Hri_Autx9qOIzVqp4oYxmX27V-4T4';

const cubejsApi = cubejs(CUBEJS_TOKEN, {
  apiUrl: `${API_URL}/cubejs-api/v1`,
});

const App = () => {
  return (
    <QueryBuilder
      query={{
        measures: ['Orders.count'],
        timeDimensions: [
          {
            dimension: 'Orders.createdAt',
            granularity: 'month',
            dateRange: ['2016-01-01', '2016-12-31'],
          },
        ],
      }}
      cubejsApi={cubejsApi}
      render={({ resultSet, measures, orderMembers, updateOrder }) => {
        return (
          <Layout.Content style={{ padding: 20 }}>
            {orderMembers.map((orderMember, index) => (
              <Row gutter={[8, 16]} align="middle">
                <Col span={6}>{orderMember.title}</Col>

                <Col>
                  <Select
                    value={orderMember.order}
                    placeholder="Select order"
                    onSelect={(order) => updateOrder.set(orderMember.id,  * order)}
                  >
                    {['none', 'asc', 'desc'].map((order) => (
                      <Select.Option key={order} value={order}>
                        {order}
                      </Select.Option>
                    ))}
                  </Select>
                </Col>

                <Col>
                  <Button
                    onClick={() => {
                      index - 1 >= 0 && updateOrder.reorder(index, index - 1);
                    }}
                  >
                    Move up
                  </Button>

                  <Button
                    onClick={() => {
                      index + 1 < orderMembers.length &&
                        updateOrder.reorder(index, index + 1);
                    }}
                  >
                    Move down
                  </Button>
                </Col>
              </Row>
            ))}
            <Divider />
            {measures.length > 0 ? (
              <ChartRenderer resultSet={resultSet} />
            ) : (
              <Empty description="Select measure or dimension to get started"  * />
            )}
          </Layout.Content>
        );
      }}
    />
  );
};

const rootElement = document.getElementById('root');
ReactDOM.render(<App />, rootElement);
```

## QueryRenderer

### • **QueryRenderer** :

`<QueryRenderer />` a react component that accepts a query, fetches the given query, and uses the render prop to render the resulting data

## isQueryPresent

▸  **isQueryPresent**(**query**: Query): *boolean*

Checks whether the query is ready

**Parameters:**

Name | Type |
------ | ------ |
query | Query |

**Returns:** *boolean*

## useCubeQuery

▸  **useCubeQuery**‹**TData**›(**query**: Query, **options?**: [UseCubeQueryOptions](#use-cube-query-options)): *[UseCubeQueryResult](#use-cube-query-result)‹TData›*

**Type parameters:**

- **TData**

**Parameters:**

Name | Type |
------ | ------ |
query | Query |
options? | [UseCubeQueryOptions](#use-cube-query-options) |

**Returns:** *[UseCubeQueryResult](#use-cube-query-result)‹TData›*

## ChartType

Ƭ **ChartType** : *"line" | "bar" | "table" | "area"*

## CubeProviderVariables

Ƭ **CubeProviderVariables** : *object*

Name | Type |
------ | ------ |
children | React.ReactNode |
cubejsApi | CubejsApi |

## MemberUpdater

Ƭ **MemberUpdater** : *object*

Name | Type |
------ | ------ |
add | function |
remove | function |
update | function |

## QueryBuilderProps

Ƭ **QueryBuilderProps** : *object*

Name | Type | Description |
------ | ------ | ------ |
cubejsApi | CubejsApi | `CubejsApi` instance to use |
defaultChartType | [ChartType](#chart-type) | - |
disableHeuristics | boolean | - |
query? | Query | Default query |
render | function | - |
setQuery | function | Called by the `QueryBuilder` when the query state has changed. Use it when state is maintained outside of the `QueryBuilder` component. |
setVizState | function | - |
stateChangeHeuristics | function | todo: wip |
vizState | [VizState](#viz-state) | - |
wrapWithQueryRenderer | boolean | - |

## QueryBuilderRenderProps

Ƭ **QueryBuilderRenderProps** : *object*

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
updateQuery | function | Used for partial of full query update |
updateSegments | [MemberUpdater](#member-updater) | - |
updateTimeDimensions | [MemberUpdater](#member-updater) | - |

## QueryRendererProps

Ƭ **QueryRendererProps** : *object*

Name | Type | Description |
------ | ------ | ------ |
cubejsApi | CubejsApi | `CubejsApi` instance to use |
loadSql? | "only" &#124; boolean | Indicates whether the generated by `Cube.js` SQL Code should be requested. See [rest-api#sql](rest-api#api-reference-v-1-sql). When set to `only` |
queries? | object | - |
query | Query | Analytic query. [Learn more about it's format](query-format) |
render | function | Output of this function will be rendered by the `QueryRenderer` |
resetResultSetOnChange? | boolean | - |
updateOnlyOnStateChange? | boolean | - |

## QueryRendererRenderProp

Ƭ **QueryRendererRenderProp** : *object*

Name | Type |
------ | ------ |
error | Error &#124; null |
loadingState | object |
resultSet | ResultSet &#124; null |

## TAvailableDimension

Ƭ **TAvailableDimension** : *[TMember](#t-member) & [TMemberType](#t-member-type) & object*

## TAvailableMeasure

Ƭ **TAvailableMeasure** : *[TMember](#t-member) & [TMemberType](#t-member-type) & object*

## TMember

Ƭ **TMember** : *object*

Name | Type |
------ | ------ |
name | string |
shortTitle | string |
title | string |

## TMemberType

Ƭ **TMemberType** : *object*

Name | Type |
------ | ------ |
type | "time" &#124; "number" &#124; "string" &#124; "boolean" |

## UseCubeQueryOptions

Ƭ **UseCubeQueryOptions** : *object*

Name | Type | Description |
------ | ------ | ------ |
cubejsApi? | CubejsApi | A `CubejsApi` instance to use. Taken from the context if the param is not passed |
resetResultSetOnChange? | boolean | - |
skip? | boolean | Query execution will be skipped when `skip` is set to `true`. You can use this flag to avoid sending incomplete queries. |
subscribe? | boolean | When `true` the resultSet will be reset to `null` first |

## UseCubeQueryResult

Ƭ **UseCubeQueryResult** : *object*

Name | Type |
------ | ------ |
error | Error &#124; null |
isLoading | boolean |
resultSet | ResultSet‹TData› &#124; null |

## VizState

Ƭ **VizState** : *object*

## CubeProvider

### • **CubeProvider** : *React.FC‹[CubeProviderVariables](#cube-provider-variables)›*
