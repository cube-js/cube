---
order: 4
title: "How to Add a Pivot Table"
---

Okay, I'll be honest, Cube.js Developer Playground has one more feature to be explored and used for the greater good.

Let's go to the "Dashboard App" tab where you can generate the code for a front-end application with a dashboard. There's a variety of templates for different frameworks (React and Angular included) and charting libraries but you can always choose to "create your own".

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/i/43ljijihw21cpknz4i22.png)

Let's choose "React", "React Antd Dynamic", "Bizcharts", and click "OK". In just a few seconds you'll have a newly created front-end app in the `dashboard-app` folder. Click "Start dashboard app" to run it, or do the same by navigating to `dashboard-app` and running:

```bash
npm run start
```

Believe it or not, this dashboard app will allow you to run the same queries you've already run the Developer Playground. On the "Explore" tab, you can create a query, tailor the chart, and then click "Add to dashboard". On the "Dashboard" tab, you'll see the result.

**Impressive? We'll go further than that, and replace the dashboard with the pivot table right now.**

We'll need to follow a series of simple steps to add AG Grid, tune it, review the result, and understand how everything works. I should say that AG Grid has excellent documentation with versions for [vanilla JavaScript](https://www.ag-grid.com/javascript-grid/), [React](https://www.ag-grid.com/react-grid/), [Angular](https://www.ag-grid.com/angular-grid/), and [Vue](https://www.ag-grid.com/vue-grid/). However, here's an even more condensed version of the steps you need to follow to set up AG Grid.

**First, let's install the AG Grid packages.** Make sure to switch to the `dashboard-app` folder now. AG Grid can be installed via [packages or modules](https://www.ag-grid.com/javascript-grid/packages-modules/), but the former way is simpler. Let's run in the console:

```bash
npm install --save ag-grid-enterprise ag-grid-react
```

Note that we're installing `ag-grid-enterprise` version. There's also `ag-grid-community` that contains a subset of the enterprise features but the [pivot table feature](https://www.ag-grid.com/react-grid/pivoting/) is included in the enterprise version only. It's going to work but it will print a giant warning in the console until you obtain a license:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/zbsrmkyr5oky570tey7w.png)

**Second, let's create a pivot table component.** Add a new file at the `src/components/Grid.js` location with the following contents. Basically, it sets AG Grid up, adds data from Cube.js API, and does the pivoting. It's not very lengthy, and we'll break this code down in a few minutes:

```js
import React, { useEffect, useState } from 'react';
import { useCubeQuery } from '@cubejs-client/react';
import { Button, Space, Layout } from 'antd';
import { AgGridColumn, AgGridReact } from 'ag-grid-react';
import 'ag-grid-enterprise';
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-alpine.css';

const query = {
  'order': {
    'Orders.count': 'desc',
  },
  'measures': [
    'Orders.count',
    'LineItems.price',
    'LineItems.quantity',
  ],
  'dimensions': [
    'Products.name',
    'Orders.status',
    'Users.city',
  ],
};

const Grid = () => {
  const [ rowData, setRowData ] = useState([]);
  const { resultSet } = useCubeQuery(query);

  useEffect(() => {
    if (resultSet) {
      setRowData(resultSet
        .tablePivot()
        .map(row => Object
          .keys(row)
          .reduce((object, key) => ({
            ...object,
            [key.replace('.', '-')]: row[key],
          }), {}),
        ),
      );
    }
  }, [ resultSet ]);

  const columnDefs = [
    ...query.dimensions,
    ...query.measures,
  ].map(field => ({
    headerName: field.split('.')[1],
    field: field.replace('.', '-'),
  }));

  return (
    <Layout>
      <Layout.Header style={{ backgroundColor: '#43436B' }}>
        <Space size='large'>
          <a href='https://cube.dev' target='_blank' rel='noreferrer'>
            <img src='https://cubejs.s3-us-west-2.amazonaws.com/downloads/logo-full.svg' alt='Cube.js' />
          </a>
          <Space>
            <Button href='https://github.com/cube-js/cube.js' target='_blank' ghost>GitHub</Button>
            <Button href='https://slack.cube.dev' target='_blank' ghost>Slack</Button>
          </Space>
        </Space>
      </Layout.Header>
      <div className='ag-theme-alpine' style={{ height: 700 }}>
        <AgGridReact
          defaultColDef={{
            flex: 1,
            minWidth: 150,
            sortable: true,
            resizable: true,
          }}
          aggFuncs={{
            'min': ({ values }) => values.reduce((min, value) => Math.min(min, Number(value)), 0),
            'max': ({ values }) => values.reduce((max, value) => Math.max(max, Number(value)), 0),
            'sum': ({ values }) => values.reduce((sum, value) => sum + Number(value), 0),
            'avg': ({ values }) => (values.reduce((sum, value) => sum + Number(value), 0) / values.length).toFixed(0),
          }}
          autoGroupColumnDef={{ minWidth: 250 }}
          pivotMode={true}
          sideBar={'columns'}
          rowData={rowData}
        >
          {columnDefs.map((column, i) => {
            const name = column.field.replace('-', '.');
            const isDimension = Object.values(query.dimensions).indexOf(name) !== -1;
            const isMeasure = Object.values(query.measures).indexOf(name) !== -1;

            return (
              <AgGridColumn
                key={i}
                headerName={column.headerName}
                field={column.field}
                enablePivot={true}
                enableRowGroup={isDimension}
                enableValue={isMeasure}
                pivot={column.headerName === 'status'}
                rowGroup={column.headerName === 'name'}
                allowedAggFuncs={[ 'sum', 'max', 'avg', 'min' ]}
                aggFunc={isMeasure ? 'sum' : null}
              />
            );
          })}
        </AgGridReact>
      </div>
    </Layout>
  );
};

export default Grid;
```

To make everything work, now go to `src/App.js` and change a few lines there to add this new `Grid` component to the view:

```diff
+ import Grid from './components/Grid';
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
+   <Grid />
  </Layout>;

  // ...
```

**Believe it or not, we're all set! 🎉** Feel free to start your `dashboard-app` again with `npm run start` and prepare to be amused. Here's our data grid:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/pq0xxdnziks2copbfy3r.png)

You can even turn "Pivot Mode" off with the knob in the top right corner, remove all measures and dimensions from "Row Groups" and "Values", and behold the raw ungrouped and unpivoted data as fetched from Cube.js API:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/u7bj4b8jg8r44m6w586w.png)

Amazing! Let's break the code down and review the features of AG Grid! 🔀
