---
order: 5
title: "How Everything Works"
---

All relevant code resides inside the `src/components/Grid.js` component. We'll explore it from top to bottom.

In the imports, you can see this React hook imported from the Cube.js client React package. We'll use it later to send a query to Cube.js API:

```js
// Cube.js React hook
import { useCubeQuery } from '@cubejs-client/react';
```

After that, we'll import AG Grid and its' React integration, which has a convenient `AgGridReact` component that we'll use. However, in complex scenarios, you'll need to use the `[onGridReady](https://www.ag-grid.com/react-grid/grid-interface/#access-the-grid--column-api-1)` callback to get access to the Grid API and tinker with it directly. Also, note that AG Grid provides style definitions and a few [themes](https://www.ag-grid.com/react-grid/themes-provided/) you can import and use.

```js
// AG Grid React components & library
import { AgGridColumn, AgGridReact } from 'ag-grid-react';
import 'ag-grid-enterprise';

// AG Grid styles
import 'ag-grid-community/dist/styles/ag-grid.css';
import 'ag-grid-community/dist/styles/ag-theme-alpine.css';
```

Next, meet the Cube.js query in JSON format. I hope you remember this query from Developer Playground where it was available on the "JSON Query" tab:

```js
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
```

Now we jump into the functional `Grid` component. Time for React stuff! Here we define a state variable where we'll store the rows to be displayed in our table. Also, we use the `useCubeQuery` hook to send the request to Cube.js API. Then, in `useEffect`, we get the result, transform it into tabular format with the convenient `tablePivot` method, and assign it to the state. (Remapping is needed because Cube.js returns column names in the `Cube.measure` and `Cube.dimension` format but AG Grid doesn't work with dots in the names.)

```js
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
```

Then we extract the column names from the dataset. We'll use them later:

```js
const columnDefs = [
  ...query.dimensions,
  ...query.measures,
].map(field => ({
  headerName: field.split('.')[1],
  field: field.replace('.', '-'),
}));
```

Time for JSX! Note that the `AgGridReact` component is wrapped with a `div.ag-theme-alpine` to apply the custom Ag Grid styles. Also, note how default column styles and properties are set.

The last three lines are the most important ones because they activate the pivot table, enable a convenient sidebar you might know from Excel or similar software, and also wire the row data into the component:

```js
<div className='ag-theme-alpine' style={{ height: 700 }}>
  <AgGridReact
    defaultColDef={{
      flex: 1,
      minWidth: 150,
      sortable: true,
      resizable: true,
    }}
    // ...
    autoGroupColumnDef={{ minWidth: 250 }}
    pivotMode={true}    // !!!
    sideBar={'columns'} // !!!
    rowData={rowData}   // !!!
  >
```

Here's the most complex part. To transform the row data into a pivot table, we need to specify the column or columns used on the left side and on the top side of the table. With the `pivot` option we specify that data is pivoted (the top side of the table) by the "status" column. With the `rowGroup` option we specify that the data is grouped by the "name" column.

Also, we use `aggFunc` to specify the default aggregation function used to queeze the pivoted values into one as `sum`. Then, we list all allowed aggregation functions under `allowedAggFuncs `.

```js
{columnDefs.map((column, i) => {
  // ...

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
```

Here's how these functions are implemented. Nothing fancy, just a little bit of JavaScript functional code for minimum, maximum, sum, and average:

```js
aggFuncs={{
  'min': ({ values }) => values.reduce((min, value) => Math.min(min, Number(value)), 0),
  'max': ({ values }) => values.reduce((max, value) => Math.max(max, Number(value)), 0),
  'sum': ({ values }) => values.reduce((sum, value) => sum + Number(value), 0),
  'avg': ({ values }) => (values.reduce((sum, value) => sum + Number(value), 0) / values.length).toFixed(0),
}}
```

You can click on "Values" to change the aggregation function used for every column, or set it programmatically as specified above:

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/nz0lkw3h4qikgwtm363i.png)

**And that's all, folks! 🎉** Thanks to AG Grid and Cube.js, we had to write only a few tiny bits of code to create a pivot table.

![Alt Text](https://dev-to-uploads.s3.amazonaws.com/uploads/articles/pq0xxdnziks2copbfy3r.png)

I strongly encourage you to [spend some time](https://react-pivot-table-demo.cube.dev) with this pivot table and explore what AG Grid is capable of. You'll find column sorting, a context menu with CSV export, drag-and-drop in the sidebar, and much more. Don't hesitate to check AG Grid [docs](https://www.ag-grid.com/react-grid/) to learn more about these features.

**Thank you for following this tutorial to learn more about [Cube.js](https://cube.dev?utm_source=dev-to&utm_medium=post&utm_campaign=react-pivot-table), build a pivot table, and explore how to work with AG Grid. I wholeheartedly hope that you enjoyed it 😇**

Please don't hesitate to like and bookmark this post, write a comment, and give a star to [Cube.js](https://github.com/cube-js/cube.js) or [AG Grid](https://github.com/ag-grid/ag-grid/) on GitHub. I hope that you'll try Cube.js and AG Grid in your next production gig or your next pet project.

Good luck and have fun!
