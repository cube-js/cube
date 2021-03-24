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
