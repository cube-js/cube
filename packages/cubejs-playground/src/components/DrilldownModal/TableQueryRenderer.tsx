import React from 'react';
import { Table } from 'antd';

import useDeepMemo from '../../hooks/deep-memo';

const TABLE_PAGE_SIZE = 50;

const formatTableData = (columns, data) => {
  function flatten(columns = []) {
    return columns.reduce<any>((memo, column: any) => {
      if (column.children) {
        return [...memo, ...flatten(column.children)];
      }

      return [...memo, column];
    }, []);
  }

  const typeByIndex = flatten(columns).reduce((memo, column) => {
    return { ...memo, [column.dataIndex]: column };
  }, {});

  function formatValue(value, { type, format }: any = {}) {
    if (value == undefined) {
      return value;
    }

    if (type === 'boolean') {
      if (typeof value === 'boolean') {
        return value.toString();
      } else if (typeof value === 'number') {
        return Boolean(value).toString();
      }

      return value;
    }

    if (type === 'number' && format === 'percent') {
      return [parseFloat(value).toFixed(2), '%'].join('');
    }

    return value.toString();
  }

  function format(row) {
    return Object.fromEntries(
      Object.entries(row).map(([dataIndex, value]) => {
        return [dataIndex, formatValue(value, typeByIndex[dataIndex])];
      })
    );
  }

  return data.map(format);
};

export function TableQueryRenderer({ resultSet, pivotConfig }) {
  const [tableColumns, dataSource] = useDeepMemo(() => {
    const columns = resultSet.tableColumns(pivotConfig);
    return [
      columns,
      formatTableData(columns, resultSet.tablePivot(pivotConfig)),
    ];
  }, [resultSet, pivotConfig]);

  return (
    <Table
      pagination={{ pageSize: TABLE_PAGE_SIZE }}
      columns={tableColumns}
      dataSource={dataSource}
    />
  );
}
