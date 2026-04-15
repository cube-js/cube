import React from 'react';
import { Table } from 'antd';
import { formatValue } from '@cubejs-client/core/format';

import { useDeepMemo } from '../../hooks/deep-memo';

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

  function format(row) {
    return Object.fromEntries(
      Object.entries(row).map(([dataIndex, value]) => {
        const { type, format: columnFormat, currency, granularity } = typeByIndex[dataIndex] || {};
        return [
          dataIndex,
          formatValue(value, {
            type,
            format: columnFormat,
            currency,
            granularity,
            emptyPlaceholder: '',
          }),
        ];
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
