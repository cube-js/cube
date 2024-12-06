import { PivotConfig, ResultSet } from '@cubejs-client/core';
import { Table } from 'antd';

import { ChartType } from './types.ts';

interface ChartViewerProps {
  resultSet: ResultSet;
  pivotConfig: PivotConfig;
  chartType: ChartType;
}

export function ChartViewer(props: ChartViewerProps) {
  const { resultSet, pivotConfig } = props;

  const columns = resultSet?.tableColumns().map((c) => {
    return { ...c, dataIndex: c.key, title: c.shortTitle };
  });

  return (
    <Table columns={columns} dataSource={resultSet?.tablePivot(pivotConfig)} />
  );
}
