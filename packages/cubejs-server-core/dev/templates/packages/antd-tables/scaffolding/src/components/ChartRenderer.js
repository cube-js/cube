import { Row, Col, Statistic, Table } from 'antd';

const TypeToChartComponent = {
  number: ({ resultSet }) => (
    <Row type="flex" justify="center" align="middle" style={{ height: '100%' }}>
      <Col>
        {resultSet
          .seriesNames()
          .map(s => (
            <Statistic value={resultSet.totalRow()[s.key]} />
          ))}
      </Col>
    </Row>
  ),
  table: ({ resultSet, pivotConfig }) => (
    <Table
      pagination={false}
      columns={resultSet.tableColumns(pivotConfig)}
      dataSource={resultSet.tablePivot(pivotConfig)}
    />
  )
};
