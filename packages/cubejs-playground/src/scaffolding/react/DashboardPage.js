import React from 'react';
import {
  Row, Col, Card
} from 'antd';

const Dashboard = ({ children }) => (
  <Row type="flex" justify="space-around" align="top" gutter={24}>{children}</Row>
);

const DashboardItem = ({ children, title }) => (
  <Col span={24} lg={12}>
    <Card title={title} style={{ marginBottom: '24px' }}>
      {children}
    </Card>
  </Col>
);

const DashboardPage = () => (
  <Dashboard />
);

export default DashboardPage;
