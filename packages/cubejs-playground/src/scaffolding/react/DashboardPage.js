import React from "react";
import {
  Row, Col, Card, Spin
} from "antd";
import { useQuery } from '@apollo/react-hooks';
import { GET_DASHBOARD_QUERY } from "./DashboardStore";
import ChartRenderer from "./ChartRenderer";

const Dashboard = ({ children }) => (
  <Row
    type="flex"
    justify="space-around"
    align="top"
    gutter={24}
    style={{
      padding: "0 25px 25px 25px",
      margin: "25px"
    }}
  >
    {children}
  </Row>
);

const DashboardItem = ({ children, title }) => (
  <Col span={24} lg={12}>
    <Card
      title={title}
      style={{
        marginBottom: "24px"
      }}
    >
      {children}
    </Card>
  </Col>
);

const DashboardPage = ({ cubejsApi }) => {
  const { loading, error, data } = useQuery(GET_DASHBOARD_QUERY);

  if (loading) {
    return <Spin />;
  }

  const dashboardItem = (item) => (
    <DashboardItem key={item.id}>
      <ChartRenderer vizState={item.vizState} cubejsApi={cubejsApi} />
    </DashboardItem>
  );

  return (
    <Dashboard>
      {data.dashboard.items.map(dashboardItem)}
    </Dashboard>
  );
};

export default DashboardPage;
