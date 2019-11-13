import React from "react";
import { Row } from 'antd';

const Empty = () => (
  <div
    style={{
      textAlign: "center",
      padding: 12
    }}
  >
    <h2>There are no charts on this dashboard. Use Playground Build to add one.</h2>
  </div>
);

const Dashboard = ({ children }) => children ? (
  <Row
    type="flex"
    justify="space-around"
    align="top"
    gutter={24}
    style={{
      padding: "0 12px 12px 12px",
      margin: "25px 8px"
    }}
  >
    {children}
  </Row>
) : <Empty />;

export default Dashboard;
