import React from "react";
import { Row } from 'antd';

const Dashboard = ({ children }) => (
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
);

export default Dashboard;
