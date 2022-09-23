import React from "react";
import { Row } from "antd";

const Dashboard = ({ children }) => (
  <Row
    type="flex"
    justify="space-around"
    align="top"
    gutter={24}
    style={{
      margin: "25px 0"
    }}
  >
    {children}
  </Row>
);

export default Dashboard;
