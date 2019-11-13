import React from "react";
import { Card, Col } from "antd";

const DashboardItem = ({ children, title }) => (
  <Col span={24} lg={12} style={{ marginBottom: '24px' }}>
    <Card
      title={title}
      style={{
        height: "100%",
        width: "100%"
      }}
    >
      {children}
    </Card>
  </Col>
);

export default DashboardItem;
