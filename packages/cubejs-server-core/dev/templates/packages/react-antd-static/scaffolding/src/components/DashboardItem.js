import React from "react";
import { Card } from "antd";

const DashboardItem = ({ children, title }) => (
  <Card
    title={title}
    style={{
      height: "100%",
      width: "100%"
    }}
  >
    {children}
  </Card>
);

export default DashboardItem;
