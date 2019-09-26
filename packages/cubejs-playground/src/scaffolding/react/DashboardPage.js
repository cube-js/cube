import React from "react";
import {
  Row, Col, Card, Spin, Button, Menu, Dropdown, Alert
} from "antd";
import { Link } from "react-router-dom";
import { useQuery, useMutation } from "@apollo/react-hooks";
import { GET_DASHBOARD_QUERY, REMOVE_DASHBOARD_ITEM } from "./DashboardStore";
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

const DashboardItemDropdown = ({ itemId }) => {
  const [removeDashboardItem] = useMutation(REMOVE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_QUERY
      }
    ]
  });

  const dashboardItemDropdownMenu = (
    <Menu>
      <Menu.Item><Link to={`/explore?itemId=${itemId}`}>Edit</Link></Menu.Item>
      <Menu.Item
        onClick={() => removeDashboardItem({ variables: { id: itemId } })}
      >
        Delete
      </Menu.Item>
    </Menu>
  );

  return (
    <Dropdown
      overlay={dashboardItemDropdownMenu}
      placement="bottomLeft"
      trigger={['click']}
    >
      <Button shape="circle" icon="menu" />
    </Dropdown>
  );
};

const DashboardItem = ({ itemId, children, title }) => (
  <Col span={24} lg={12}>
    <Card
      title={title}
      style={{
        marginBottom: "24px"
      }}
      extra={<DashboardItemDropdown itemId={itemId} />}
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

  if (error) {
    return (
      <Alert
        message="Error occured while loading your query"
        description={error.toString()}
        type="error"
      />
    );
  }

  const dashboardItem = item => (
    <DashboardItem key={item.id} itemId={item.id}>
      <ChartRenderer vizState={item.vizState} cubejsApi={cubejsApi} />
    </DashboardItem>
  );

  return <Dashboard>{data && data.dashboard.items.map(dashboardItem)}</Dashboard>;
};

export default DashboardPage;
