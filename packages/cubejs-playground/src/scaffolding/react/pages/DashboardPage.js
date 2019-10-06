import React from "react";
import {
  Card, Spin, Button, Menu, Dropdown, Alert, Modal
} from "antd";
import { Link } from "react-router-dom";
import { useQuery, useMutation } from "@apollo/react-hooks";
import RGL, { WidthProvider } from "react-grid-layout";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";
import {
  GET_DASHBOARD_ITEMS
} from "../graphql/queries";
import {
  UPDATE_DASHBOARD_ITEM,
  DELETE_DASHBOARD_ITEM
} from "../graphql/mutations";
import ChartRenderer from "../components/ChartRenderer";

const ReactGridLayout = WidthProvider(RGL);

const deserializeItem = i => ({
  ...i,
  layout: JSON.parse(i.layout) || {},
  vizState: JSON.parse(i.vizState)
});

const defaultLayout = i => ({
  x: i.layout.x || 0,
  y: i.layout.y || 0,
  w: i.layout.w || 4,
  h: i.layout.h || 8,
  minW: 4,
  minH: 8
});

const Dashboard = ({ children, dashboardItems }) => {
  const [updateDashboardItem] = useMutation(UPDATE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_ITEMS
      }
    ]
  });

  const onLayoutChange = newLayout => {
    newLayout.forEach(l => {
      const item = dashboardItems.find(i => i.id.toString() === l.i);
      const toUpdate = JSON.stringify({
        x: l.x,
        y: l.y,
        w: l.w,
        h: l.h
      });

      if (item && toUpdate !== item.layout) {
        updateDashboardItem({
          variables: {
            id: item.id,
            input: { layout: toUpdate }
          }
        });
      }
    });
  };

  return (
    <ReactGridLayout cols={12} rowHeight={50} onLayoutChange={onLayoutChange}>
      {children}
    </ReactGridLayout>
  );
};

const DashboardItemDropdown = ({ itemId }) => {
  const [removeDashboardItem] = useMutation(DELETE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_ITEMS
      }
    ]
  });
  const dashboardItemDropdownMenu = (
    <Menu>
      <Menu.Item>
        <Link to={`/explore?itemId=${itemId}`}>Edit</Link>
      </Menu.Item>
      <Menu.Item
        onClick={() =>
          Modal.confirm({
            title: "Are you sure you want to delete this item?",
            okText: "Yes",
            okType: "danger",
            cancelText: "No",

            onOk() {
              removeDashboardItem({
                variables: {
                  id: itemId
                }
              });
            }
          })
        }
      >
        Delete
      </Menu.Item>
    </Menu>
  );
  return (
    <Dropdown
      overlay={dashboardItemDropdownMenu}
      placement="bottomLeft"
      trigger={["click"]}
    >
      <Button shape="circle" icon="menu" />
    </Dropdown>
  );
};

const DashboardItem = ({ itemId, children, title }) => (
  <Card
    title={title}
    style={{
      height: "100%",
      width: "100%"
    }}
    extra={<DashboardItemDropdown itemId={itemId} />}
  >
    {children}
  </Card>
);

const DashboardPage = ({ cubejsApi }) => {
  const { loading, error, data } = useQuery(GET_DASHBOARD_ITEMS);

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
    <div key={item.id} data-grid={defaultLayout(item)}>
      <DashboardItem key={item.id} itemId={item.id} title={item.name}>
        <ChartRenderer vizState={item.vizState} cubejsApi={cubejsApi} />
      </DashboardItem>
    </div>
  );

  const Empty = () => (
    <div
      style={{
        textAlign: "center",
        padding: 12
      }}
    >
      <h2>There are no charts on this dashboard</h2>
      <Link to="/explore">
        <Button type="primary" size="large" icon="plus">
          Add chart
        </Button>
      </Link>
    </div>
  );

  return !data || data.dashboardItems.length ? (
    <Dashboard dashboardItems={data && data.dashboardItems}>
      {data && data.dashboardItems.map(deserializeItem).map(dashboardItem)}
    </Dashboard>
  ) : <Empty />;
};

export default DashboardPage;
