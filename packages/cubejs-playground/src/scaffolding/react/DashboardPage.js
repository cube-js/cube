import React, { useState } from "react";
import {
  Card, Spin, Button, Menu, Dropdown, Alert
} from "antd";
import { Link } from "react-router-dom";
import { useQuery, useMutation } from "@apollo/react-hooks";
import RGL, { WidthProvider } from "react-grid-layout";
import "react-grid-layout/css/styles.css";
import "react-resizable/css/styles.css";
import { GET_DASHBOARD_QUERY, REMOVE_DASHBOARD_ITEM, UPDATE_DASHBOARD_ITEM } from "./DashboardStore";
import ChartRenderer from "./ChartRenderer";

const ReactGridLayout = WidthProvider(RGL);

const defaultLayout = (i) => ({ ...i.layout, minW: 4, minH: 8 });

const Dashboard = ({ children, dashboardItems }) => {
  const [updateDashboardItem] = useMutation(UPDATE_DASHBOARD_ITEM, {
    refetchQueries: [
      {
        query: GET_DASHBOARD_QUERY
      }
    ]
  });

  const [layout, setLayout] = useState(dashboardItems.map(i => ({ i: i.id.toString(), ...defaultLayout(i) })));
  const onLayoutChange = (newLayout) => {
    newLayout.forEach(l => {
      if (!l.w || !l.h || l.w < 4 || l.h < 4) {
        return;
      }
      const item = dashboardItems.find(i => i.id.toString() === l.i);
      const toUpdate = { x: l.x, y: l.y, w: l.w, h: l.h };
      if (item && JSON.stringify(toUpdate) !== JSON.stringify(item.layout)) {
        updateDashboardItem({ variables: { id: item.id, layout: toUpdate } });
      }
    });
    setLayout(newLayout);
  };
  return (
    <ReactGridLayout
      cols={12}
      rowHeight={50}
      layout={layout}
      onLayoutChange={onLayoutChange}
    >
      {children}
    </ReactGridLayout>
  )
};

const DashboardItemDropdown = ({ itemId }) => {
  const [removeDashboardItem, { data }] = useMutation(REMOVE_DASHBOARD_ITEM, {
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
  <Card
    title={title}
    style={{
      height: '100%',
      width: '100%'
    }}
    extra={<DashboardItemDropdown itemId={itemId} />}
  >
    {children}
  </Card>
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
    <div key={item.id} data-grid={defaultLayout(item)}>
      <DashboardItem key={item.id} itemId={item.id}>
        <ChartRenderer vizState={item.vizState} cubejsApi={cubejsApi} />
      </DashboardItem>
    </div>
  );

  return (
    <Dashboard dashboardItems={data && data.dashboard.items}>
      {data && data.dashboard.items.map(dashboardItem)}
    </Dashboard>
  );
};

export default DashboardPage;
