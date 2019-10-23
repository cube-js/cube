import React from "react";
import { Spin, Button, Alert, Typography } from "antd";
import { Link } from "react-router-dom";
import { useQuery } from "@apollo/react-hooks";
import { GET_DASHBOARD_ITEMS } from "../graphql/queries";
import ChartRenderer from "../components/ChartRenderer";
import Dashboard from "../components/Dashboard";
import DashboardItem from "../components/DashboardItem";
import PageHeader from "../components/PageHeader";

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

const DashboardPage = () => {
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
        <ChartRenderer vizState={item.vizState} />
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
    <div>
      <PageHeader
        title={<Typography.Title level={4}>Dashboard</Typography.Title>}
        button={<Link to="/explore">
          <Button type="primary">
            Add chart
          </Button>
        </Link>}
      />
      <Dashboard dashboardItems={data && data.dashboardItems}>
        {data && data.dashboardItems.map(deserializeItem).map(dashboardItem)}
      </Dashboard>
    </div>
  ) : <Empty />;
};

export default DashboardPage;
