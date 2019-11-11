import React from "react";
import { Col, Row } from "antd";
import ChartRenderer from "../components/ChartRenderer";
import Dashboard from "../components/Dashboard";
import DashboardItem from "../components/DashboardItem";
const DashboardItems = [
  {
    id: 10,
    name: "Users Online",
    vizState: {
      query: {
        measures: ["Events.online"],
        timeDimensions: [],
        filters: []
      },
      chartType: "number"
    },
    size: 8
  },
  {
    id: 2,
    name: "Total Button Clicks",
    vizState: {
      query: {
        measures: ["Events.buttonClick"],
        timeDimensions: [],
        filters: []
      },
      chartType: "number"
    },
    size: 8
  },
  {
    id: 3,
    name: "Total Page Views",
    vizState: {
      query: {
        measures: ["Events.pageView"],
        timeDimensions: [],
        filters: []
      },
      chartType: "number"
    },
    size: 8
  },
  {
    id: 4,
    name: "Real Time Events",
    vizState: {
      query: {
        measures: ["EventsBucketed.events"],
        dimensions: ["EventsBucketed.quarter"],
        timeDimensions: [
          {
            dimension: "EventsBucketed.time",
            dateRange: "last 150 seconds"
          }
        ],
        order: {
          "EventsBucketed.quarter": "asc"
        },
      },
      chartType: "line"
    },
    size: 12
  },
  {
    id: 4,
    name: "Events per Minute ago",
    vizState: {
      query: {
        measures: ["Events.count"],
        dimensions: ["Events.minutesAgo"],
        filters: [
          {
            member: "Events.minutesAgo",
            operator: "lte",
            values: ["10"]
          }
        ],
        order: {
          "Events.minutesAgo": "desc"
        },
        limit: 10
      },
      chartType: "bar"
    },
    size: 12
  },
  {
    id: 4,
    name: "Last Events",
    vizState: {
      query: {
        measures: [],
        timeDimensions: [
          {
            dimension: "Events.timestamp"
          }
        ],
        dimensions: [
          "Events.anonymousId",
          "Events.eventType",
          "Events.minutesAgoHumanized",
          "Events.timestamp"
        ],
        filters: [],
        order: {
          "Events.timestamp": "desc"
        },
        limit: 10
      },
      chartType: "table"
    },
    size: 24
  },
];

const DashboardPage = () => {
  const dashboardItem = item => (
    <Col
      span={24}
      lg={item.size}
      key={item.id}
      style={{
        marginBottom: "24px"
      }}
    >
      <DashboardItem title={item.name}>
        <ChartRenderer vizState={item.vizState} />
      </DashboardItem>
    </Col>
  );

  const Empty = () => (
    <div
      style={{
        textAlign: "center",
        padding: 12
      }}
    >
      <h2>There are no charts on this dashboard</h2>
    </div>
  );

  return DashboardItems.length ? (
    <div
      style={{
        padding: "0 12px 12px 12px",
        margin: "10px 8px"
      }}
    >
      <Row
        style={{
          padding: "0 20px"
        }}
      ></Row>
      <Row>
        <Dashboard dashboardItems={DashboardItems}>
          {DashboardItems.map(dashboardItem)}
        </Dashboard>
      </Row>
    </div>
  ) : (
    <Empty />
  );
};

export default DashboardPage;
