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
        measures: ["OnlineUsers.count"],
        timeDimensions: [
          {
            dimension: "OnlineUsers.lastSeen",
            dateRange: "last 120 seconds"
          }
        ]
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
        measures: ["Events.count"],
        timeDimensions: [],
        filters: [{
          member: `Events.type`,
          operator: `equals`,
          values: ['track']
        }]
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
        measures: ["Events.count"],
        timeDimensions: [],
        filters: [{
          member: `Events.type`,
          operator: `equals`,
          values: ['page']
        }]
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
        measures: ["Events.count"],
        timeDimensions: [
          {
            dimension: "Events.time",
            granularity: "second",
            dateRange: "last 60 seconds"
          }
        ],
        order: {
          "Events.time": "asc"
        },
      },
      chartType: "line"
    },
    size: 12
  },
  {
    id: 5,
    name: "Last Events",
    vizState: {
      query: {
        measures: [],
        timeDimensions: [
          {
            dimension: "Events.time"
          }
        ],
        dimensions: [
          "Events.anonymousId",
          "Events.type",
          "Events.time"
        ],
        filters: [],
        order: {
          "Events.time": "desc"
        },
        limit: 6
      },
      chartType: "table"
    },
    size: 12
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
          <DashboardItem title="Architecture">
            <div>
              <img width="100%" src="https://ucarecdn.com/4efc3459-88b4-4a54-8596-8a0e6fa16814/" alt="Architecture" />
            </div>
          </DashboardItem>
        </Dashboard>
      </Row>
    </div>
  ) : (
    <Empty />
  );
};

export default DashboardPage;
