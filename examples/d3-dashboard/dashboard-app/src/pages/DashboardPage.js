import React from "react";
import Grid from "@material-ui/core/Grid";
import Typography from "@material-ui/core/Typography";
import ChartRenderer from "../components/ChartRenderer";
import Dashboard from "../components/Dashboard";
import DashboardItem from "../components/DashboardItem";
const DashboardItems = [
  {
    id: 0,
    name: "Orders last 14 days",
    vizState: {
      query: {
        measures: ["Orders.count"],
        timeDimensions: [
          {
            dimension: "Orders.createdAt",
            granularity: "day",
            dateRange: "last 14 days"
          }
        ],
        filters: []
      },
      chartType: "line"
    }
  },
  {
    id: 1,
    name: "Orders Status by Customers City",
    vizState: {
      query: {
        measures: ["Orders.count"],
        dimensions: ["Users.city", "Orders.status"],
        timeDimensions: [
          {
            dimension: "Orders.createdAt",
            dateRange: "last year"
          }
        ]
      },
      chartType: "bar",
      pivotConfig: {
        x: ["Users.city"],
        y: ["Orders.status", "measures"]
      }
    }
  },
  {
    id: 3,
    name: "Orders by Product Categories Over Time",
    vizState: {
      query: {
        measures: ["Orders.count"],
        timeDimensions: [
          {
            dimension: "Orders.createdAt",
            granularity: "month",
            dateRange: "last year"
          }
        ],
        dimensions: ["ProductCategories.name"]
      },
      chartType: "area"
    }
  },
  {
    id: 3,
    name: "Orders by Price Range",
    vizState: {
      query: {
        measures: ["Orders.count"],
        filters: [
          {
            "dimension": "Orders.price",
            "operator": "set"
          }
        ],
        dimensions: ["Orders.priceRange"]
      },
      chartType: "pie"
    }
  }
];

const DashboardPage = () => {
  const dashboardItem = item => (
    <Grid item xs={12} lg={6} key={item.id}>
      <DashboardItem title={item.name}>
        <ChartRenderer vizState={item.vizState} />
      </DashboardItem>
    </Grid>
  );

  const Empty = () => (
    <div
      style={{
        textAlign: "center",
        padding: 12
      }}
    >
      <Typography variant="h5" color="inherit">
        There are no charts on this dashboard. Use Playground Build to add one.
      </Typography>
    </div>
  );

  return DashboardItems.length ? (
    <Dashboard>{DashboardItems.map(dashboardItem)}</Dashboard>
  ) : (
    <Empty />
  );
};

export default DashboardPage;
