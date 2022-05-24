import React from "react";
import cubejs from "@cubejs-client/core";
import { QueryRenderer } from "@cubejs-client/react";
import { Spin } from "antd";
import {
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend,
  BarChart,
  Bar
} from "recharts";

const CartesianChart = ({ resultSet, children, ChartComponent }) => (
  <ResponsiveContainer width="100%" height={350}>
    <ChartComponent data={resultSet.chartPivot()}>
      <XAxis dataKey="x" />
      <YAxis />
      <CartesianGrid />
      {children}
      <Legend />
      <Tooltip />
    </ChartComponent>
  </ResponsiveContainer>
);

const cubejsApi = cubejs(
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTMyODIzNDQsImV4cCI6MTY1NTg3NDM0NH0.6__5oRpMmh8dEbBmhN-tkFOVc-B8CNU8IkxX7E_z5XI",
  {
    apiUrl: "https://inherent-lynx.aws-us-east-1.cubecloudapp.dev/cubejs-api/v1"
  }
);

const renderChart = ({
  resultSet,
  error,
  pivotConfig,
  onDrilldownRequested
}) => {
  if (error) {
    return <div>{error.toString()}</div>;
  }

  if (!resultSet) {
    return <Spin />;
  }

  return (
    <CartesianChart resultSet={resultSet} ChartComponent={BarChart}>
      {resultSet.seriesNames().map((series) => {
        return (
          <Bar
            key={series.key}
            stackId="a"
            dataKey={series.key}
            name={series.title}
            fill={"#141446"}
          />
        );
      })}
    </CartesianChart>
  );
};

const ChartRenderer = () => {
  return (
    <QueryRenderer
      query={{
        measures: ["Fraud.amount"],
        timeDimensions: [],
        order: {
          "Fraud.amount": "desc"
        },
        dimensions: ["Fraud.type"]
      }}
      cubejsApi={cubejsApi}
      resetResultSetOnChange={false}
      render={(props) =>
        renderChart({
          ...props,
          chartType: "bar",
          pivotConfig: {
            x: ["Fraud.type"],
            y: ["measures"],
            fillMissingDates: true,
            joinDateRange: false
          }
        })
      }
    />
  );
};

export default ChartRenderer;