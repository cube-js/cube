import React, { useContext } from "react";
import { QueryRenderer } from "@cubejs-client/react";
import { CubeContext } from '@cubejs-client/react';
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
  const { cubejsApi } = useContext(CubeContext);
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