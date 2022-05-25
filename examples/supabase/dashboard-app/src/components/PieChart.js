import { QueryRenderer } from "@cubejs-client/react";
import { CubeContext } from '@cubejs-client/react';
import { Spin } from "antd";
import "antd/dist/antd.css";
import React, { useContext } from "react";
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend
} from "recharts";

const colors = ["#FF6492", "#141446", "#7A77FF", "#FFB964"];

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
    <ResponsiveContainer width="100%" height={350}>
      <PieChart>
        <Pie
          isAnimationActive={true}
          data={resultSet.chartPivot()}
          nameKey="x"
          dataKey={resultSet.seriesNames()[0].key}
          fill="#8884d8"
        >
          {resultSet.chartPivot().map((e, index) => (
            <Cell key={index} fill={colors[index % colors.length]} />
          ))}
        </Pie>
        <Legend />
        <Tooltip />
      </PieChart>
    </ResponsiveContainer>
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
          chartType: "pie",
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