import React from "react";
import ChartRenderer from "./ChartRenderer";

const DataTable = ({ query }) => (
  <ChartRenderer height={300} vizState={{ query, chartType: 'table' }} />
);

export default DataTable;
