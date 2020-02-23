import React from "react";
import ChartRenderer from "./ChartRenderer";

const DataTable = ({ query }) => (
  <ChartRenderer vizState={{ query, chartType: 'table' }} />
);

export default DataTable;
