import { useEffect, useState } from "react"
import cubejs from "@cubejs-client/core";
import { Spin, Table } from "antd";

// Initiate Cube.js API
const cubejsApi = cubejs(
  'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpYXQiOjE2NTMyODIzNDQsImV4cCI6MTY1NTg3NDM0NH0.6__5oRpMmh8dEbBmhN-tkFOVc-B8CNU8IkxX7E_z5XI',
  { apiUrl: 'https://inherent-lynx.aws-us-east-1.cubecloudapp.dev/cubejs-api/v1' }
);

// Declaire Pivot Configuration [Constant for each chart]
const pivotConfig = {
  x: [
    "Fraud.type",
    "Fraud.newbalancedest",
    "Fraud.isfraud",
    "Fraud.isflaggedfraud"
  ],
  y: ["measures"],
  fillMissingDates: true,
  joinDateRange: false
}

const TableRenderer = () => {

  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [columns, setColumns] = useState([])
  
  useEffect(() => {
    // Load data from Cube.js API on component mount
    cubejsApi
      .load({
        "measures": [
          "Fraud.amount",
          "Fraud.count"
        ],
        "timeDimensions": [],
        "order": {
          "Fraud.nameorig2": "desc"
        },
        "dimensions": [
          "Fraud.type",
          "Fraud.isfraud",
          "Fraud.isflaggedfraud"
        ],
        "limit": 10000
      })
      .then((resultSet) => {
        setColumns(resultSet.tableColumns(pivotConfig));
        setData(formatTableData(columns, resultSet.tablePivot(pivotConfig)))
        
      })
      .catch((error) => {
        setError(error);
      })
  }, [])

  if(!data) {
    return <Spin />;
  }
  
  return (
    <Table 
      columns={columns}
      pagination={true} 
      dataSource={data} 
    />
  )
}

// helper function to format data
const formatTableData = (columns, data) => {
  function flatten(columns = []) {
    return columns.reduce((memo, column) => {
      if (column.children) {
        return [...memo, ...flatten(column.children)];
      }

      return [...memo, column];
    }, []);
  }

  const typeByIndex = flatten(columns).reduce((memo, column) => {
    return { ...memo, [column.dataIndex]: column };
  }, {});

  function formatValue(value, { type, format } = {}) {
    if (value == undefined) {
      return value;
    }

    if (type === "boolean") {
      if (typeof value === "boolean") {
        return value.toString();
      } else if (typeof value === "number") {
        return Boolean(value).toString();
      }

      return value;
    }

    if (type === "number" && format === "percent") {
      return [parseFloat(value).toFixed(2), "%"].join("");
    }

    return value.toString();
  }

  function format(row) {
    return Object.fromEntries(
      Object.entries(row).map(([dataIndex, value]) => {
        return [dataIndex, formatValue(value, typeByIndex[dataIndex])];
      })
    );
  }

  return data.map(format);
};

export default TableRenderer;