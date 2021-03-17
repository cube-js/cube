import cubejs from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { Spin, Table } from 'antd';
import 'antd/dist/antd.css';
import PropTypes from 'prop-types';
import React from 'react';
import { Line, Pie } from 'react-chartjs-2';


// Different Example Color Series
const LINE_COLORS_SERIES = ['#89CFF0', '#FF6492', '#7A77FF'];
const PIE_COLORS_SERIES = ['#FF6492', '#141446', '#7A77FF'];
const AREA_COLORS_SERIES = ['#89CFF0', '#FF6492', '#7A77FF'];


// Cube JS API
const cubejsApi = cubejs(
  process.env.REACT_APP_CUBEJS_TOKEN,
  { apiUrl: process.env.REACT_APP_API_URL }
);

//  Common Option for all types of Charts
const commonOptions = {
  maintainAspectRatio: false,
};

const renderChart = ({ resultSet, error, chartType, pivotConfig }) => {
  if (error) {
    return <div>{error.toString()}</div>;
  }

  if (!resultSet) {
    return <Spin />;
  }

  // Render Pie Chart from ChartJS
  if(chartType === 'pie'){    
    const data = {
    labels: resultSet.categories().map((c) => c.category),
    datasets: resultSet.series().map((s) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        backgroundColor: PIE_COLORS_SERIES,
        hoverBackgroundColor: PIE_COLORS_SERIES,
      })),
    };
    const options = { ...commonOptions };
    return <Pie data={data} options={options} />;
  }

  // Render Line Chart from ChartJS
  if(chartType === 'line'){
    const data = {
    labels: resultSet.categories().map((c) => c.category),  
    datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        borderColor: LINE_COLORS_SERIES[index],
        fill: false,
      })),
    };
    const options = { ...commonOptions };
    return <Line data={data} options={options} />
  }

  // Render Area Chart from ChartJS
  if(chartType === 'area'){
    const data = {
      labels: resultSet.categories().map((c) => c.category),
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map((r) => r.value),
        backgroundColor: AREA_COLORS_SERIES[index],
      })),
    };
    const options = {
      ...commonOptions,
      scales: {
        yAxes: [
          {
            stacked: true,
          },
        ],
      },
    };
    return <Line data={data} options={options} />;
  }

  // Render Table from Ant Design
  if(chartType === 'table'){
    return (
    <Table
      pagination={false}
      columns={resultSet.tableColumns(pivotConfig)}
      dataSource={resultSet.tablePivot(pivotConfig)}
    />)
  }
  
};

const WhichQueryRenderer = (chartType) => {
  if(chartType === 'line'){
    return <QueryRenderer
      query={{
        "measures": [
          "Airline.count",
          "Airline.statisticsMinutesDelayedTotal",
          "Airline.statisticsFlightsTotal"
        ],
        "timeDimensions": [],
        "order": {
          "Airline.count": "desc"
        },
        "dimensions": [
          "Airline.airportCode"
        ]
      }}
      cubejsApi={cubejsApi}
      resetResultSetOnChange={false}
      render={(props) => renderChart({
        ...props,
        chartType: 'line',
        pivotConfig: {
          "x": [
            "Airline.airportCode"
          ],
          "y": [
            "measures"
          ],
          "fillMissingDates": true,
          "joinDateRange": false
        }
      })}
    />
  }
  if (chartType === "bar"){
    return <QueryRenderer
      query={{
        "measures": [
          "Airline.count",
          "Airline.statisticsMinutesDelayedTotal",
          "Airline.statisticsFlightsTotal"
        ],
        "timeDimensions": [],
        "order": {
          "Airline.count": "desc"
        },
        "dimensions": [
          "Airline.airportCode"
        ]
      }}
      cubejsApi={cubejsApi}
      resetResultSetOnChange={false}
      render={(props) => renderChart({
        ...props,
        chartType: 'bar',
        pivotConfig: {
          "x": [
            "Airline.airportCode"
          ],
          "y": [
            "measures"
          ],
          "fillMissingDates": true,
          "joinDateRange": false
        }
      })}
    />
  }
  if (chartType === "area"){
    return <QueryRenderer
      query={{
          "measures": [
            "Airline.statisticsFlightsTotal"
          ],
          "timeDimensions": [],
          "order": {
            "Airline.statisticsFlightsTotal": "desc"
          },
          "dimensions": [
            "Airline.airportCode"
          ]
        }}
            cubejsApi={cubejsApi}
            resetResultSetOnChange={false}
            render={(props) => renderChart({
              ...props,
              chartType: 'area',
              pivotConfig: {
        "x": [
          "Airline.airportCode"
        ],
        "y": [
          "measures"
        ],
        "fillMissingDates": true,
        "joinDateRange": false
      }
      })}
    />
  }
  if(chartType === "pie"){
    return <QueryRenderer
      query={{
        "measures": [
          "Airline.statisticsCarriersTotal"
        ],
        "timeDimensions": [],
        "order": {
          "Airline.statisticsFlightsTotal": "desc"
        },
        "dimensions": [
          "Airline.timeMonthName"
        ]
      }}
            cubejsApi={cubejsApi}
            resetResultSetOnChange={false}
            render={(props) => renderChart({
              ...props,
              chartType: 'pie',
              pivotConfig: {
        "x": [
          "Airline.timeMonthName"
        ],
        "y": [
          "measures"
        ],
        "fillMissingDates": true,
        "joinDateRange": false
      }
      })}
    />
  }
  if(chartType === 'table'){
    return <QueryRenderer
      query={{
        "measures": [
          "Airline.statisticsCarriersTotal",
          "Airline.statisticsMinutesDelayedTotal"
        ],
        "timeDimensions": [],
        "order": {
          "Airline.statisticsCarriersTotal": "desc"
        },
        "dimensions": [
          "Airline.airportCode",
          "Airline.timeLabel"
        ]
      }}
            cubejsApi={cubejsApi}
            resetResultSetOnChange={false}
            render={(props) => renderChart({
              ...props,
              chartType: 'table',
              pivotConfig: {
        "x": [
          "Airline.airportCode",
          "Airline.timeLabel"
        ],
        "y": [
          "measures"
        ],
        "fillMissingDates": true,
        "joinDateRange": false
      }
      })}
    />
  }
}

const ChartRenderer = ({chartType}) => {
  console.log(chartType)  
  return (
    <>{WhichQueryRenderer(chartType)}</>)
};

ChartRenderer.propTypes = {
  chartType: PropTypes.string,
  cubejsApi: PropTypes.object
};

export default ChartRenderer;