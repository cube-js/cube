import React from "react";
import PropTypes from "prop-types";
import moment from "moment";
import { useCubeQuery } from "@cubejs-client/react";
import { Spin, Row, Col, Statistic, Table } from "antd";
import { Line, Bar, Pie } from "react-chartjs-2";

const COLORS_SERIES = ["#FF6492", "#141446", "#7A77FF"];
const minutesAgo = ["10", "9", "8", "7", "6", "5", "4", "3", "2", "1", "0"];
const TypeToChartComponent = {
  line: ({ resultSet }) => {
    const data = {
      labels: resultSet.categories().map(c => moment(c.category).format("mm:ss")),
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map(r => r.value),
        borderColor: COLORS_SERIES[index],
        fill: false
      }))
    };
    const options = {
      legend: {
        display: false
      },
      scales: {
        yAxes: [{
          ticks: {
            precision: 0,
            min: 0
          }
        }]
      }
    };
    return <Line height={157} data={data} options={options} />;
  },
  bar: ({ resultSet }) => {
    const data = {
      labels: minutesAgo,
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: minutesAgo.map((minute) => {
          const value = s.series.find(i  => i.category === minute)
          return (value && value.value) || 0
        }),
        backgroundColor: COLORS_SERIES[index],
        fill: false
      }))
    };
    const options = {
      tooltips: {
        callbacks: {
          title: function(tooltipItem, data) {
            return `${data['labels'][tooltipItem[0]['index']]} mins ago`;
          },
        }
      },
      legend: {
        display: false
      },
      scales: {
        yAxes: [{
          ticks: {
            precision: 0,
            min: 0
          }
        }],
        xAxes: [
          {
            stacked: true
          },
        ]
      }
    };
    return <Bar height={157} data={data} options={options} />;
  },
  area: ({ resultSet }) => {
    const data = {
      labels: resultSet.categories().map(c => c.category),
      datasets: resultSet.series().map((s, index) => ({
        label: s.title,
        data: s.series.map(r => r.value),
        backgroundColor: COLORS_SERIES[index]
      }))
    };
    const options = {
      scales: {
        yAxes: [
          {
            stacked: true
          }
        ]
      }
    };
    return <Line data={data} options={options} />;
  },
  pie: ({ resultSet }) => {
    const data = {
      labels: resultSet.categories().map(c => c.category),
      datasets: resultSet.series().map(s => ({
        label: s.title,
        data: s.series.map(r => r.value),
        backgroundColor: COLORS_SERIES,
        hoverBackgroundColor: COLORS_SERIES
      }))
    };
    const options = {};
    return <Pie data={data} options={options} />;
  },
  table: ({ resultSet }) => (
    <Table
      pagination={false}
      columns={[
        { title: "Anonymous ID", dataIndex: "Events.anonymousId" },
        { title: "Event Type", dataIndex: "Events.eventType" },
        { title: "Time", dataIndex: "Events.minutesAgoHumanized"}
      ]}
      dataSource={resultSet.tablePivot().map((row) => {
        const addMinsAgo = (text) => {
          if (text.match(/minute/)) {
            return text;
          } else {
            return `${text} minutes ago`;
          }
        }
        return {
          "Events.anonymousId": row["Events.anonymousId"],
          "Events.eventType": row["Events.eventType"],
          "Events.minutesAgoHumanized": addMinsAgo(row["Events.minutesAgoHumanized"])
        }
      })}
    />
  ),
  number: ({ resultSet }) => (
    <Row
      type="flex"
      justify="center"
      align="middle"
      style={{
        height: "100%"
      }}
    >
      <Col>
        {resultSet.seriesNames().map(s => (
          <Statistic value={resultSet.totalRow()[s.key]} />
        ))}
      </Col>
    </Row>
  )
};
const TypeToMemoChartComponent = Object.keys(TypeToChartComponent)
  .map(key => ({
    [key]: React.memo(TypeToChartComponent[key])
  }))
  .reduce((a, b) => ({ ...a, ...b }));

const renderChart = Component => ({ resultSet, error }) =>
  (resultSet && <Component resultSet={resultSet} />) ||
  (error && error.toString()) || <Spin />;

const ChartRenderer = ({ vizState, cubejsApi }) => {
  const { query, chartType } = vizState;
  const component = TypeToMemoChartComponent[chartType];
  const renderProps = useCubeQuery(query, { subscribe: true, cubejsApi });
  return component && renderChart(component)(renderProps);
};

ChartRenderer.propTypes = {
  vizState: PropTypes.object,
  cubejsApi: PropTypes.object
};
ChartRenderer.defaultProps = {
  vizState: {},
  cubejsApi: null
};
export default ChartRenderer;
