import React from "react";
import { Col, Row, Statistic } from "antd";
import { useCubeQuery } from '@cubejs-client/react'
import { format, formatDistanceToNowStrict, differenceInSeconds } from "date-fns"
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Filler
} from 'chart.js';
import { Bar } from "react-chartjs-2";
import DashboardItem from '../components/DashboardItem'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Filler
);

const DashboardPage = () => {
  const usersOnlineQuery = {
    measures: [
      "OnlineUsers.count"
    ],
    timeDimensions: [ {
      dimension: "OnlineUsers.lastSeen",
      dateRange: "last 300 seconds"
    } ]
  }

  const buttonClicksLastHourQuery = {
    measures: [
      "Events.count"
    ],
    timeDimensions: [ {
      dimension: "Events.time",
      dateRange: "from 1 hour ago to now"
    } ],
    filters: [ {
      member: `Events.type`,
      operator: `equals`,
      values: ['track']
    } ]
  }

  const pageViewsLastHourQuery = {
    ...buttonClicksLastHourQuery,
    filters: [ {
      member: `Events.type`,
      operator: `equals`,
      values: ['page']
    } ]
  }

  const realTimeEventsChartQuery = {
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
    }
  }

  const usersOnlineLastDayChartQuery = {
    "measures": [
      "Events.userCount"
    ],
    "timeDimensions": [
      {
        "dimension": "Events.time",
        "granularity": "hour"
      }
    ],
    "order": {
      "Events.time": "asc"
    }
  }

  const realTimeEventsQuery = {
    dimensions: [
      "Events.anonymousId",
      "Events.type",
      "Events.time.second"
    ],
    order: {
      "Events.time": "desc"
    },
    limit: 15
  }

  const usersOnlineLastDayQuery = {
    ...usersOnlineQuery,
    timeDimensions: [ {
      dimension: "OnlineUsers.lastSeen",
      dateRange: "from 24 hours ago to now"
    } ]
  }

  const buttonClicksLastDayQuery = {
    ...buttonClicksLastHourQuery,
    timeDimensions: [ {
      dimension: "Events.time",
      dateRange: "from 24 hours ago to now"
    } ],
  }

  const pageViewsLastDayQuery = {
    ...pageViewsLastHourQuery,
    timeDimensions: [ {
      dimension: "Events.time",
      dateRange: "from 24 hours ago to now"
    } ],
  }

  const { resultSet: usersOnlineResultSet } = useCubeQuery(usersOnlineQuery, { subscribe: true });
  const { resultSet: buttonClicksLastHourResultSet } = useCubeQuery(buttonClicksLastHourQuery, { subscribe: true });
  const { resultSet: pageViewsLastHourResultSet } = useCubeQuery(pageViewsLastHourQuery, { subscribe: true });
  const { resultSet: realTimeEventsChartResultSet } = useCubeQuery(realTimeEventsChartQuery, { subscribe: true });
  const { resultSet: realTimeEventsResultSet } = useCubeQuery(realTimeEventsQuery, { subscribe: true });

  const { resultSet: usersOnlineLastDayResultSet } = useCubeQuery(usersOnlineLastDayQuery, { subscribe: true });
  const { resultSet: buttonClicksLastDayResultSet } = useCubeQuery(buttonClicksLastDayQuery, { subscribe: true });
  const { resultSet: pageViewsLastDayResultSet } = useCubeQuery(pageViewsLastDayQuery, { subscribe: true });
  const { resultSet: usersOnlineLastDayChartResultSet } = useCubeQuery(usersOnlineLastDayChartQuery, { subscribe: true });
  
  const rowPadding = { padding: 10 }

  return (
    <>
      <div style={{ padding: "0 30px" }}>
        <Row>
          <Col span={6}>
            <Row style={rowPadding}>
              <DashboardItem title="Users Online">
                <NumberCard resultSet={usersOnlineResultSet} />
              </DashboardItem>
            </Row>
            <Row style={rowPadding}>
              <DashboardItem title="Button Clicks (Last Hour)">
                <NumberCard resultSet={buttonClicksLastHourResultSet} />
              </DashboardItem>
            </Row>
            <Row style={rowPadding}>
              <DashboardItem title="Page Views (Last Hour)">
                <NumberCard resultSet={pageViewsLastHourResultSet} />
              </DashboardItem>
            </Row>
          </Col>
          <Col span={18}>
            <Row style={rowPadding}>
              <DashboardItem title='Real Time Events'>
                <Chart resultSet={realTimeEventsChartResultSet} />
              </DashboardItem>
            </Row>
          </Col>
        </Row>
      </div>

      <div style={{ padding: "0 30px" }}>
        <Row style={rowPadding}>
          <DashboardItem title="Real-Time Events">
            <EventCards resultSet={realTimeEventsResultSet} />
          </DashboardItem>
        </Row>
      </div>

      <div style={{ padding: "0 30px" }}>
        <Row>
          <Col span={6}>
            <Row style={rowPadding}>
              <DashboardItem title="Users Online (Last Day)">
                <NumberCard resultSet={usersOnlineLastDayResultSet} />
              </DashboardItem>
            </Row>
            <Row style={rowPadding}>
              <DashboardItem title="Button Clicks (Last Day)">
                <NumberCard resultSet={buttonClicksLastDayResultSet} />
              </DashboardItem>
            </Row>
            <Row style={rowPadding}>
              <DashboardItem title="Page Views (Last Hour)">
                <NumberCard resultSet={pageViewsLastDayResultSet} />
              </DashboardItem>
            </Row>
          </Col>
          <Col span={18}>
            <Row style={rowPadding}>
              <DashboardItem title='Users Online (Last Day)'>
                <Chart resultSet={usersOnlineLastDayChartResultSet} />
              </DashboardItem>
            </Row>
          </Col>
        </Row>
      </div>

      <div style={{ padding: "0 30px" }}>
        <DashboardItem title="Sreaming Data Pipeline Architecture">
          <div>
            <img width="100%" src="https://ucarecdn.com/4efc3459-88b4-4a54-8596-8a0e6fa16814/" alt="Architecture" />
          </div>
        </DashboardItem>
      </div>
    </>
  )
};

export default DashboardPage;

function NumberCard({ resultSet }) {
  if (!resultSet) {
    return <></>
  }

  return (
    <Row
    type="flex"
    justify="center"
    align="middle"
  >
    {resultSet.seriesNames().map((s, key) => (
      <Statistic key={key} value={resultSet.totalRow()[s.key]} />
    ))}
  </Row>
  )
}

function EventCards({ resultSet }) {
  if (!resultSet) {
    return <></>
  }

  const eventStyle = {
    background: '#f3f3fb',
    borderRadius: '1em',
    display: 'inline-block',
    marginRight: '0.5em',
    marginBottom: '0.5em',
    padding: '0.5em 0.5em',
    width: '125px'
  }

  const chipStyle = {
    display: 'block',
    padding: '0 0.25em'
  }

  const eventChipStyle = {
    ...chipStyle,
    color: '#FF6492',
    fontWeight: 'bolder'
  }

  return (
    <ul style={{ margin: 0 }}>
      {resultSet.tablePivot().map((row, i) => {
        const id = row["Events.anonymousId"].split('-')[0]
        const color = '#' + id.substr(0, 6);

        const idChipStyle = {
          ...chipStyle,
          color,
          fontWeight: 'bolder'
        }

        const dateUtc = new Date(row["Events.time.second"] + 'Z')
        const diff = differenceInSeconds(new Date(), dateUtc)
        
        const eventStyleWithOpacity = {
          ...eventStyle,
          opacity: (100 - diff) / 100
        }
        
        var eventType = row["Events.type"];
         if (row["Events.type"] === 'track') {
          eventType = "button_click"
         }

        return (
          <li key={i} style={eventStyleWithOpacity}>
            <span style={idChipStyle}>{id}</span>
            <span style={eventChipStyle}>{eventType}</span>
            <span style={chipStyle}>{formatDistanceToNowStrict(dateUtc, { includeSeconds: true }) + ' ago'}</span>
          </li>
        )
      })}
    </ul>
  )
}

function Chart({ resultSet }) {
  if (!resultSet) {
    return <></>
  }

  const COLORS_SERIES = ["#FF6492", "#141446", "#7A77FF"];
  const data = {
    labels: resultSet.categories().map(c => format(new Date(c.x), "mm:ss")),
    datasets: resultSet.series().map((s, index) => ({
      label: s.title,
      data: s.series.map(r => r.value),
      borderWidth: 0,
      backgroundColor: COLORS_SERIES[index],
      fill: true,
      stepped: 'middle',
      pointRadius: 0,
      pointHoverRadius: 0,
      barPercentage: 1.2
    }))
  };
  const options = {
    plugins: {
      legend: {
        display: false
      }
    },
    scales: {
      x: {
        grid: {
          display: false
        },
      },
      y: {
        grid: {
          display: false
        }
      }
    },
    animation: {
      duration: 0
    }
  };
  return <Bar height={157} data={data} options={options} />
}