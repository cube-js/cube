import React, { useState } from "react";
import Grid from "@material-ui/core/Grid";

import OverTimeChart from "../components/OverTimeChart";
import Chart from "../components/Chart";
import Dropdown from "../components/Dropdown";
import SwitchTable from "../components/SwitchTable";

const queries = {
  usersCount: {
    chartType: 'number',
    query: {
      measures: ['SessionUsers.usersCount'],
      timeDimensions: [{
        dimension: 'SessionUsers.sessionStart'
      }]
    }
  },

  sessionsCount: {
    chartType: 'number',
    query: {
      measures: ['Sessions.count']
    }
  },

  newUsersCount: {
    chartType: 'number',
    query: {
      measures: ['SessionUsers.newUsersCount'],
      timeDimensions: [{
        dimension: 'SessionUsers.sessionStart'
      }]
    }
  },

  bounceRate: {
    chartType: 'number',
    query: {
      measures: ['Sessions.bounceRate']
    }
  },

  averageDuration: {
    chartType: 'number',
    query: {
      measures: ['Sessions.averageDurationSeconds']
    }
  },

  averageNumberSessions: {
    chartType: 'number',
    query: {
      measures: ['SessionUsers.sessionsPerUser'],
      timeDimensions: [{
        dimension: 'SessionUsers.sessionStart'
      }]
    }
  },

  usersByType: {
    chartType: 'pie',
    query: {
      measures: ['SessionUsers.usersCount'],
      dimensions: ['SessionUsers.type'],
      timeDimensions: [{
        dimension: 'SessionUsers.sessionStart'
      }]
    }
  }
};

const overTimeQueries = {
  "Users": {
    measures: ["SessionUsers.usersCount"],
    timeDimensions: [{
      granularity: 'day',
      dimension: "SessionUsers.sessionStart"
    }]
  },
  "Sessions": {
    measures: ["Sessions.count"],
    timeDimensions: [{
      granularity: 'day',
      dimension: "Sessions.sessionStart"
    }]
  },
  "Page Views": {
    measures: ["PageViews.count"],
    timeDimensions: [{
      granularity: 'day',
      dimension: "PageViews.time"
    }]
  }
};

const AudiencePage = ({ withTime }) => {
  const [overTimeQuery, setOverTimeQuery] = useState("Users");
  return (
    <>
      <Grid item xs={12}>
        <OverTimeChart
          title={
            <Dropdown
              value={overTimeQuery}
              options={
                Object.keys(overTimeQueries).reduce((out, measure) => {
                  out[measure] = () => setOverTimeQuery(measure)
                  return out;
                }, {})
              }
            />
          }
          vizState={withTime({
            chartType: 'line',
            legend: false,
            query: overTimeQueries[overTimeQuery]
          })}
        />
      </Grid>
      <Grid item xs={6}>
        <Grid container spacing={3}>
          <Grid item xs={6}>
            <Chart title="Users" vizState={withTime(queries.usersCount)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="New Users" vizState={withTime(queries.newUsersCount)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="Sessions" vizState={withTime(queries.sessionsCount)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="Bounce Rate" vizState={withTime(queries.bounceRate)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="Avg. Session Duration" vizState={withTime(queries.averageDuration)} />
          </Grid>
          <Grid item xs={6}>
            <Chart title="Number of Sessions per User" vizState={withTime(queries.averageNumberSessions)} />
          </Grid>
        </Grid>
      </Grid>
      <Grid item xs={6}>
        <Chart
          title="Users by Type"
          vizState={withTime(queries.usersByType)}
        />
      </Grid>
      <SwitchTable
        options={[{
          title: "Demographics",
          values: [{
            name: "Language",
            fn: ({ query, ...vizState }) => ({
              ...vizState,
              query: {
                ...query,
                dimensions: ["SessionUsers.language"]
              }
            })
          },
          {
            name: "Country",
            fn: ({ query, ...vizState }) => ({
              ...vizState,
              query: {
                ...query,
                dimensions: ["SessionUsers.country"]
              }
            })
          },
          {
            name: "City",
            fn: ({ query, ...vizState }) => ({
              ...vizState,
              query: {
                ...query,
                dimensions: ["SessionUsers.city"]
              }
            })
          }]
        }]}
        query={withTime({ query: queries.usersCount.query, chartType: 'table' })}
      />
    </>
  );
}

export default AudiencePage;
