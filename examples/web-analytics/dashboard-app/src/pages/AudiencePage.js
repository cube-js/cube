import React from "react";
import Grid from "@material-ui/core/Grid";

import ChartRenderer from "../components/ChartRenderer";
import DashboardItem from "../components/DashboardItem";
import OverTimeChart from "../components/OverTimeChart";
import Chart from "../components/Chart";
import SwitchTable from "../components/SwitchTable";

const queries = {
  usersOvertime: {
    chartType: 'line',
    legend: false,
    query: {
      measures: ['SessionUsers.usersCount'],
      timeDimensions: [{
        dimension: 'SessionUsers.sessionStart',
        granularity: 'day'
      }]
    }
  },

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


const AudiencePage = ({ withTime }) => {
  return (
    <>
      <Grid item xs={12}>
        <OverTimeChart
          title="Users Over Time"
          vizState={withTime(queries.usersOvertime)}
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
