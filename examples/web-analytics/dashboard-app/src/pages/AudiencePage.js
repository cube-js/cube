import React, { useState } from "react";
import Grid from "@material-ui/core/Grid";
import moment from 'moment';

import ChartRenderer from "../components/ChartRenderer";
import DashboardItem from "../components/DashboardItem";
import OverTimeChart from "../components/OverTimeChart";
import DateRangePicker from "../components/DateRangePicker";

const queries = {
  usersOvertime: {
    chartType: 'line',
    legend: false,
    query: {
      measures: ['PageViews.usersCount'],
      timeDimensions: [{
        dimension: 'PageViews.time',
        granularity: 'day'
      }]
    }
  },

  usersCount: {
    chartType: 'number',
    query: {
      measures: ['Sessions.usersCount']
    }
  },

  newUsersCount: {
    chartType: 'number',
    query: {
      measures: ['Sessions.newUsersCount']
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

  usersByType: {
    chartType: 'pie',
    query: {
      measures: ['Sessions.usersCount'],
      dimensions: ['Sessions.type']
    }
  }
};

const withTime = ({ query, ...vizState }, begin, end) => ({
  ...vizState,
  query: {
    ...query,
    timeDimensions: [{
      dimension: 'PageViews.time',
      dateRange: [begin.format(moment.HTML5_FMT.DATE), end.format(moment.HTML5_FMT.DATE)],
      granularity: 'day'
    }]
  }
});

const AudiencePage = () => {
  const [beginDate, setBeginDate] = useState(moment().subtract(7, 'days'));
  const [endDate, setEndDate] = useState(moment());
  return (
    <Grid
      container
      spacing={3}
      justify="flex-end"
    >
      <Grid item xs={3}>
        <DateRangePicker
          value={[beginDate, endDate]}
          placeholder="Select a date range"
          onChange={values => {
            setBeginDate(values.begin);
            setEndDate(values.end);
          }}
        />
      </Grid>
      <Grid item xs={12}>
        <OverTimeChart
          title="Users Over Time"
          vizState={withTime(queries.usersOvertime, beginDate, endDate)}
        />
      </Grid>
        <Grid item xs={6}>
          <Grid container spacing={3}>
            <Grid item xs={6}>
              <DashboardItem title="Users">
                <ChartRenderer
                  vizState={queries.usersCount}
                />
              </DashboardItem>
            </Grid>
            <Grid item xs={6}>
              <DashboardItem title="New Users">
                <ChartRenderer
                  vizState={queries.newUsersCount}
                />
              </DashboardItem>
            </Grid>
            <Grid item xs={6}>
              <DashboardItem title="Avg. Session Duration">
                <ChartRenderer
                  vizState={queries.averageDuration}
                />
              </DashboardItem>
            </Grid>
            <Grid item xs={6}>
              <DashboardItem title="Bounce Rate">
                <ChartRenderer
                  vizState={queries.bounceRate}
                />
              </DashboardItem>
            </Grid>
            <Grid item xs={6}>
              <DashboardItem title="Avg. Session Duration">
                <ChartRenderer
                  vizState={queries.averageDuration}
                />
              </DashboardItem>
            </Grid>
            <Grid item xs={6}>
              <DashboardItem title="Bounce Rate">
                <ChartRenderer
                  vizState={queries.bounceRate}
                />
              </DashboardItem>
            </Grid>
          </Grid>
        </Grid>
        <Grid item xs={6}>
          <DashboardItem title="Users by Type">
            <ChartRenderer vizState={queries.usersByType} />
          </DashboardItem>
        </Grid>
      </Grid>
  );
}

export default AudiencePage;
