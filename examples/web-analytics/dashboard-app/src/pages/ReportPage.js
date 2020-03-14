import React, { useState, useEffect } from "react";
import Grid from "@material-ui/core/Grid";
import moment from 'moment';

import DateRangePicker from "../components/DateRangePicker";

// TODO: Save last selected daterange into cookie/localstorage and use it instead
const DEFAULT_BEGIN_DATE = moment().subtract(7, 'days');
const DEFAULT_END_DATE = moment();

const getDateRange = () => {
  const savedDateRange = window.localStorage.getItem('daterange');

  if (savedDateRange) {
    return JSON.parse(savedDateRange).map(date => moment(date));
  } else {
    return [
      DEFAULT_BEGIN_DATE,
      DEFAULT_END_DATE
    ]
  }
}

const setDateRange = (beginDate, endDate) => (
  window.localStorage.setItem('daterange', JSON.stringify([beginDate, endDate]))
)

const withTimeFunc = ({ query, ...vizState }, begin, end) => {
  const timeDimensionObj = (query.timeDimensions || [])[0] || {};
  const timeDimension = timeDimensionObj.dimension || 'Sessions.sessionStart';
  const granularity = timeDimensionObj.granularity || null;
  return {
    ...vizState,
    query: {
      ...query,
      timeDimensions: [{
        dimension: timeDimension,
        dateRange: [begin.format(moment.HTML5_FMT.DATE), end.format(moment.HTML5_FMT.DATE)],
        granularity: granularity
      }]
    }
  }
};

const ReportPage = ({ report: Component }) => {
  const [beginDate, setBeginDate] = useState(getDateRange()[0]);
  const [endDate, setEndDate] = useState(getDateRange()[1]);
  const withTime = (vizState) => withTimeFunc(vizState, beginDate, endDate);
  useEffect(() => setDateRange(beginDate, endDate));

  return (
    <Grid
      container
      spacing={3}
      justify="space-between"
    >
    <Grid item xs={3}>
      Segment: All Users
    </Grid>
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
    <Component withTime={withTime} />
  </Grid>
  )
};

export default ReportPage;
