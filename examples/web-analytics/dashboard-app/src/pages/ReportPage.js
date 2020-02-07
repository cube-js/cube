import React, { useState } from "react";
import Grid from "@material-ui/core/Grid";
import moment from 'moment';

import DateRangePicker from "../components/DateRangePicker";

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

// TODO: Save last selected daterange into cookie/localstorage and use it instead
const DEFAULT_DATE_RANGE = moment().subtract(7, 'days');

const ReportPage = ({ report: Component }) => {
  const [beginDate, setBeginDate] = useState(DEFAULT_DATE_RANGE);
  const [endDate, setEndDate] = useState(moment());
  const withTime = (vizState) => withTimeFunc(vizState, beginDate, endDate);

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
