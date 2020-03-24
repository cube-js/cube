import React, { useState, createContext } from "react";
import Grid from "@material-ui/core/Grid";
import moment from 'moment';

import DateRangePicker from "./DateRangePicker";

const WithTimeContext = React.createContext(null);

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

const ReportPageLayout = ({ children }) => {
  const [beginDate, setBeginDate] = useState(moment().subtract(7, 'days'));
  const [endDate, setEndDate] = useState(moment());
  const withTime = (vizState) => withTimeFunc(vizState, beginDate, endDate);

  return (
    <WithTimeContext.Provider value={withTime}>
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
      { children }
    </Grid>
    </WithTimeContext.Provider>
  )
};

export default ReportPageLayout;
