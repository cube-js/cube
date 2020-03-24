import React, { useState } from "react";
import Grid from "@material-ui/core/Grid";
import FormControl from '@material-ui/core/FormControl';
import InputLabel from '@material-ui/core/InputLabel';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import Input from '@material-ui/core/Input';

import Chart from "../components/Chart";
import ChartRenderer from "../components/ChartRenderer";

const queries = {
  topSources: {
    chartType: 'pie',
    legend: 'vertical',
    query: {
      measures: ['SessionUsers.usersCount'],
      dimensions: ['SessionUsers.sourceMedium'],
      timeDimensions: [{
        dimension: 'SessionUsers.sessionStart'
      }]
    }
  },
  usersOvertime: {
    chartType: 'line',
    query: {
      measures: ['SessionUsers.usersCount'],
      timeDimensions: [{
        dimension: 'SessionUsers.sessionStart',
        granularity: 'day'
      }]
    }
  },

  tableQuery: {
    query: {
      measures: [
        "SessionUsers.count",
        "SessionUsers.usersCount",
        "SessionUsers.newUsersCount"
      ],
      timeDimensions: [{
        dimension: 'SessionUsers.sessionStart'
      }]
    },
    chartType: 'table'
  }
}

const dimensionOptions = {
  "Top Sources/Mediums": "SessionUsers.sourceMedium",
  "Top Sources": "SessionUsers.referrerSource",
  "Top Mediums": "SessionUsers.referrerMedium"
};

const withPrimaryDimension = ({ query, ...vizState }, dimension) => ({
  ...vizState,
  query: {
    ...query,
    dimensions: [dimension]
  }
})

const AcquisitionPage = ({ withTime }) => {
  const [primaryDimension, setPrimaryDimension] = useState(Object.keys(dimensionOptions)[0]);
  const handleChange = (event) => setPrimaryDimension(event.target.value);
  return (
    <Grid item xs={12}>
      <Grid container spacing={3}>
        <Grid item xs={12}>
          <FormControl>
            <InputLabel
              id="primary-dimension-label"
              style={{ width: 140 }}
            >
              Primary Dimension
            </InputLabel>
            <Select
              labelId="primary-dimension-label"
              value={primaryDimension}
              onChange={handleChange}
              input={<Input />}
            >
              {Object.keys(dimensionOptions).map(key => (
                <MenuItem key={key} value={key}>
                  {key}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>
        <Grid item xs={6}>
          <Chart
            title={primaryDimension}
            vizState={withPrimaryDimension(withTime(queries.topSources), dimensionOptions[primaryDimension])}
          />
        </Grid>
        <Grid item xs={6}>
          <Chart title="Users" vizState={withTime(queries.usersOvertime)} />
        </Grid>
        <Grid item xs={12}>
          <ChartRenderer
            vizState={withPrimaryDimension(withTime(queries.tableQuery), dimensionOptions[primaryDimension])} />
        </Grid>
      </Grid>
    </Grid>
  );
};

export default AcquisitionPage;
