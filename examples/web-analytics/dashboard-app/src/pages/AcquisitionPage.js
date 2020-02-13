import React, { useState } from "react";
import Grid from "@material-ui/core/Grid";
import FormControl from '@material-ui/core/FormControl';
import InputLabel from '@material-ui/core/InputLabel';
import Select from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import Input from '@material-ui/core/Input';


import Chart from "../components/Chart";

const queries = {
  topSources: {
    chartType: 'pie',
    query: {
      measures: ['SessionUsers.usersCount'],
      dimensions: ['SessionUsers.referrerSource'],
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
  }
}

const dimensionOptions = [
  "Top Channels",
  "Top Sources/Mediums",
  "Top Sources",
  "Top Mediums"
];

const AcquisitionPage = ({ withTime }) => {
  const [primaryDimension, setPrimaryDimension] = useState(dimensionOptions[0]);
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
              {dimensionOptions.map(name => (
                <MenuItem key={name} value={name}>
                  {name}
                </MenuItem>
              ))}
            </Select>
          </FormControl>
        </Grid>
        <Grid item xs={6}>
          <Chart title={primaryDimension} vizState={withTime(queries.topSources)} />
        </Grid>
        <Grid item xs={6}>
          <Chart title="Users" vizState={withTime(queries.usersOvertime)} />
        </Grid>
      </Grid>
    </Grid>
  );
};

export default AcquisitionPage;
