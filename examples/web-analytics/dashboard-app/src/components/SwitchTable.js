import React, { useState } from "react";
import Grid from "@material-ui/core/Grid";
import ListSubheader from '@material-ui/core/ListSubheader';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import ListItemIcon from '@material-ui/core/ListItemIcon';
import ListItemText from '@material-ui/core/ListItemText';
import ChartRenderer from "../components/ChartRenderer";

const SwitchTable = ({ options, query }) => {
  const [option, setOption] = useState(options[0]);
  debugger
  return ([
    <Grid item xs={3}>
      <List
        component="nav"
        aria-labelledby="nested-list-subheader"
        subheader={
          <ListSubheader component="div" id="nested-list-subheader">
            Site Content
          </ListSubheader>
        }
      >
        {options.map(opt => (
          <ListItem onClick={() => setOption(opt)} selected={option.name === opt.name} button>
            <ListItemText primary={opt.name} />
          </ListItem>
        ))}
      </List>
    </Grid>,
    <Grid item xs={9}>
      <ChartRenderer vizState={option.fn(query)} />
    </Grid>
  ]);
};

export default SwitchTable;
