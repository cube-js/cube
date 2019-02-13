import React from 'react';

import { withStyles } from '@material-ui/core/styles';
import FormControl from '@material-ui/core/FormControl';
import Select from '@material-ui/core/Select';
import InputLabel from '@material-ui/core/InputLabel';
import Input from '@material-ui/core/Input';
import MenuItem from '@material-ui/core/MenuItem';

const styles = ({
  formControl: {
    minWidth: 120,
  }
});

const events = [
  { title: "Pave View", key: "pageView" },
  { title: "Navigation: Menu Opened", key: "navigationMenuOpened" },
  { title: "Navigation: Menu Closed", key: "navigationMenuClosed" }
]

const EventsSelect = ({ classes }) => (
  <FormControl className={classes.formControl}>
    <InputLabel shrink htmlFor="age-label-placeholder">
      Events
    </InputLabel>
    <Select
      input={<Input name="age" id="age-label-placeholder" />}
      displayEmpty
      name="age"
    >
      {
        events.map((event, i) => (
          <MenuItem key={event.key} value={event.key}>{event.title}</MenuItem>
        ))
      }
    </Select>
  </FormControl>
)

export default withStyles(styles)(EventsSelect);
