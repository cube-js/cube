import React from 'react';
import Select from 'react-select';

import { withStyles } from '@material-ui/core/styles';
import { default as MaterialSelect } from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import IconButton from '@material-ui/core/IconButton';
import ClearIcon from '@material-ui/icons/Clear';

const options = [
  { value: 'Events.anyEvent', label: 'Any Event', default: true },
  { value: 'Events.pageView', label: 'Page View' },
  { value: 'Events.Navigation__Menu_Closed', label: 'Navigation: Menu Closed' },
  { value: 'Events.Navigation__Menu_Opened', label: 'Navigation: Menu Opened' }
]
export const defaultEvent = options.find(i => i.default)

const handleChange = (value, action, id, onChangeProp) => {
  onChangeProp({ type: "REMOVE_MEASURE", id })
  onChangeProp({
    type: 'ADD_MEASURE',
    value: value.value,
    id
  })
}

const customStyles = {
  container: (provided) => ({
    ...provided,
    width: 300
  })
}

const styles = {
  container: {
    position: "relative",
  },
  clearButton: {
    position: "absolute",
    right: -10,
    top: -10
  }
};

const EventsSelect = ({ onChange, defaultValue, id, clearable, classes }) => (
  <div className={classes.container}>
    <MaterialSelect
      disableUnderline
      value="total"
    >
      <MenuItem value="total">Total</MenuItem>
      <MenuItem value="unique">Unique</MenuItem>
    </MaterialSelect>
    { clearable &&
      <IconButton
        className={classes.clearButton}
        onClick={() => {
          onChange({ type: 'REMOVE_MEASURE', id })
          onChange({ type: 'REMOVE_EVENT_SELECT', id })
        }}
        aria-label="Delete"
      >
        <ClearIcon />
      </IconButton>
    }
    <Select
      defaultValue={defaultValue}
      styles={customStyles}
      options={options}
      onChange={(value, action) => handleChange(value, action, id, onChange)}
    />
  </div>
)

export default withStyles(styles)(EventsSelect);
