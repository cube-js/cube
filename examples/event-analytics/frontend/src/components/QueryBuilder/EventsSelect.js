import React from 'react';
import Select from 'react-select';

import { withStyles } from '@material-ui/core/styles';
import { default as MaterialSelect } from '@material-ui/core/Select';
import MenuItem from '@material-ui/core/MenuItem';
import IconButton from '@material-ui/core/IconButton';
import ClearIcon from '@material-ui/icons/Clear';

// Every event could either total or unique. Total events have plain key, ex.: 'Events.anyEvent'
// Unique events have the same key, except with postfix `Uniq`, ex: 'Events.anyEventUniq'
const options = [
  { value: 'Events.anyEvent', label: 'Any Event', default: true },
  { value: 'Events.pageView', label: 'Page View' },
  { value: 'Events.Navigation__Menu_Closed', label: 'Navigation: Menu Closed' },
  { value: 'Events.Navigation__Menu_Opened', label: 'Navigation: Menu Opened' },
  { value: 'Events.Reports__Event_Selected', label: 'Reports: Event Selected' },
  { value: 'Events.Reports__Property_Selected', label: 'Reports: Property Selected' },
  { value: 'Events.Reports__Date_Range_Changed', label: 'Reports: Date Range Changed' },
  { value: 'Events.Reports__Visualization_Changed', label: 'Reports: Visualization Changed' }
]
export const defaultEvent = options.find(i => i.default)

const withEventType = (value, eventType) => {
  if (eventType === 'total') {
    return value;
  } else {
    return `${value}Uniq`;
  }
}
const handleChange = (value, eventType, id, onChangeProp) => {
  onChangeProp({ type: "REMOVE_MEASURE", id })
  onChangeProp({
    type: 'ADD_MEASURE',
    value: withEventType(value, eventType),
    id
  })
  window.snowplow('trackStructEvent', 'Reports', 'Event Selected');
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

class EventsSelect extends React.Component {
  state = {
    eventType: 'total'
  }

  render() {
    const { onChange, defaultValue, id, clearable, classes } = this.props;

    return (
      <div className={classes.container}>
        <MaterialSelect
          disableUnderline
          value={this.state.eventType}
          onChange={(event) => {
            const newType = event.target.value
            if (newType !== this.state.eventType) {
              this.setState({ eventType: event.target.value })
              const value = this.refs.select.state.value.value
              handleChange(value, newType, id, onChange)
            }
          }}
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
          ref="select"
          defaultValue={defaultValue}
          styles={customStyles}
          options={options}
          onChange={(value, action) => handleChange(value.value, this.state.eventType, id, onChange)}
        />
      </div>
    )
  }
}

export default withStyles(styles)(EventsSelect);
