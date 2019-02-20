import React, { Component } from 'react';

import Grid from '@material-ui/core/Grid';
import Fab from '@material-ui/core/Fab';
import AddIcon from '@material-ui/icons/Add';

import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';

import EventsSelect, { defaultEvent } from './EventsSelect';
import DimensionSelect from './DimensionSelect';
import VisualizationToggle from './VisualizationToggle';
import GranularitySelect, { defaultGranularity } from './GranularitySelect';
import DateRangeSelect, { defaultDateRange } from './TimeSelect';
import SaveButton from './SaveButton';
import withQueryBuilder from './withQueryBuilder';

import Chart from '../Charts';

const styles = {
  dimensionSelectContainer: {
    display: "inline-flex",
    marginLeft: "20px"
  },
  granularitySelectContainer: {
    paddingTop: 10,
    marginRight: 20
  }
};

const defaultVisualizationType = 'line';
const DEFAULT_EVENT_SELECT_ID = 1;

const buildQuery = ({ dateRange, granularity, measures, dimensions, visualizationType }) => ({
  type: visualizationType,
  query: {
    measures: Object.values(measures),
    dimensions,
    timeDimensions: [{
      dimension: 'Events.time',
      dateRange,
      granularity
    }]
  }
})

class QueryBuilder extends Component {
  get query() {
    return buildQuery(this.props);
  }

  get canAddEventsSelects() {
    return this.props.eventSelects.length < 3;
  }

  get canRemoveEventsSelects() {
    return this.props.eventSelects.length > 1;
  }

  render() {
    const {
      classes,
      onChange,
      eventSelects,
      granularity,
      visualizationType
    } = this.props;

    return (
      <Grid container spacing={24}>
        <Grid item xs={10}>
          <Grid container alignItems='flex-end' spacing={16}>
            {eventSelects.map(i => (
              <Grid key={i} item>
                <EventsSelect id={i}
                  defaultValue={(i === DEFAULT_EVENT_SELECT_ID && defaultEvent)}
                  onChange={onChange}
                  clearable={this.canRemoveEventsSelects}
                />
              </Grid>
            ))}
            { this.canAddEventsSelects &&
              <Grid item>
                <Fab onClick={() => onChange({ type: "ADD_EVENT_SELECT" }) } size="small" aria-label="Add" className={classes.fab}>
                  <AddIcon />
                </Fab>
              </Grid>
            }
          </Grid>
        </Grid>
        <Grid item xs={2}>
          <Grid container justify="flex-end">
            <SaveButton  />
          </Grid>
        </Grid>
          <Grid item xs={12}>
            <Button variant="contained" color="secondary" disabled>
              By
            </Button>
            <div className={classes.dimensionSelectContainer}>
              <DimensionSelect onChange={onChange} />
            </div>
          </Grid>
          <Grid item xs={12}>
            <Grid container justify='flex-end'>
              <Grid item>
                <div className={classes.granularitySelectContainer}>
                  <DateRangeSelect onChange={onChange} defaultValue={defaultDateRange} />
                </div>
              </Grid>
              <Grid item>
                {granularity &&
                  <div className={classes.granularitySelectContainer}>
                    <GranularitySelect
                      defaultValue={defaultGranularity}
                      onChange={onChange} />
                  </div>
                }
              </Grid>
              <Grid item>
                <div>
                  <VisualizationToggle
                    value={visualizationType}
                    onChange={onChange}
                  />
                </div>
              </Grid>
            </Grid>
          </Grid>
          <Grid item xs={12}>
            <Chart {...this.query} />
          </Grid>
      </Grid>
    )
  }
}

const initialData = {
  eventSelects: [DEFAULT_EVENT_SELECT_ID],
  measures: { [DEFAULT_EVENT_SELECT_ID]: defaultEvent.value },
  dimensions: [],
  dateRange: defaultDateRange.value,
  granularity: defaultGranularity.value,
  visualizationType: defaultVisualizationType
}

export default withQueryBuilder(initialData, withStyles(styles)(QueryBuilder));
