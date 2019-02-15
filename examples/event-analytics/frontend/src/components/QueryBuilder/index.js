import React, { Component } from 'react';
import moment from 'moment';

import Grid from '@material-ui/core/Grid';

import Button from '@material-ui/core/Button';
import { withStyles } from '@material-ui/core/styles';

import EventsSelect from './EventsSelect';
import DimensionSelect from './DimensionSelect';
import VisualizationToggle from './VisualizationToggle';
import GranularitySelect from './GranularitySelect';
import SaveButton from './SaveButton';

import Chart from '../Charts';

const styles = theme => ({
  dimensionSelectContainer: {
    display: "inline-flex",
    marginLeft: "20px"
  },
  granularitySelectContainer: {
    paddingTop: 10,
    marginRight: 20
  }
});

const defaultDateRange = [
  moment().subtract(14,'d').format('YYYY-MM-DD'),
  moment().format('YYYY-MM-DD'),
];
const defaultGranularity = 'day';
const defaultVisualizationType = 'line';

const buildQuery = ({ dateRange, granularity, measures, dimensions, visualizationType }) => ({
  type: visualizationType,
  query: {
    measures,
    dimensions,
    timeDimensions: [{
      dimension: 'Events.time',
      dateRange,
      granularity
    }]
  }
})

const resolveGranularity = (visualizationType, state) => {
  // Reset granularity if pie chart selected,
  // but memorized previousily selected
  if (visualizationType === 'pie') {
    return { granularity: null, memorizedGranularity: state.granularity }
  // For the rest of the charts use currently selected granularity,
  // or in case it is null the memorized one
  } else {
    return {
      granularity: (state.granularity || state.memorizedGranularity),
      memorizedGranularity: null
    }
  }
}

const reducer = (state, action) => {
  switch (action.type) {
    case 'CHANGE_GRANULARITY':
      return {
        ...state,
        granularity: action.value
      }
    case 'CHANGE_VISUALIZATION_TYPE':
      const {
        granularity,
        memorizedGranularity
      } = resolveGranularity(action.value, state)
      return {
        ...state,
        granularity,
        memorizedGranularity,
        visualizationType: action.value
      }
    case 'ADD_DIMENSION':
      return {
        ...state,
        dimensions: [action.dimension]
      }
    case 'REMOVE_DIMENSION':
      return {
        ...state,
        dimensions: []
      }
    case 'ADD_MEASURE':
      return {
        ...state,
        measures: [action.measure]
      }
    case 'REMOVE_MEASURE':
      return {
        ...state,
        measures: []
      }
    default:
      return state
  }
}

class QueryBuilder extends Component {
  state = {
    measures: [],
    dimensions: [],
    dateRange: defaultDateRange,
    granularity: defaultGranularity,
    visualizationType: defaultVisualizationType
  };

  onChange(action) {
    this.setState(reducer(this.state, action));
  }

  get ready() {
    return this.state.measures.length > 0;
  }

  get query() {
    return buildQuery(this.state);
  }

  render() {
    const { classes } = this.props;
    const onChange = this.onChange.bind(this);

    return (
      <Grid container spacing={24}>
        <Grid item xs={6}>
          <EventsSelect onChange={onChange} />
        </Grid>
        <Grid item xs={6}>
          <Grid container justify="flex-end">
            <SaveButton disabled={!this.ready} />
          </Grid>
        </Grid>
        { this.ready && (
          <>
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
                  { this.state.granularity &&
                    <div className={classes.granularitySelectContainer}>
                      <GranularitySelect
                        value={this.state.granularity}
                        onChange={onChange} />
                    </div>
                  }
                </Grid>
                <Grid item>
                  <div>
                    <VisualizationToggle
                      value={this.state.visualizationType}
                      onChange={onChange}
                    />
                  </div>
                </Grid>
              </Grid>
            </Grid>
            <Grid item xs={12}>
              <Chart {...this.query} />
            </Grid>
          </>
          )
        }
      </Grid>
    )
  }
}

export default withStyles(styles)(QueryBuilder);
