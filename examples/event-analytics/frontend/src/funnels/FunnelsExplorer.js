import React, { Component } from 'react';

import { withStyles } from '@material-ui/core/styles';
import Grid from '@material-ui/core/Grid';
import Button from '@material-ui/core/Button';
import EditIcon from '@material-ui/icons/Edit';
import AddIcon from '@material-ui/icons/Add';
import Select from 'react-select';

import Funnel from './Funnel';
import CompletionRate from './CompletionRate';
import DateRangeSelect, { defaultDateRange } from '../components/DateRangeSelect';

const options = [
  { value: 'ReportsFunnel', label: 'Reports' },
  { value: 'FunnelsUsageFunnel', label: 'Funnels Usage' },
  { value: 'FunnelsEditFunnel', label: 'Funnels Editing' }
]

const styles = theme => ({
  button: {
    marginRight: theme.spacing.unit,
    marginLeft: theme.spacing.unit
  },
  icon: {
    marginRight: theme.spacing.unit,
  }
});

class FunnelsExplorer extends Component {
  constructor(props) {
    super(props)
    this.state = {
      funnelId: null,
      dateRange: defaultDateRange.value
    }
  }

  get completionRateQuery() {
    return {
      measures: [`${this.state.funnelId}.conversionsPercent`],
      filters: [
        {
          dimension: `${this.state.funnelId}.time`,
          operator: `inDateRange`,
          values: this.state.dateRange
        }
      ]
    }
  }

  get query() {
    return {
      measures: [`${this.state.funnelId}.conversions`],
      dimensions: [`${this.state.funnelId}.step`],
      filters: [
        {
          dimension: `${this.state.funnelId}.time`,
          operator: `inDateRange`,
          values: this.state.dateRange
        }
      ]
    }
  }

  render() {
    const { classes } = this.props;

    return (
      <>
      <Grid container spacing={24}>
        <Grid item xs={3}>
          <Select
            options={options}
            placeholder="Select a funnel"
            onChange={(value) => {
              window.snowplow('trackStructEvent', 'Funnels', 'Funnel Selected');
              this.setState({funnelId: value.value})
            }}
          />
        </Grid>
        <Grid item xs={3}>
          <Button
            className={classes.button}
            variant="contained"
            disabled={!this.state.funnelId}
            onClick={() => {
              window.snowplow('trackStructEvent', 'Funnels', 'Edit Button Clicked');
              alert("Editing funnels in demo mode isn't available")
            }}
          >
            <EditIcon className={classes.icon} />
            Edit
          </Button>
          <Button
            variant="contained"
            className={classes.button}
            onClick={() => {
              window.snowplow('trackStructEvent', 'Funnels', 'New Button Clicked');
              alert("Adding new funnels in demo mode isn't available")
            }}
          >
            <AddIcon className={classes.icon} />
            New
          </Button>
        </Grid>
        { this.state.funnelId &&
          <>
            <Grid item xs={12}>
                <Grid container>
                  <Grid item xs={6}>
                    <CompletionRate query={this.query} id={this.state.funnelId} />
                  </Grid>
                  <Grid item xs={6}>
                    <Grid container justify="flex-end">
                      <DateRangeSelect
                        onChange={({ value }) => this.setState({ dateRange: value}) }
                        defaultValue={defaultDateRange}
                      />
                    </Grid>
                  </Grid>
                </Grid>
            </Grid>
            <Grid item xs={12}>
              <Funnel
                dateRange={this.state.dateRange}
                query={this.query}
              />
            </Grid>
          </>
        }
      </Grid>
      </>
    )
  }
}

export default withStyles(styles)(FunnelsExplorer);
