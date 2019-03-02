import React, { Component } from 'react';

import Grid from '@material-ui/core/Grid';
import Select from 'react-select';

import Funnel from './Funnel';

const options = [
  { value: 'Reports', label: 'Reports' }
]

class FunnelsExplorer extends Component {
  constructor(props) {
    super(props)
    this.state = {
      funnelId: null
    }
  }
  render() {
    return (
      <>
      <Grid container spacing={24}>
        <Grid item xs={3}>
          <Select
            options={options}
            placeholder="Select a funnel"
            onChange={(value) => this.setState({funnelId: value.value}) }
          />
        </Grid>
        { this.state.funnelId &&
          <Grid item xs={12}>
            <Funnel id={this.state.funnelId} />
          </Grid>
        }
      </Grid>
      </>
    )
  }
}

export default FunnelsExplorer;
