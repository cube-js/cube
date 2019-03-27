import React, { Component } from 'react';
import cubejs from '@cubejs-client/core';
import { fetch } from 'whatwg-fetch';
import DashboardSource from './DashboardSource';
import PlaygroundQueryBuilder from './PlaygroundQueryBuilder';

class ExplorePage extends Component {
  constructor(props) {
    super(props);
    this.state = {};
    this.dashboardSource = new DashboardSource();
  }

  async componentDidMount() {
    const res = await fetch('/playground/context');
    const result = await res.json();
    this.setState({
      cubejsToken: result.cubejsToken,
      apiUrl: result.apiUrl
    });
  }

  cubejsApi() {
    if (!this.cubejsApiInstance && this.state.cubejsToken) {
      this.cubejsApiInstance = cubejs(this.state.cubejsToken, {
        apiUrl: this.state.apiUrl + '/cubejs-api/v1'
      });
    }
    return this.cubejsApiInstance;
  }

  render() {
    return this.cubejsApi() && (<PlaygroundQueryBuilder
      query={{}}
      cubejsApi={this.cubejsApi()}
      apiUrl={this.state.apiUrl}
      cubejsToken={this.state.cubejsToken}
      dashboardSource={this.dashboardSource}
    />) || null;
  }
}

export default ExplorePage;
