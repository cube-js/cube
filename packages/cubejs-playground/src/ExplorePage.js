/* global window */
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
      apiUrl: result.apiUrl || window.location.href.split('#')[0].replace(/\/$/, '')
    });
  }

  cubejsApi() {
    const { cubejsToken, apiUrl } = this.state;
    if (!this.cubejsApiInstance && cubejsToken) {
      this.cubejsApiInstance = cubejs(cubejsToken, {
        apiUrl: `${apiUrl}/cubejs-api/v1`
      });
    }
    return this.cubejsApiInstance;
  }

  render() {
    const { cubejsToken, apiUrl } = this.state;
    return this.cubejsApi() && (
      <PlaygroundQueryBuilder
        query={{}}
        cubejsApi={this.cubejsApi()}
        apiUrl={apiUrl}
        cubejsToken={cubejsToken}
        dashboardSource={this.dashboardSource}
      />
    ) || null;
  }
}

export default ExplorePage;
