import React, { Component } from 'react';
import "antd/dist/antd.css";
import "./index.css";
import { Layout} from "antd";
import cubejs from '@cubejs-client/core';
import { fetch } from 'whatwg-fetch';
import PlaygroundQueryBuilder from './PlaygroundQueryBuilder';

const { Header, Content } = Layout;

const API_URL = ``;

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  cubejsApi() {
    if (!this.cubejsApiInstance && this.state.cubejsToken) {
      this.cubejsApiInstance = cubejs(this.state.cubejsToken, {
        apiUrl: this.state.apiUrl + '/cubejs-api/v1'
      });
    }
    return this.cubejsApiInstance;
  }

  async componentDidMount() {
    const res = await fetch(API_URL + '/playground/context');
    const result = await res.json();
    this.setState({
      cubejsToken: result.cubejsToken,
      apiUrl: result.apiUrl
    });
    window.analytics && window.analytics.identify(result.anonymousId);
  }

  render() {
    return [
      <Layout>
        <Header>
          <img src='./cubejs-logo.svg' style={{ display: 'inline', width: 50}} />
          <h2 style={{ color: '#fff', display: 'inline' }}>Cube.js Playground</h2>
        </Header>
        <Content style={{ padding: '25px', margin: '25px' }}>
          {this.cubejsApi() && <PlaygroundQueryBuilder
            query={{
              measures: ['Orders.count'],
              dimensions: ['Orders.status']
            }}
            cubejsApi={this.cubejsApi()}
            apiUrl={this.state.apiUrl}
            cubejsToken={this.state.cubejsToken}
          />}
        </Content>
      </Layout>
    ];
  }
}

export default App;
