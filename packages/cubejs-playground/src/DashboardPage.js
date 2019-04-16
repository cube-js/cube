import React, { Component } from 'react';
import { Spin, Button } from 'antd';
import DashboardSource from "./DashboardSource";
import DashboardRenderer from './DashboardRenderer';

class DashboardPage extends Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  async componentDidMount() {
    this.dashboardSource = new DashboardSource();
    await this.loadDashboard();
  }

  async loadDashboard(createApp) {
    this.setState({
      appCode: null,
      sourceFiles: null,
      loadError: null
    });
    await this.dashboardSource.load(createApp);
    this.setState({
      appCode: !this.dashboardSource.loadError && this.dashboardSource.dashboardAppCode(),
      sourceFiles: this.dashboardSource.sourceFiles,
      loadError: this.dashboardSource.loadError
    });
  }

  render() {
    const { appCode, sourceFiles, loadError } = this.state;
    if (loadError) {
      return (
        <div style={{ textAlign: 'center' }}>
          <h2>
            {loadError}
          </h2>
          <p style={{ textAlign: 'center' }}>
            <Button
              type="primary"
              size="large"
              onClick={() => this.loadDashboard(true)}
            >
              Create dashboard app template in your project directory
            </Button>
          </p>
        </div>
      );
    }
    return appCode && <DashboardRenderer source={appCode} sourceFiles={sourceFiles}/>
      || (
        <h2 style={{ textAlign: 'center' }}>
          <Spin />
          &nbsp;Creating dashboard react-app. It may take several minutes...
        </h2>
      );
  }
}

export default DashboardPage;
