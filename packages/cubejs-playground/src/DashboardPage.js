/* globals window */
import React, { Component } from 'react';
import { Spin, Button } from 'antd';
import DashboardSource from "./DashboardSource";
import fetch from './playgroundFetch';

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
      loadError: null
    });
    await this.dashboardSource.load(createApp);
    this.setState({
      dashboardStarting: false,
      appCode: !this.dashboardSource.loadError && this.dashboardSource.dashboardAppCode(),
      loadError: this.dashboardSource.loadError
    });
    const dashboardStatus = await (await fetch('/playground/dashboard-app-status')).json();
    this.setState({
      dashboardRunning: dashboardStatus.running,
      dashboardPort: dashboardStatus.dashboardPort
    });
  }

  async startDashboardApp() {
    this.setState({
      dashboardStarting: true
    });
    await fetch('/playground/start-dashboard-app');
    await this.loadDashboard();
  }

  render() {
    const {
      appCode, dashboardPort, loadError, dashboardRunning, dashboardStarting
    } = this.state;
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
    if (!appCode) {
      return (
        <h2 style={{ textAlign: 'center' }}>
          <Spin />
          &nbsp;Creating dashboard react-app. It may take several minutes...
        </h2>
      );
    }
    if (!dashboardRunning) {
      return (
        <div style={{ textAlign: 'center' }}>
          <h2>
            Dashboard App is not running.
            <br/>
            Please start dashboard app or run it manually using `$ npm run start` in dashboard-app directory.
          </h2>
          <p style={{ textAlign: 'center' }}>
            <Button
              type="primary"
              size="large"
              loading={dashboardStarting}
              onClick={() => this.startDashboardApp(true)}
            >
              Start dashboard app
            </Button>
          </p>
        </div>
      );
    }
    return (
      <iframe
        src={`http://${window.location.hostname}:${dashboardPort}`}
        style={{
          width: '100%', height: '100%', border: 0, borderRadius: 4, overflow: 'hidden'
        }}
        sandbox="allow-modals allow-forms allow-popups allow-scripts allow-same-origin"
        title="Dashboard"
      />
    );
  }
}

export default DashboardPage;
