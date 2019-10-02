/* globals window */
import React, { Component } from 'react';
import {
  Spin, Button, Alert, Menu, Dropdown, Icon, Form
} from 'antd';
import DashboardSource from "./DashboardSource";
import fetch from './playgroundFetch';
import { frameworks } from "./ChartContainer";
import { playgroundAction } from "./events";
import { chartLibraries } from "./ChartRenderer";

const Frame = ({ children }) => (
  <div style={{ textAlign: 'center', marginTop: 50 }}>
    { children }
  </div>
);

const Hint = () => (
  <p style={{ width: 450, margin: "20px auto" }}>
    Dashboard App is a convenient way to setup and deploy frontend app to work with Cube.js backend. You can learn more about it the <a href="https://cube.dev/docs/dashboard-app" target="_blankl">Cube.js docs</a>.
  </p>
);

class DashboardPage extends Component {
  constructor(props) {
    super(props);
    this.state = {
      chartLibrary: chartLibraries[0].value,
      framework: 'react'
    };
  }

  async componentDidMount() {
    this.dashboardSource = new DashboardSource();
    await this.loadDashboard();
  }

  async loadDashboard(createApp) {
    const { chartLibrary } = this.state;
    this.setState({
      appCode: null,
      loadError: null
    });
    try {
      await this.dashboardSource.load(createApp, { chartLibrary });
      this.setState({
        dashboardStarting: false,
        appCode: !this.dashboardSource.loadError && this.dashboardSource.dashboardAppCode(),
        loadError: this.dashboardSource.loadError
      });
      const dashboardStatus = await (await fetch('/playground/dashboard-app-status')).json();
      this.setState({
        dashboardRunning: dashboardStatus.running,
        dashboardPort: dashboardStatus.dashboardPort,
        dashboardAppPath: dashboardStatus.dashboardAppPath
      });
      if (createApp) {
        await this.startDashboardApp();
      }
    } catch (e) {
      this.setState({
        dashboardStarting: false,
        loadError: <pre>{e.toString()}</pre>
      });
      throw e;
    }
  }

  async startDashboardApp() {
    this.setState({
      dashboardStarting: true
    });
    await fetch('/playground/start-dashboard-app');
    await this.loadDashboard();
  }

  render() {
    const { chartLibrary, framework } = this.state;
    const currentLibraryItem = chartLibraries.find(m => m.value === chartLibrary);
    const frameworkItem = frameworks.find(m => m.id === framework);

    const chartLibrariesMenu = (
      <Menu
        onClick={(e) => {
          playgroundAction('Set Chart Library', { chartLibrary: e.key });
          this.setState({ chartLibrary: e.key });
        }}
      >
        {
          chartLibraries.map(library => (
            <Menu.Item key={library.value}>
              {library.title}
            </Menu.Item>
          ))
        }
      </Menu>
    );

    const frameworkMenu = (
      <Menu
        onClick={(e) => {
          playgroundAction('Set Framework', { framework: e.key });
          this.setState({ framework: e.key });
        }}
      >
        {
          frameworks.map(f => (
            <Menu.Item key={f.id}>
              {f.title}
            </Menu.Item>
          ))
        }
      </Menu>
    );

    const {
      appCode, dashboardPort, loadError, dashboardRunning, dashboardStarting, dashboardAppPath
    } = this.state;
    if (loadError) {
      return (
        <Frame>
          <h2>
            {loadError}
          </h2>
          <Form layout="inline">
            <Form.Item>
              <Dropdown overlay={frameworkMenu}>
                <Button>
                  {frameworkItem && frameworkItem.title}
                  <Icon type="down" />
                </Button>
              </Dropdown>
            </Form.Item>
            <Form.Item>
              <Dropdown
                overlay={chartLibrariesMenu}
                disabled={!!frameworkItem.docsLink}
              >
                <Button>
                  {currentLibraryItem && currentLibraryItem.title}
                  <Icon type="down" />
                </Button>
              </Dropdown>
            </Form.Item>
          </Form>
          {
            frameworkItem && frameworkItem.docsLink && (
              <h2 style={{ paddingTop: 24, textAlign: 'center' }}>
                We do not support&nbsp;
                {frameworkItem.title}
                &nbsp;dashboard scaffolding generation yet.
                < br/>
                Please refer to&nbsp;
                <a
                  href={frameworkItem.docsLink}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={() => playgroundAction('Unsupported Dashboard Framework Docs', { framework })}
                >
                  {frameworkItem.title}
                  &nbsp;docs
                </a>
                &nbsp;to see on how to use it with Cube.js.
              </h2>
            )
          }
          <p style={{ marginTop: 25 }}>
            <Button
              type="primary"
              size="large"
              onClick={() => this.loadDashboard(true)}
              disabled={!!frameworkItem.docsLink}
            >
              Create dashboard app template in your project directory
            </Button>
          </p>
          <Hint />
        </Frame>
      );
    }
    if (!appCode) {
      return (
        <Frame>
          <h2>
            &nbsp;Creating Dashboard App
          </h2>
          <p>
            <Spin tip="It may take several minutes. Please check console for progress..." />
          </p>
          <Hint />
        </Frame>
      );
    }
    if (!dashboardRunning) {
      return (
        <Frame>
          <h2>
            Dashboard App is not running
          </h2>
          <h3>
            Please start dashboard app or run it manually using
            <code className="inline-code">$ npm run start</code>
            <br />
            in&nbsp;
            <b>{dashboardAppPath}</b>
            &nbsp;directory.
          </h3>
          <p style={{ marginTop: 25 }}>
            <Button
              type="primary"
              size="large"
              loading={dashboardStarting}
              onClick={() => this.startDashboardApp(true)}
            >
              {dashboardStarting ? 'Dashboard app is starting. It may take a while. Please check console for progress...' : 'Start dashboard app'}
            </Button>
          </p>
          <Hint />
        </Frame>
      );
    }
    const devServerUrl = `http://${window.location.hostname}:${dashboardPort}`;
    return (
      <div
        style={{
          height: '100%', width: '100%', padding: "15px 30px 30px 30px", background: "#fff"
        }}
      >
        <Alert
          message={(
            <span>
              This dashboard app can be edited at&nbsp;
              <b>{dashboardAppPath}</b>
              .
              Dev server is running at&nbsp;
              <a href={devServerUrl} target="_blank" rel="noopener noreferrer">{devServerUrl}</a>
              .
              Learn more how to customize and deploy it at&nbsp;
              <a href="https://cube.dev/docs/dashboard-app">Cube.js&nbsp;docs</a>
              .&nbsp;
              <a onClick={() => window.location.reload()} style={{ cursor: 'pointer' }}>Refresh page</a>
              &nbsp;if it is empty.
            </span>
          )}
          type="info"
          closable
          style={{ marginBottom: 15 }}
        />
        <iframe
          src={devServerUrl}
          style={{
            width: '100%', height: '100%', border: 0, borderRadius: 4, overflow: 'hidden'
          }}
          sandbox="allow-modals allow-forms allow-popups allow-scripts allow-same-origin"
          title="Dashboard"
        />
      </div>
    );
  }
}

export default DashboardPage;
