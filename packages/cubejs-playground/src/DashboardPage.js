import React, { Component } from 'react';
import { Spin } from 'antd';
import DashboardSource from "./DashboardSource";
import DashboardRenderer from './DashboardRenderer';

class DashboardPage extends Component {
  constructor(props) {
    super(props);
    this.state = {};
  }

  async componentDidMount() {
    this.dashboardSource = new DashboardSource();
    await this.dashboardSource.load();
    this.setState({
      appCode: this.dashboardSource.dashboardAppCode(),
      sourceFiles: this.dashboardSource.sourceFiles
    });
  }

  render() {
    const { appCode, sourceFiles } = this.state;
    return appCode && <DashboardRenderer source={appCode} sourceFiles={sourceFiles}/> || <Spin />;
  }
}

export default DashboardPage;
