/* globals window */
import React, { Component } from 'react';
import styled from 'styled-components';
import {
  Button, Switch, Menu, Dropdown, Icon, Form, Row, Col, Card, Modal, Typography
} from 'antd';
import { withRouter } from "react-router-dom";
import DashboardSource from "../DashboardSource";
import fetch from '../playgroundFetch';
import { frameworks } from "../ChartContainer";
import { playgroundAction } from "../events";
import { chartLibraries } from "../ChartRenderer";

const MarginFrame = ({ children }) => (
  <div style={{ marginTop: 50, margin: 25 }}>
    { children }
  </div>
);

const RecipeCard = styled(Card)`
  border-radius: 4px;
  button {
    display: none;
    position: absolute;
    margin-left: -64px;
    top: 80px;
    left: 50%;
  }
  padding: 16px;

  && .ant-card-cover {
    height: 168px;
    background: #EEEEF5;
    border-radius: 4px;
  }

  &&.ant-card-hoverable:hover {
    box-shadow: 0px 15px 20px rgba(67, 67, 107, 0.1);
    button { display: block; }
  }

  && .ant-card-meta {
    text-align: center;
  }

  && .ant-card-meta-title {
    white-space: unset;
    color: #43436B;
  }

  && .ant-card-meta-description {
    color: #A1A1B5;
    font-size: 11px;
  }
`;

const CreateOwnDashboardForm = styled(Form)`
  && {
    .ant-dropdown-trigger {
      width: 100%
    }
  }
`;

const StyledTitle = styled(Typography.Text)`
  display: block;
  font-size: 16px;
  margin-bottom: 25px;
  margin-left: 15px;
`

class TemplateGalleryPage extends Component {
  constructor(props) {
    super(props);
    this.state = {
      chartLibrary: chartLibraries[0].value,
      framework: 'react',
      templatePackageName: 'react-antd-dynamic'
    };
  }

  async componentDidMount() {
    this.dashboardSource = new DashboardSource();
  }

  async loadDashboard(createApp) {
    const { chartLibrary, templatePackageName } = this.state;
    this.setState({
      appCode: null,
      loadError: null
    });
    try {
      await this.dashboardSource.load(createApp, { chartLibrary, templatePackageName });
      this.setState({
        dashboardStarting: false,
        appCode: !this.dashboardSource.loadError && this.dashboardSource.dashboardCreated,
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

  render() {
    const { chartLibrary, framework, templatePackageName, createOwnModalVisible, enableWebSocketTransport } = this.state;
    const { history } = this.props;
    const currentLibraryItem = chartLibraries.find(m => m.value === chartLibrary);
    const frameworkItem = frameworks.find(m => m.id === framework);
    const templatePackage = this.dashboardSource && this.dashboardSource.templatePackages
      .find(m => m.name === templatePackageName);

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

    const templatePackagesMenu = (
      <Menu
        onClick={(e) => {
          playgroundAction('Set Template Package', { templatePackageName: e.key });
          this.setState({ templatePackageName: e.key });
        }}
      >
        {
          (this.dashboardSource && this.dashboardSource.templatePackages || []).map(f => (
            <Menu.Item key={f.name}>
              {f.description}
            </Menu.Item>
          ))
        }
      </Menu>
    );

    const {
      appCode, dashboardPort, loadError, dashboardRunning, dashboardStarting, dashboardAppPath
    } = this.state;

    const recipes = [{
      name: 'React Antd dynamic dashboard with Chart.js',
      description: 'Use this template if you need to create dashboard application where users can edit their dashboards',
      templatePackages: ['create-react-app', 'react-antd-dynamic', 'chartjs-charts', 'antd-tables', 'credentials']
    }, {
      name: 'React Antd static dashboard with Recharts',
      description: 'Use this template if you want to create static dashboard application and add charts to it using code or Cube.js Playground',
      templatePackages: ['create-react-app', 'react-antd-static', 'recharts-charts', 'antd-tables', 'credentials']
    }];

    const CreateOwnModal = (
      <Modal
        title="Create your own Dashboard App"
        visible={createOwnModalVisible}
        onOk={async () => {
          this.setState({ createOwnModalVisible: false });
          const templatePackages = [
            'create-react-app',
            templatePackageName,
            `${chartLibrary}-charts`,
            `${templatePackageName.match(/^react-(\w+)/)[1]}-tables`, // TODO
            'credentials'
          ].concat(enableWebSocketTransport ? ['web-socket-transport'] : []);
          await this.dashboardSource.applyTemplatePackages(templatePackages);
          history.push('/dashboard');
        }}
        onCancel={() => this.setState({ createOwnModalVisible: false })}
      >
        <CreateOwnDashboardForm>
          <Form.Item label="Framework">
            <Dropdown overlay={frameworkMenu}>
              <Button>
                {frameworkItem && frameworkItem.title}
                <Icon type="down" />
              </Button>
            </Dropdown>
          </Form.Item>
          {
            frameworkItem && frameworkItem.docsLink && (
              <p style={{ paddingTop: 24 }}>
                We do not support&nbsp;
                {frameworkItem.title}
                &nbsp;dashboard scaffolding generation yet.
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
              </p>
            )
          }
          <Form.Item label="Main Template">
            <Dropdown
              overlay={templatePackagesMenu}
              disabled={!!frameworkItem.docsLink}
            >
              <Button>
                {templatePackage && templatePackage.description}
                <Icon type="down" />
              </Button>
            </Dropdown>
          </Form.Item>
          <Form.Item label="Charting Library">
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
          <Form.Item label="Web Socket Transport (Real-time)">
            <Switch
              checked={enableWebSocketTransport}
              onChange={(checked) => this.setState({ enableWebSocketTransport: checked })}
            />
          </Form.Item>
        </CreateOwnDashboardForm>
      </Modal>
    );

    const recipeCards = recipes.map(({ name, description, templatePackages }) => (
      <Col span={6} key={name}>
        <RecipeCard
          hoverable
          bordered={false}
          cover={<div />}
        >
          <Card.Meta title={name} description={description} />
          <Button
            type="primary"
            onClick={async () => {
              await this.dashboardSource.applyTemplatePackages(templatePackages);
              history.push('/dashboard');
            }}
          >
            Create App
          </Button>
        </RecipeCard>
      </Col>
    )).concat([
      <Col span={6} key="own">
        <RecipeCard
          hoverable
          bordered={false}
          cover={<Icon type="plus" size="large" style={{ fontSize: 160 }}/>}
        >
          <Card.Meta
            title="Create your Own"
            description="Mix different templates together to create your own dashboard application"
          />
        </RecipeCard>
        {CreateOwnModal}
      </Col>
    ]);

    return (
      <MarginFrame>
        <StyledTitle>
          Build your app from one the popular templates below or create your own
        </StyledTitle>
        <Row type="flex" align="top" gutter={24}>
          {recipeCards}
        </Row>
      </MarginFrame>
    );
  }
}

export default withRouter(TemplateGalleryPage);
