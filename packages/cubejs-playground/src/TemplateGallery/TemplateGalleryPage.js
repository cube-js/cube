/* globals window */
import React, { Component } from 'react';
import styled from 'styled-components';
import { Switch, Menu, Dropdown, Icon, Form, Row, Col, Card, Modal, Typography } from 'antd';
import { withRouter } from "react-router-dom";
import DashboardSource from "../DashboardSource";
import fetch from '../playgroundFetch';
import { frameworks } from "../ChartContainer";
import { playgroundAction } from "../events";
import { chartLibraries } from "../ChartRenderer";
import Button from "../components/Button";
import { ReactComponent as PlusSVG } from "./plus.svg";
import CreateOwnModal from "./CreateOwnModal";

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
  svg path {
    transition: stroke 0.25s ease;
  }

  && .ant-card-cover {
    height: 168px;
    border-radius: 4px;
    background: ${props => props.createYourOwn ? "#F8F8FB" : "#EEEEF5"}
    display: flex;
    align-items: center;
  }

  &&.ant-card-hoverable:hover {
    box-shadow: 0px 15px 20px rgba(67, 67, 107, 0.1);
    button { display: block; }
    svg path {
      stroke: #7A77FF;
    }
  }

  && .ant-card-body {
    min-height: 144px;
    display: flex;
    align-items: center;
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

  render() {
    const { chartLibrary, framework, templatePackageName, createOwnModalVisible, enableWebSocketTransport } = this.state;
    const { history } = this.props;
    const currentLibraryItem = chartLibraries.find(m => m.value === chartLibrary);
    const frameworkItem = frameworks.find(m => m.id === framework);
    const templatePackage = this.dashboardSource && this.dashboardSource.templatePackages
      .find(m => m.name === templatePackageName);

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
          onClick={() => this.setState({ createOwnModalVisible: true })}
          hoverable
          createYourOwn
          bordered={false}
          cover={<PlusSVG />}
        >
          <Card.Meta
            title="Create your Own"
            description="Mix different templates together to create your own dashboard application"
          />
        </RecipeCard>
        <CreateOwnModal
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
          onChange={(key, value) => this.setState({ [key]: value })}
          chartLibraries={chartLibraries}
          currentLibraryItem={currentLibraryItem}
          frameworks={frameworks}
          framework={framework}
          frameworkItem={frameworkItem}
          templatePackages={this.dashboardSource && this.dashboardSource.templatePackages}
          templatePackage={templatePackage}
          enableWebSocketTransport={enableWebSocketTransport}
        />
      </Col>
    ]);

    return (
      <MarginFrame>
        <StyledTitle>
          Build your app from one the popular templates below or <a onClick={() => this.setState({ createOwnModalVisible: true })}>create your own</a>
        </StyledTitle>
        <Row type="flex" align="top" gutter={24}>
          {recipeCards}
        </Row>
      </MarginFrame>
    );
  }
}

export default withRouter(TemplateGalleryPage);
