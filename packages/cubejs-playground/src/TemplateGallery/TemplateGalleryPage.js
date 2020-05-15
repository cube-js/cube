import React, { Component } from 'react';
import styled from 'styled-components';
import '@ant-design/compatible/assets/index.css';
import { Card, Col, Row, Typography } from 'antd';
import { Redirect, withRouter } from 'react-router-dom';
import DashboardSource from '../DashboardSource';
import { frameworks } from '../ChartContainer';
import { chartLibraries } from '../ChartRenderer';
import Button from '../components/Button';
import { ReactComponent as PlusSVG } from './plus.svg';
import CreateOwnModal from './CreateOwnModal';

const MarginFrame = ({ children }) => (
  <div style={{ marginTop: 50, margin: 25 }}>
    { children }
  </div>
);

const Image = styled.div`
  position: relative;
  width: 100%;
  height: 100%;
  margin: auto;
  max-width: 1024px;
  background-size: cover;
  background-repeat: no-repeat;
  background-position: center;
  background-image: ${props => `url("${props.src}")`}
`;

const RecipeCard = styled(Card)`
  border: 1px solid #ECECF0;

  border-radius: 4px;
  margin-bottom: 20px;
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
    background: ${props => props.createYourOwn ? "#F8F8FB" : "#EEEEF5"};
    display: flex;
    align-items: center;
    position: relative;

    &:after {
      content: '';
      position: absolute;
      width: 100%; height:100%;
      top:0;
      left:0;
      background: rgba(81, 87, 125, 0.3);
      opacity: 0;
      transition: all 0.25s;
    }

  }

  &&.ant-card-hoverable:hover {
    box-shadow: 0px 15px 20px rgba(67, 67, 107, 0.1);
    button { display: block; }
    svg path {
      stroke: #7A77FF;
    }
    &:hover .ant-card-cover:after {
      opacity: ${props => props.createYourOwn ? "0" : "1"};
    }
  }

  && .ant-card-body {
    min-height: 175px;
    display: flex;
    align-items: center;
    justify-content: center;
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
    font-size: 13px;
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
    await this.dashboardSource.load(true);
    this.setState({
      loadError: this.dashboardSource.loadError
    });
  }

  render() {
    const {
      loadError
    } = this.state;
    if (loadError && loadError.indexOf('Dashboard app not found') === -1) {
      return <Redirect to="/dashboard" />;
    }

    const {
      chartLibrary, framework, templatePackageName, createOwnModalVisible, enableWebSocketTransport
    } = this.state;
    const { history } = this.props;
    const currentLibraryItem = chartLibraries.find(m => m.value === chartLibrary);
    const frameworkItem = frameworks.find(m => m.id === framework);
    const templatePackage = this.dashboardSource && this.dashboardSource.templatePackages
      .find(m => m.name === templatePackageName);

    const recipes = [{
      name: 'Dynamic Dashboard with React, AntD, and Recharts',
      description: 'Use this template to create a dynamic dashboard application with React, AntD, and Chart.js. It comes with a dynamic query builder and Apollo GraphQL client. Use it when you want to allow users to edit dashboards.',
      coverUrl: "https://cube.dev/downloads/template-react-dashboard.png",
      templatePackages: ['create-react-app', 'react-antd-dynamic', 'recharts-charts', 'antd-tables', 'credentials']
    }, {
      name: 'Real-Time Dashboard with React, AntD, and Chart.js',
      description: 'Use this template to create a static dashboard application with real-time WebSocket transport. Use it when users should not be allowed to edit dashboards and you want to provide them with real-time data refresh.',
      templatePackages: ['create-react-app', 'react-antd-static', 'chartjs-charts', 'antd-tables', 'credentials', 'web-socket-transport'],
      coverUrl: "https://cube.dev/downloads/template-real-time-dashboard.png"
    }, {
      name: 'Material UI React Dashboard',
      coverUrl: 'https://cube.dev/downloads/template-material-ui.jpg',
      description: 'Use this template to create a Material UI–based static dashboard application and add charts to it by editing the source code or via Cube.js Playground. Use it when users should not be allowed to edit dashboards.',
      templatePackages: ['create-react-app', 'react-material-static', 'recharts-charts', 'material-tables', 'credentials']
    }, {
      name: 'Material UI D3 Dashboard',
      coverUrl: 'https://cube.dev/downloads/template-material-d3.png',
      description: 'Use this template to create a Material UI–based dashboard with D3.js. Add charts to it by editing the source code or via Cube.js Playground. Use it when users should not be allowed to edit dashboards.',
      templatePackages: ['create-react-app', 'react-material-static', 'd3-charts', 'material-tables', 'credentials']
    }];


    const recipeCards = recipes.map(({ name, description, templatePackages, coverUrl }) => (
      <Col xs={{ span: 24 }} md={{span: 12 }} lg={{ span: 8 }} xl={{ span: 6 }} key={name}>
        <RecipeCard
          hoverable
          bordered={false}
          cover={<Image src={coverUrl} />}
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
      <Col xs={{ span: 24 }} md={{ span: 8 }} lg={{ span: 6 }} key="own">
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
