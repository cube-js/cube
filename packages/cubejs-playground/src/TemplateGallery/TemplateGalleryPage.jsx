import React, { Component } from 'react';
import styled from 'styled-components';
import '@ant-design/compatible/assets/index.css';
import { Card, Col, Row, Typography, Spin } from 'antd';
import { Redirect, withRouter } from 'react-router-dom';
import DashboardSource from '../DashboardSource';
import { frameworks } from '../ChartContainer';
import { chartLibraries } from '../ChartRenderer';
import Button from '../components/Button';
import { ReactComponent as PlusSVG } from './plus.svg';
import CreateOwnModal from './CreateOwnModal';

const MarginFrame = ({ children }) => (
  <div style={{ marginTop: 50, margin: 25 }}>{children}</div>
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
  background-image: ${(props) => `url("${props.src}")`};
`;

const RecipeCard = styled(Card)`
  border: 1px solid #ececf0;

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
    background: ${(props) => (props.createYourOwn ? '#F8F8FB' : '#EEEEF5')};
    display: flex;
    align-items: center;
    position: relative;

    &:after {
      content: '';
      position: absolute;
      width: 100%;
      height: 100%;
      top: 0;
      left: 0;
      background: rgba(81, 87, 125, 0.3);
      opacity: 0;
      transition: all 0.25s;
    }
  }

  &&.ant-card-hoverable:hover {
    box-shadow: 0px 15px 20px rgba(67, 67, 107, 0.1);
    button {
      display: block;
    }
    svg path {
      stroke: #7a77ff;
    }
    &:hover .ant-card-cover:after {
      opacity: ${(props) => (props.createYourOwn ? '0' : '1')};
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
    color: #43436b;
  }

  && .ant-card-meta-description {
    color: #a1a1b5;
    font-size: 13px;
  }
`;

const StyledTitle = styled(Typography.Text)`
  display: block;
  font-size: 16px;
  margin-bottom: 25px;
  margin-left: 15px;
`;

class TemplateGalleryPage extends Component {
  constructor(props) {
    super(props);
    this.state = {
      chartLibrary: chartLibraries[0].value,
      framework: 'react',
      templatePackageName: 'react-antd-dynamic',
      templates: null,
    };
  }

  async componentDidMount() {
    this.dashboardSource = new DashboardSource();
    await this.dashboardSource.load(true);

    this.setState({
      loadError: this.dashboardSource.loadError,
      templates: await this.dashboardSource.templates(),
    });
  }

  render() {
    const { loadError, templates } = this.state;

    if (loadError && loadError.indexOf('Dashboard app not found') === -1) {
      return <Redirect to="/dashboard" />;
    }

    if (templates == null) {
      return <Spin />;
    }

    const {
      chartLibrary,
      framework,
      templatePackageName,
      createOwnModalVisible,
      enableWebSocketTransport,
    } = this.state;
    const { history } = this.props;
    const currentLibraryItem = chartLibraries.find(
      (m) => m.value === chartLibrary
    );
    const frameworkItem = frameworks.find((m) => m.id === framework);
    const templatePackage =
      this.dashboardSource &&
      this.dashboardSource.templatePackages.find(
        (m) => m.name === templatePackageName
      );

    const recipeCards = templates
      .map(({ name, description, templatePackages, coverUrl }) => (
        <Col
          xs={{ span: 24 }}
          md={{ span: 12 }}
          lg={{ span: 8 }}
          xl={{ span: 6 }}
          key={name}
        >
          <RecipeCard
            hoverable
            bordered={false}
            cover={<Image src={coverUrl} />}
          >
            <Card.Meta title={name} description={description} />
            <Button
              type="primary"
              onClick={async () => {
                await this.dashboardSource.applyTemplatePackages(name);
                history.push('/dashboard');
              }}
            >
              Create App
            </Button>
          </RecipeCard>
        </Col>
      ))
      .concat([
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
                'react-credentials',
              ].concat(
                enableWebSocketTransport ? ['react-web-socket-transport'] : []
              );
              await this.dashboardSource.applyTemplatePackages(
                templatePackages
              );
              history.push('/dashboard');
            }}
            onCancel={() => this.setState({ createOwnModalVisible: false })}
            onChange={(key, value) => this.setState({ [key]: value })}
            chartLibraries={chartLibraries}
            currentLibraryItem={currentLibraryItem}
            frameworks={frameworks}
            framework={framework}
            frameworkItem={frameworkItem}
            templatePackages={
              this.dashboardSource && this.dashboardSource.templatePackages
            }
            templatePackage={templatePackage}
            enableWebSocketTransport={enableWebSocketTransport}
          />
        </Col>,
      ]);

    return (
      <MarginFrame>
        <StyledTitle>
          Build your app from one the popular templates below or{' '}
          <a onClick={() => this.setState({ createOwnModalVisible: true })}>
            create your own
          </a>
        </StyledTitle>
        <Row align="top" gutter={24}>
          {recipeCards}
        </Row>
      </MarginFrame>
    );
  }
}

export default withRouter(TemplateGalleryPage);
