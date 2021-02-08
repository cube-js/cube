import { Component } from 'react';
import styled from 'styled-components';
import '@ant-design/compatible/assets/index.css';
import { Col, Row, Spin, Typography } from 'antd';
import { Redirect, withRouter } from 'react-router-dom';

import DashboardSource from '../../DashboardSource';
import { frameworks } from '../../ChartContainer';
import { Button, Card } from '../../components';
import { ReactComponent as PlusSVG } from './plus.svg';
import CreateOwnModal from './CreateOwnModal';
import { frameworkChartLibraries } from '../../PlaygroundQueryBuilder';

const MarginFrame = ({ children }) => (
  <div style={{ margin: 25 }}>{children}</div>
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
  && {
    border: none;
    border-radius: 8px;
    margin-bottom: 24px;
    padding: 16px;
    ${(props) =>
      props.createYourOwn
        ? `
      background: transparent;
      border: 1px solid var(--purple-03-color);
    `
        : ''}

    &:hover {
      padding: 16px;
      ${(props) =>
        props.createYourOwn
          ? `
        border: 1px solid var(--purple-03-color);
      `
          : 'border: none;'}
    }

    button {
      display: none;
      position: absolute;
      margin-left: -64px;
      top: 80px;
      left: 50%;
    }

    svg path {
      transition: stroke 0.25s ease;
    }

    && .ant-card-cover {
      height: 168px;
      border-radius: 8px 8px 0 0;
      background: ${(props) =>
        props.createYourOwn ? 'transparent' : '#DEDEF1'};
      display: flex;
      align-items: center;
      position: relative;
      margin: -16px -16px 0 -16px;
      padding: 24px 24px 0 24px;

      &::after {
        content: '';
        position: absolute;
        width: 100%;
        height: 100%;
        top: 0;
        left: 0;
        background: rgba(81, 87, 125, 0.3);
        opacity: 0;
        transition: all 0.25s;
        border-radius: 8px 8px 0 0;
      }

      div {
        //box-shadow: 0 -1px 6px rgba(20, 20, 70, .06);
        background-position: top;
        border-radius: 4px 4px 0 0;
      }
    }

    &&.ant-card-hoverable:hover {
      box-shadow: 0px 15px 20px rgba(67, 67, 107, 0.1);
      button {
        display: block;
      }
      &:hover .ant-card-cover:after {
        opacity: ${(props) => (props.createYourOwn ? '0' : '1')};
      }
    }

    svg path {
      stroke: var(--primary-color);
    }

    && .ant-card-body {
      min-height: 175px;
      display: flex;
      place-items: stretch;
      place-content: start stretch;
      text-align: left;
      z-index: 1;
      padding: 24px 0 16px;
    }

    && .ant-card-meta {
      text-align: left;
    }

    && .ant-card-meta-title {
      white-space: unset;
      color: ${(props) =>
        props.createYourOwn ? 'var(--primary-color)' : 'var(--text-color)'};
      text-align: ${(props) => (props.createYourOwn ? 'center' : 'left')};
      margin-bottom: 16px;
    }

    && .ant-card-meta-description {
      color: ${(props) =>
        props.createYourOwn ? 'var(--primary-color)' : 'var(--dark-04-color)'};
      opacity: ${(props) => (props.createYourOwn ? '0.8' : 1)};
      font-size: 13px;
      text-align: ${(props) => (props.createYourOwn ? 'center' : 'left')};
      ${(props) => (props.createYourOwn ? 'padding: 0 32px;' : '')}
    }
  }
`;

const StyledTitle = styled(Typography.Text)`
  display: block;
  font-size: 16px;
  margin-bottom: 24px;
`;

class TemplateGalleryPage extends Component {
  constructor(props) {
    super(props);
    this.state = {
      chartLibrary: frameworkChartLibraries.react[0].value,
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
    const currentLibraryItem = frameworkChartLibraries[framework].find(
      (m) => m.value === chartLibrary
    );
    const frameworkItem = frameworks.find((m) => m.id === framework);
    const templatePackage = this.dashboardSource
      ?.templatePackages(framework)
      .find((m) => m.name === templatePackageName);

    const recipeCards = templates
      .map(({ name, description, coverUrl }) => (
        <Col
          xs={{ span: 24 }}
          md={{ span: 12 }}
          lg={{ span: 8 }}
          xl={{ span: 6 }}
          style={{ display: 'flex' }}
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
        <Col
          xs={{ span: 24 }}
          md={{ span: 12 }}
          lg={{ span: 8 }}
          xl={{ span: 6 }}
          style={{ display: 'flex' }}
          key="own"
        >
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
              let templatePackages = [];
              this.setState({ createOwnModalVisible: false });

              if (framework.toLowerCase() === 'react') {
                templatePackages = [
                  'create-react-app',
                  templatePackageName,
                  `${chartLibrary}-charts`,
                  `${templatePackageName.match(/^react-(\w+)/)[1]}-tables`, // TODO
                  'react-credentials',
                ].concat(
                  enableWebSocketTransport ? ['react-web-socket-transport'] : []
                );
              } else {
                templatePackages = [
                  'create-ng-app',
                  templatePackageName,
                  `ng2-charts`,
                  'ng-credentials',
                ];
              }

              await this.dashboardSource.applyTemplatePackages(
                templatePackages
              );
              history.push('/dashboard');
            }}
            onCancel={() => this.setState({ createOwnModalVisible: false })}
            onChange={(key, value) => {
              if (key === 'framework' && framework !== value) {
                this.setState({
                  templatePackageName: 'ng-material-dynamic',
                  chartLibrary:
                    frameworkChartLibraries[value.toLowerCase()][0].value,
                });
              }
              this.setState({ [key]: value });
            }}
            chartLibraries={frameworkChartLibraries[framework]}
            currentLibraryItem={currentLibraryItem}
            frameworks={frameworks}
            framework={framework}
            frameworkItem={frameworkItem}
            templatePackages={
              this.dashboardSource &&
              this.dashboardSource.templatePackages(framework)
            }
            templatePackage={templatePackage}
            enableWebSocketTransport={
              enableWebSocketTransport && framework.toLowerCase() !== 'angular'
            }
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
        <Row gutter={24}>{recipeCards}</Row>
      </MarginFrame>
    );
  }
}

export default withRouter(TemplateGalleryPage);
