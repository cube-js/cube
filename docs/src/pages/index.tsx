import React, { Component } from 'react';
import Helmet from 'react-helmet';
import { Row, Col } from 'antd';

import { InfoBox } from '../components/AlertBox/AlertBox';
import MainTab from '../components/templates/MainTab';

import imgGettingStarted from './images/getting-started.svg';
import imgForDevs from './images/for-devs.svg';
import imgInsights from './images/insights.svg';
import imgDashboards from './images/dashboards.svg';

import * as styles from '../../static/styles/index.module.scss';
import { Page, Scopes, SetScrollSectionsAndGithubUrlFunction } from '../types';

type Props = {
  changePage(page: Page): void;
  setScrollSectionsAndGithubUrl: SetScrollSectionsAndGithubUrlFunction;
};

class IndexPage extends Component<Props> {
  componentWillMount() {
    this.props.changePage({ scope: Scopes.DEFAULT, category: 'Index' });
    this.props.setScrollSectionsAndGithubUrl([], '');
  }

  render() {
    return (
      <div className={styles.docContent}>
        <Helmet title="Main | Cube.js Docs" />
        <h1>Documentation</h1>

<InfoBox>

        The Cube data modeling workshop is on January 19 at 9-10:30 am PST! If you want to learn how to get started with data schema, key data modeling concepts, advanced use cases in Cube, you will want to attend this event 😀
        <br />
        You can register for the workshop at <a href="https://cube.dev/events/data-modeling/">the event page</a>.

        </InfoBox>
        
        <Row>
          <Col span={24}>
            <p>
              Read about major concepts, dive into technical details or follow
              practical examples to learn how Cube.js works.
            </p>
          </Col>
        </Row>
        <div className={styles.mainTabs}>
          <Row>
            <MainTab
              title="Getting Started"
              img={imgGettingStarted}
              desc="Start here if you're new to Cube"
              to="/getting-started"
            />
            <MainTab
              title="Connecting to the Database"
              img={imgInsights}
              desc="How to connect to your database"
              to="/config/databases"
              right
            />
          </Row>
          <Row>
            <MainTab
              title="Data Schema"
              img={imgForDevs}
              desc="Learn how to build Data Schemas for"
              to="/schema/getting-started"
            />
            <MainTab
              title="Caching"
              img={imgDashboards}
              desc="Find out how to get the best performance from Cube"
              to="/caching"
              right
            />
          </Row>
          <Row>
            <MainTab
              title="Deployment"
              img={imgInsights}
              desc="How to deploy your application"
              to="/deployment"
            />
            <MainTab
              title="Frontend"
              img={imgDashboards}
              desc="Frontend libraries tutorials and API reference"
              to="/frontend-introduction"
              right
            />
          </Row>
        </div>
      </div>
    );
  }
}

export default IndexPage;
