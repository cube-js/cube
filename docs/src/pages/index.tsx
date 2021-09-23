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

        <InfoBox >

        The Cube multitenancy workshop is on October 27th at 9-10:30 am PST! If you want to learn why you need multitenancy, how to get started, recipes and more, you will want to attend this event 😀
        <br />
        You can register for the workshop at <a href="https://cube.dev/events/multitenancy/">the event page</a>.

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
              desc="It is a good place to start, if you are new to Cube.js."
              to="/getting-started"
            />
            <MainTab
              title="Data Schema"
              img={imgForDevs}
              desc="Learn how to build Cube.js Data Schema."
              to="/getting-started-cubejs-schema"
              right
            />
          </Row>
          <Row>
            <MainTab
              title="Cube.js Backend"
              img={imgInsights}
              desc="How to connect to database and deploy Cube.js service."
              to="/connecting-to-the-database"
            />
            <MainTab
              title="Cube.js Frontend"
              img={imgDashboards}
              desc="Frontend libraries tutorials and API reference."
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
