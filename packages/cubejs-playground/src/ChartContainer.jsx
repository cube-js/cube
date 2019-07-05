import React from 'react';
import {
  Card, Button, Menu, Dropdown, Icon
} from 'antd';
import { getParameters } from 'codesandbox-import-utils/lib/api/define';
import { fetch } from 'whatwg-fetch';
import { map } from 'ramda';
import { Redirect } from 'react-router-dom';
import { QueryRenderer } from '@cubejs-client/react';
import sqlFormatter from "sql-formatter";
import PropTypes from 'prop-types';
import PrismCode from './PrismCode';
import { playgroundAction } from './events';

const frameworks = [{
  id: 'vanilla',
  title: 'Vanilla JavaScript',
  docsLink: 'https://cube.dev/docs/@cubejs-client-core'
}, {
  id: 'angular',
  title: 'Angular',
  docsLink: 'https://cube.dev/docs/@cubejs-client-ngx'
}, {
  id: 'react',
  title: 'React'
}, {
  id: 'vue',
  title: 'Vue.js',
  docsLink: 'https://cube.dev/docs/@cubejs-client-vue'
}];

class ChartContainer extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      showCode: false,
      framework: 'react'
    };
  }

  async componentDidMount() {
    const {
      codeSandboxSource,
      dependencies
    } = this.props;
    const codeSandboxRes = await fetch("https://codesandbox.io/api/v1/sandboxes/define?json=1", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json"
      },
      body: JSON.stringify(this.codeSandboxDefinition(codeSandboxSource, dependencies))
    });
    const codeSandboxJson = await codeSandboxRes.json();
    this.setState({ sandboxId: codeSandboxJson.sandbox_id });
  }

  codeSandboxDefinition(codeSandboxSource, dependencies) {
    return {
      files: {
        ...(typeof codeSandboxSource === 'string' ? {
          'index.js': {
            content: codeSandboxSource,
          }
        } : codeSandboxSource),
        'package.json': {
          content: {
            dependencies: {
              'react-dom': 'latest',
              ...map(() => 'latest', dependencies)
            }
          },
        },
      },
      template: 'create-react-app'
    };
  }

  render() {
    const {
      redirectToDashboard, showCode, sandboxId, addingToDashboard, framework
    } = this.state;
    const {
      resultSet,
      error,
      codeExample,
      render,
      title,
      codeSandboxSource,
      dependencies,
      dashboardSource,
      hideActions,
      query,
      cubejsApi,
      chartLibrary,
      setChartLibrary,
      chartLibraries
    } = this.props;

    if (redirectToDashboard) {
      return <Redirect to="/dashboard" />;
    }

    const parameters = getParameters(this.codeSandboxDefinition(codeSandboxSource, dependencies));

    console.log(chartLibraries);

    const chartLibrariesMenu = (
      <Menu onClick={(e) => setChartLibrary(e.key)}>
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
      <Menu onClick={(e) => this.setState({ framework: e.key })}>
        {
          frameworks.map(f => (
            <Menu.Item key={f.id}>
              {f.title}
            </Menu.Item>
          ))
        }
      </Menu>
    );

    const currentLibraryItem = chartLibraries.find(m => m.value === chartLibrary);
    const frameworkItem = frameworks.find(m => m.id === framework);
    const extra = (
      <form action="https://codesandbox.io/api/v1/sandboxes/define" method="POST" target="_blank">
        <input type="hidden" name="parameters" value={parameters} />
        <Button.Group>
          {dashboardSource && (
            <Button
              onClick={async () => {
                playgroundAction('Add to Dashboard');
                this.setState({ addingToDashboard: true });
                await dashboardSource.addChart(codeExample);
                this.setState({ redirectToDashboard: true, addingToDashboard: false });
              }}
              icon="plus"
              size="small"
              loading={addingToDashboard}
            >
              {addingToDashboard ? 'Creating app and installing modules...' : 'Add to Dashboard'}
            </Button>
          )}
          <Dropdown overlay={frameworkMenu}>
            <Button size="small">
              {frameworkItem && frameworkItem.title}
              <Icon type="down" />
            </Button>
          </Dropdown>
          <Dropdown overlay={chartLibrariesMenu}>
            <Button size="small">
              {currentLibraryItem && currentLibraryItem.title}
              <Icon type="down" />
            </Button>
          </Dropdown>
          <Button
            onClick={() => {
              playgroundAction('Show Code');
              this.setState({ showCode: showCode === 'code' ? null : 'code' });
            }}
            icon="code"
            size="small"
            type={showCode === 'code' ? 'primary' : 'default'}
          >
            Code
          </Button>
          <Button
            onClick={() => {
              playgroundAction('Show SQL');
              this.setState({ showCode: showCode === 'sql' ? null : 'sql' });
            }}
            icon="question-circle"
            size="small"
            type={showCode === 'sql' ? 'primary' : 'default'}
          >
            SQL
          </Button>
          <Button
            icon="code-sandbox"
            size="small"
            onClick={() => playgroundAction('Open Code Sandbox')}
            htmlType="submit"
          >
            Edit
          </Button>
        </Button.Group>
      </form>
    );

    const renderChart = () => {
      if (frameworkItem && frameworkItem.docsLink) {
        return (
          <h2 style={{ padding: 24, textAlign: 'center' }}>
            We do not support&nbsp;
            {frameworkItem.title}
            &nbsp;code generation here yet.
            < br/>
            Please refer to&nbsp;
            <a href={frameworkItem.docsLink} target="_blank">
              {frameworkItem.title}
              &nbsp;docs
            </a>
            &nbsp;to see on how to use it with Cube.js.
          </h2>
        );
      } else if (showCode === 'code') {
        return <PrismCode code={codeExample} />;
      } else if (showCode === 'sql') {
        return (
          <QueryRenderer
            loadSql="only"
            query={query}
            cubejsApi={cubejsApi}
            render={({ sqlQuery }) => <PrismCode code={sqlQuery && sqlFormatter.format(sqlQuery.sql())} />}
          />
        );
      }
      return render({ resultSet, error, sandboxId });
    };

    return hideActions ? render({ resultSet, error, sandboxId }) : (
      <Card
        title={title}
        style={{ minHeight: 420 }}
        extra={extra}
      >
        {renderChart()}
      </Card>
    );
  }
}

ChartContainer.propTypes = {
  resultSet: PropTypes.object,
  error: PropTypes.object,
  codeExample: PropTypes.string,
  render: PropTypes.func.isRequired,
  title: PropTypes.string,
  codeSandboxSource: PropTypes.string,
  dependencies: PropTypes.array.isRequired,
  dashboardSource: PropTypes.string,
  hideActions: PropTypes.array,
  query: PropTypes.object,
  cubejsApi: PropTypes.object,
  chartLibrary: PropTypes.string.isRequired,
  setChartLibrary: PropTypes.func.isRequired,
  chartLibraries: PropTypes.array.isRequired
};

ChartContainer.defaultProps = {
  query: {},
  cubejsApi: null,
  hideActions: null,
  dashboardSource: null,
  codeSandboxSource: null,
  title: null,
  codeExample: null,
  error: null,
  resultSet: null
};

export default ChartContainer;
