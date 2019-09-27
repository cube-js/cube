/* global navigator */
import React from 'react';
import {
  Card, Button, Menu, Dropdown, Icon, notification
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

export const frameworks = [{
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

    const chartLibrariesMenu = (
      <Menu
        onClick={(e) => {
          playgroundAction('Set Chart Library', { chartLibrary: e.key });
          setChartLibrary(e.key);
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

    const currentLibraryItem = chartLibraries.find(m => m.value === chartLibrary);
    const frameworkItem = frameworks.find(m => m.id === framework);
    const extra = (
      <form action="https://codesandbox.io/api/v1/sandboxes/define" method="POST" target="_blank">
        <input type="hidden" name="parameters" value={parameters} />
        <Button.Group>
          {/* TODO: implement add to static dashboard */}
          {/*{dashboardSource && (
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
              disabled={!!frameworkItem.docsLink}
            >
              {addingToDashboard ? 'Installing modules. It may take a while. Please check console for progress...' : 'Add to Dashboard'}
            </Button>
          )}*/}
          <Dropdown overlay={frameworkMenu}>
            <Button size="small">
              {frameworkItem && frameworkItem.title}
              <Icon type="down" />
            </Button>
          </Dropdown>
          <Dropdown
            overlay={chartLibrariesMenu}
            disabled={!!frameworkItem.docsLink}
          >
            <Button
              size="small"
            >
              {currentLibraryItem && currentLibraryItem.title}
              <Icon type="down" />
            </Button>
          </Dropdown>
          <Button
            onClick={() => {
              playgroundAction('Show Query');
              this.setState({ showCode: showCode === 'query' ? null : 'query' });
            }}
            icon="thunderbolt"
            size="small"
            type={showCode === 'query' ? 'primary' : 'default'}
            disabled={!!frameworkItem.docsLink}
          >
            JSON Query
          </Button>
          <Button
            onClick={() => {
              playgroundAction('Show Code');
              this.setState({ showCode: showCode === 'code' ? null : 'code' });
            }}
            icon="code"
            size="small"
            type={showCode === 'code' ? 'primary' : 'default'}
            disabled={!!frameworkItem.docsLink}
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
            disabled={!!frameworkItem.docsLink}
          >
            SQL
          </Button>
          <Button
            icon="code-sandbox"
            size="small"
            onClick={() => playgroundAction('Open Code Sandbox')}
            htmlType="submit"
            disabled={!!frameworkItem.docsLink}
          >
            Edit
          </Button>
        </Button.Group>
      </form>
    );

    const queryText = JSON.stringify(query, null, 2);

    const renderChart = () => {
      if (frameworkItem && frameworkItem.docsLink) {
        return (
          <h2 style={{ padding: 24, textAlign: 'center' }}>
            We do not support&nbsp;
            {frameworkItem.title}
            &nbsp;code generation here yet.
            < br/>
            Please refer to&nbsp;
            <a
              href={frameworkItem.docsLink}
              target="_blank"
              rel="noopener noreferrer"
              onClick={() => playgroundAction('Unsupported Framework Docs', { framework })}
            >
              {frameworkItem.title}
              &nbsp;docs
            </a>
            &nbsp;to see on how to use it with Cube.js.
          </h2>
        );
      } else if (showCode === 'code') {
        return <PrismCode code={codeExample} />;
      } else if (showCode === 'query') {
        return <PrismCode code={queryText} />;
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

    let title;

    const copyCodeToClipboard = async () => {
      if (!navigator.clipboard) {
        notification.error({
          message: `Your browser doesn't support copy to clipboard`
        });
      }
      try {
        await navigator.clipboard.writeText(showCode === 'query' ? queryText : codeExample);
        notification.success({
          message: `Copied to clipboard`
        });
      } catch (e) {
        notification.error({
          message: `Can't copy to clipboard`,
          description: e,
        });
      }
    };

    if (showCode === 'code') {
      title = (
        <Button
          icon="copy"
          onClick={() => {
            copyCodeToClipboard();
            playgroundAction('Copy Code to Clipboard');
          }}
          type="primary"
        >
          Copy Code to Clipboard
        </Button>
      );
    } else if (showCode === 'query') {
      title = (
        <Button
          icon="copy"
          onClick={() => {
            copyCodeToClipboard();
            playgroundAction('Copy Query to Clipboard');
          }}
          type="primary"
        >
          Copy Query to Clipboard
        </Button>
      );
    } else if (showCode === 'sql') {
      title = 'SQL';
    } else {
      title = 'Chart';
    }

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
  codeExample: null,
  error: null,
  resultSet: null
};

export default ChartContainer;
