/* global navigator */
import React from 'react';
import {
  CodeOutlined,
  CodeSandboxOutlined,
  CopyOutlined,
  DownOutlined,
  PlusOutlined,
  QuestionCircleOutlined,
  SyncOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { Dropdown, Menu, Modal, notification } from 'antd';
import { Button, Card, SectionRow } from './components';
import { getParameters } from 'codesandbox-import-utils/lib/api/define';
import styled from 'styled-components';
import { Redirect, withRouter } from 'react-router-dom';
import { QueryRenderer } from '@cubejs-client/react';
import sqlFormatter from 'sql-formatter';
import PropTypes from 'prop-types';
import PrismCode from './PrismCode';
import CachePane from './components/CachePane';
import { playgroundAction } from './events';
import { codeSandboxDefinition } from './utils';

const frameworkToTemplate = {
  react: 'create-react-app',
  angular: 'angular-cli',
  vue: 'vue-cli',
};

const StyledCard = styled(Card)`
  .ant-card-head {
    position: sticky;
    top: 0;
    z-index: 100;
    background: white;
  }

  .ant-card-body {
    max-width: 100%;
    overflow: auto;
  }
`;

export const frameworks = [
  {
    id: 'vanilla',
    title: 'Vanilla JavaScript',
    docsLink: 'https://cube.dev/docs/@cubejs-client-core',
  },
  {
    id: 'react',
    title: 'React',
    supported: true,
    scaffoldingSupported: true,
  },
  {
    id: 'angular',
    title: 'Angular',
    supported: true,
    scaffoldingSupported: true,
  },
  {
    id: 'vue',
    title: 'Vue.js',
    docsLink: 'https://cube.dev/docs/@cubejs-client-vue',
  },
];

class ChartContainer extends React.Component {
  static getDerivedStateFromProps(props, state) {
    if (
      props.isChartRendererReady &&
      props.iframeRef.current != null &&
      props.chartingLibrary
    ) {
      const { __cubejsPlayground } = props.iframeRef.current.contentWindow;

      const codesandboxFiles = __cubejsPlayground.getCodesandboxFiles(
        props.chartingLibrary,
        {
          chartType: props.chartType,
          query: JSON.stringify(props.query, null, 2),
          pivotConfig: JSON.stringify(props.pivotConfig, null, 2),
          apiUrl: `${props.apiUrl}/cubejs-api/v1`,
          cubejsToken: props.cubejsToken
        }
      );
      let codeExample = '';
      
      if (state.framework === 'react') {
        codeExample = codesandboxFiles['index.js'];
      } else if (state.framework === 'angular') {
        codeExample = codesandboxFiles['src/app/query-renderer/query-renderer.component.ts'];
      }

      return {
        ...state,
        dependencies: __cubejsPlayground.getDependencies(props.chartingLibrary),
        codeExample,
        codesandboxFiles,
      };
    }
    return state;
  }

  constructor(props) {
    super(props);
    this.state = {
      showCode: false,
      framework: 'react',
    };
  }

  render() {
    const {
      codeExample,
      codesandboxFiles,
      dependencies,
      redirectToDashboard,
      showCode,
      addingToDashboard,
      framework,
    } = this.state;
    const {
      isChartRendererReady,
      resultSet,
      error,
      render,
      dashboardSource,
      hideActions,
      query,
      cubejsApi,
      chartingLibrary,
      setChartLibrary,
      chartLibraries,
      history,
      onChartRendererReadyChange,
    } = this.props;

    if (redirectToDashboard) {
      return <Redirect to="/dashboard" />;
    }

    const parameters = isChartRendererReady
      ? getParameters(
          codeSandboxDefinition(
            frameworkToTemplate[framework],
            codesandboxFiles,
            dependencies
          )
        )
      : null;

    const chartLibrariesMenu =
      (chartLibraries[framework] || []).length > 0 ? (
        <Menu
          onClick={(e) => {
            playgroundAction('Set Chart Library', { chartingLibrary: e.key });
            setChartLibrary(e.key);
          }}
        >
          {(chartLibraries[framework] || []).map((library) => (
            <Menu.Item key={library.value}>{library.title}</Menu.Item>
          ))}
        </Menu>
      ) : null;

    const frameworkMenu = (
      <Menu
        onClick={(e) => {
          playgroundAction('Set Framework', { framework: e.key });
          this.setState({ framework: e.key });
          onChartRendererReadyChange(false);
          setChartLibrary(chartLibraries[e.key]?.[0]?.value || null);
        }}
      >
        {frameworks.map((f) => (
          <Menu.Item key={f.id}>{f.title}</Menu.Item>
        ))}
      </Menu>
    );

    const currentLibraryItem = (chartLibraries[framework] || []).find(
      (m) => m.value === chartingLibrary
    );

    const frameworkItem = frameworks.find((m) => m.id === framework);
    const extra = (
      <form
        action="https://codesandbox.io/api/v1/sandboxes/define"
        method="POST"
        target="_blank"
      >
        {parameters != null ? (
          <input type="hidden" name="parameters" value={parameters} />
        ) : null}
        <SectionRow>
          <Button.Group>
            <Dropdown overlay={frameworkMenu}>
              <Button size="small">
                {frameworkItem?.title}
                <DownOutlined />
              </Button>
            </Dropdown>
            {chartLibrariesMenu ? (
              <Dropdown
                overlay={chartLibrariesMenu}
                disabled={!frameworkItem.supported}
              >
                <Button size="small">
                  {currentLibraryItem?.title}
                  <DownOutlined />
                </Button>
              </Dropdown>
            ) : null}
          </Button.Group>
          <Button.Group>
            <Button
              onClick={() => {
                playgroundAction('Show Chart');
                this.setState({
                  showCode: null,
                });
              }}
              size="small"
              type={!showCode ? 'primary' : 'default'}
              disabled={!frameworkItem.supported}
            >
              Chart
            </Button>
            <Button
              onClick={() => {
                playgroundAction('Show Query');
                this.setState({
                  showCode: 'query',
                });
              }}
              icon={<ThunderboltOutlined />}
              size="small"
              type={showCode === 'query' ? 'primary' : 'default'}
              disabled={!frameworkItem.supported}
            >
              JSON Query
            </Button>
            <Button
              onClick={() => {
                playgroundAction('Show Code');
                this.setState({ showCode: 'code' });
              }}
              icon={<CodeOutlined />}
              size="small"
              type={showCode === 'code' ? 'primary' : 'default'}
              disabled={!frameworkItem.supported}
            >
              Code
            </Button>
            <Button
              onClick={() => {
                playgroundAction('Show SQL');
                this.setState({ showCode: 'sql' });
              }}
              icon={<QuestionCircleOutlined />}
              size="small"
              type={showCode === 'sql' ? 'primary' : 'default'}
              disabled={!frameworkItem.supported}
            >
              SQL
            </Button>
            <Button
              onClick={() => {
                playgroundAction('Show Cache');
                this.setState({
                  showCode: 'cache',
                });
              }}
              icon={<SyncOutlined />}
              size="small"
              type={showCode === 'cache' ? 'primary' : 'default'}
              disabled={!frameworkItem.supported}
            >
              Cache
            </Button>
          </Button.Group>
          <Button
            icon={<CodeSandboxOutlined />}
            size="small"
            onClick={() => playgroundAction('Open Code Sandbox')}
            htmlType="submit"
            disabled={!frameworkItem.supported}
          >
            Edit
          </Button>
          {dashboardSource && (
            <Button
              onClick={async () => {
                this.setState({ addingToDashboard: true });
                const canAddChart = await dashboardSource.canAddChart();
                if (typeof canAddChart === 'boolean' && canAddChart) {
                  playgroundAction('Add to Dashboard');
                  await dashboardSource.addChart(codeExample);
                  this.setState({
                    redirectToDashboard: true,
                    addingToDashboard: false,
                  });
                } else if (!canAddChart) {
                  this.setState({ addingToDashboard: false });
                  Modal.error({
                    title:
                      'Your dashboard app does not support adding of static charts',
                    content: 'Please use static dashboard template',
                  });
                } else {
                  this.setState({ addingToDashboard: false });
                  Modal.error({
                    title: 'There is an error loading your dashboard app',
                    content: canAddChart,
                    okText: 'Fix',
                    okCancel: true,
                    onOk() {
                      history.push('/dashboard');
                    },
                  });
                }
              }}
              icon={<PlusOutlined />}
              size="small"
              loading={addingToDashboard}
              disabled={!frameworkItem.supported}
              type="primary"
            >
              {addingToDashboard
                ? 'Preparing dashboard app. It may take a while. Please check console for progress...'
                : 'Add to Dashboard'}
            </Button>
          )}
        </SectionRow>
      </form>
    );

    const queryText = JSON.stringify(query, null, 2);

    const renderChart = () => {
      if (!frameworkItem?.supported) {
        return (
          <h2 style={{ padding: 24, textAlign: 'center' }}>
            We do not support&nbsp;
            {frameworkItem.title}
            &nbsp;code generation here yet.
            <br />
            Please refer to&nbsp;
            <a
              href={frameworkItem.docsLink}
              target="_blank"
              rel="noopener noreferrer"
              onClick={() =>
                playgroundAction('Unsupported Framework Docs', { framework })
              }
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
            render={({ sqlQuery }) => {
              const [query] = Array.isArray(sqlQuery) ? sqlQuery : [sqlQuery];
              // in the case of a compareDateRange query the SQL will be the same
              return (
                <PrismCode code={query && sqlFormatter.format(query.sql())} />
              );
            }}
          />
        );
      } else if (showCode === 'cache') {
        return <CachePane query={query} cubejsApi={cubejsApi} />;
      }
      return render({ framework, error });
    };

    let title;

    const copyCodeToClipboard = async () => {
      if (!navigator.clipboard) {
        notification.error({
          message: "Your browser doesn't support copy to clipboard",
        });
      }
      try {
        await navigator.clipboard.writeText(
          showCode === 'query' ? queryText : codeExample
        );
        notification.success({
          message: 'Copied to clipboard',
        });
      } catch (e) {
        notification.error({
          message: "Can't copy to clipboard",
          description: e,
        });
      }
    };

    if (showCode === 'code') {
      title = (
        <SectionRow>
          <div>Query</div>
          <Button
            icon={<CopyOutlined />}
            size="small"
            onClick={() => {
              copyCodeToClipboard();
              playgroundAction('Copy Code to Clipboard');
            }}
            type="primary"
          >
            Copy to Clipboard
          </Button>
        </SectionRow>
      );
    } else if (showCode === 'query') {
      title = (
        <SectionRow>
          <div>Query</div>
          <Button
            icon={<CopyOutlined />}
            size="small"
            onClick={() => {
              copyCodeToClipboard();
              playgroundAction('Copy Query to Clipboard');
            }}
            type="primary"
          >
            Copy to Clipboard
          </Button>
        </SectionRow>
      );
    } else if (showCode === 'sql') {
      title = 'SQL';
    } else {
      title = 'Chart';
    }

    return hideActions ? (
      render({ resultSet, error })
    ) : (
      <StyledCard title={title} style={{ minHeight: 420 }} extra={extra}>
        {renderChart()}
      </StyledCard>
    );
  }
}

ChartContainer.propTypes = {
  resultSet: PropTypes.object,
  error: PropTypes.object,
  render: PropTypes.func.isRequired,
  codeSandboxSource: PropTypes.string,
  dashboardSource: PropTypes.object,
  hideActions: PropTypes.array,
  query: PropTypes.object,
  cubejsApi: PropTypes.object,
  history: PropTypes.object.isRequired,
  chartingLibrary: PropTypes.string.isRequired,
  setChartLibrary: PropTypes.func.isRequired,
};

ChartContainer.defaultProps = {
  query: {},
  cubejsApi: null,
  hideActions: null,
  dashboardSource: null,
  codeSandboxSource: null,
  error: null,
  resultSet: null,
};

export default withRouter(ChartContainer);
