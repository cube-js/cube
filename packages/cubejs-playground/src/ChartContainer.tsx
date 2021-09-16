import { Component, useEffect } from 'react';
import {
  CodeOutlined,
  CodeSandboxOutlined,
  CopyOutlined,
  DownOutlined,
  PlusOutlined,
  QuestionCircleOutlined,
  SyncOutlined,
} from '@ant-design/icons';
import { Dropdown, Menu, Modal } from 'antd';
import { getParameters } from 'codesandbox-import-utils/lib/api/define';
import styled from 'styled-components';
import { Redirect, RouteComponentProps, withRouter } from 'react-router-dom';
import { QueryRenderer } from '@cubejs-client/react';
import { ChartType, Query, ResultSet } from '@cubejs-client/core';
import { format } from 'sql-formatter';

import { SectionRow } from './components';
import { Button, Card, FatalError } from './atoms';
import PrismCode from './PrismCode';
import CachePane from './components/CachePane';
import { playgroundAction } from './events';
import { codeSandboxDefinition, copyToClipboard } from './utils';
import DashboardSource from './DashboardSource';

const frameworkToTemplate = {
  react: 'create-react-app',
  angular: 'angular-cli',
  vue: 'vue-cli',
};

const StyledCard: any = styled(Card)`
  .ant-card-head {
    position: sticky;
    top: 0;
    z-index: 100;
    background: white;
  }

  .ant-card-body {
    max-width: 100%;
    overflow: auto;
    position: relative;
  }
`;

type FrameworkDescriptor = {
  id: string;
  title: string;
  docsLink?: string;
  supported?: boolean;
  scaffoldingSupported?: boolean;
};

export const frameworks: FrameworkDescriptor[] = [
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
    title: 'Vue',
    supported: true,
    scaffoldingSupported: true,
  },
  {
    id: 'vanilla',
    title: 'Vanilla JavaScript',
    docsLink: 'https://cube.dev/docs/@cubejs-client-core',
  },
];

type ChartContainerProps = {
  query: Query;
  hideActions: boolean;
  chartType: ChartType;
  dashboardSource?: DashboardSource;
  error?: Error;
  resultSet?: ResultSet;
  [k: string]: any;
};

type ChartContainerState = {
  sql: {
    loading: boolean;
    value?: string;
  };
  [k: string]: any;
};

class ChartContainer extends Component<
  ChartContainerProps & RouteComponentProps,
  ChartContainerState
> {
  static defaultProps = {
    query: {},
    hideActions: false,
  };

  static getDerivedStateFromProps(props, state) {
    if (
      props.isChartRendererReady &&
      props.iframeRef.current != null &&
      props.chartingLibrary
    ) {
      const { __cubejsPlayground } = props.iframeRef.current.contentWindow;

      if (!__cubejsPlayground) {
        return {
          ...state,
          chartRendererError: 'The chart renderer failed to load',
        };
      }

      const codesandboxFiles = __cubejsPlayground.getCodesandboxFiles(
        props.chartingLibrary,
        {
          chartType: props.chartType,
          query: JSON.stringify(props.query, null, 2),
          pivotConfig: JSON.stringify(props.pivotConfig, null, 2),
          apiUrl: props.apiUrl,
          cubejsToken: props.cubejsToken,
        }
      );
      let codeExample = '';

      if (props.framework === 'react') {
        codeExample = codesandboxFiles['index.js'];
      } else if (props.framework === 'angular') {
        codeExample =
          codesandboxFiles[
            'src/app/query-renderer/query-renderer.component.ts'
          ];
      } else if (props.framework === 'vue') {
        codeExample = codesandboxFiles['src/components/ChartRenderer.vue'];
      }

      return {
        ...state,
        chartRendererError: null,
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
      chartRendererError: null,
      sql: {
        loading: false,
      },
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
      chartRendererError,
      sql,
    } = this.state;
    const {
      isChartRendererReady,
      resultSet,
      error,
      render,
      dashboardSource,
      hideActions,
      query,
      chartingLibrary,
      setChartLibrary,
      chartLibraries,
      history,
      framework,
      setFramework,
      isFetchingMeta,
      onChartRendererReadyChange,
    } = this.props;

    if (redirectToDashboard) {
      return <Redirect to="/dashboard" />;
    }

    if (chartRendererError) {
      return <div>{chartRendererError}</div>;
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
          data-testid="charting-library-dropdown"
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
        data-testid="framework-dropdown"
        onClick={(e) => {
          if (e.key === framework) {
            return;
          }

          playgroundAction('Set Framework', { framework: e.key });
          setFramework(e.key);
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
            <Dropdown overlay={frameworkMenu} disabled={isFetchingMeta}>
              <Button data-testid="framework-btn" size="small">
                {frameworkItem?.title}
                <DownOutlined />
              </Button>
            </Dropdown>

            {chartLibrariesMenu ? (
              <Dropdown
                overlay={chartLibrariesMenu}
                disabled={!frameworkItem?.supported || isFetchingMeta}
              >
                <Button data-testid="charting-library-btn" size="small">
                  {currentLibraryItem?.title}
                  <DownOutlined />
                </Button>
              </Dropdown>
            ) : null}
          </Button.Group>

          <Button.Group>
            <Button
              data-testid="chart-btn"
              size="small"
              type={!showCode ? 'primary' : 'default'}
              disabled={!frameworkItem?.supported || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show Chart');
                this.setState({
                  showCode: null,
                });
              }}
            >
              Chart
            </Button>

            <Button
              data-testid="json-query-btn"
              size="small"
              type={showCode === 'query' ? 'primary' : 'default'}
              disabled={!frameworkItem?.supported || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show Query');
                this.setState({
                  showCode: 'query',
                });
              }}
            >
              JSON Query
            </Button>

            <Button
              data-testid="code-btn"
              icon={<CodeOutlined />}
              size="small"
              type={showCode === 'code' ? 'primary' : 'default'}
              disabled={!frameworkItem?.supported || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show Code');
                this.setState({ showCode: 'code' });
              }}
            >
              Code
            </Button>

            <Button
              data-testid="sql-btn"
              icon={<QuestionCircleOutlined />}
              size="small"
              type={showCode === 'sql' ? 'primary' : 'default'}
              disabled={!frameworkItem?.supported || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show SQL');
                this.setState({ showCode: 'sql' });
              }}
            >
              SQL
            </Button>

            <Button
              data-testid="cache-btn"
              icon={<SyncOutlined />}
              size="small"
              type={showCode === 'cache' ? 'primary' : 'default'}
              disabled={!frameworkItem?.supported || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show Cache');
                this.setState({
                  showCode: 'cache',
                });
              }}
            >
              Cache
            </Button>
          </Button.Group>

          <Button
            data-testid="edit-btn"
            icon={<CodeSandboxOutlined />}
            size="small"
            htmlType="submit"
            disabled={!frameworkItem?.supported || isFetchingMeta}
            onClick={() => playgroundAction('Open Code Sandbox')}
          >
            Edit
          </Button>

          {dashboardSource && (
            <Button
              data-testid="add-to-dashboard-btn"
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
              disabled={!frameworkItem?.supported || isFetchingMeta}
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
            {frameworkItem?.title}
            &nbsp;code generation here yet.
            <br />
            Please refer to&nbsp;
            <a
              href={frameworkItem?.docsLink}
              target="_blank"
              rel="noopener noreferrer"
              onClick={() =>
                playgroundAction('Unsupported Framework Docs', { framework })
              }
            >
              {frameworkItem?.title}
              &nbsp;docs
            </a>
            &nbsp;to see on how to use it with Cube.js.
          </h2>
        );
      } else if (showCode === 'code') {
        if (error) {
          return <FatalError error={error} />;
        }

        return <PrismCode code={codeExample} />;
      } else if (showCode === 'query') {
        return <PrismCode code={queryText} />;
      } else if (showCode === 'sql') {
        return (
          <QueryRenderer
            loadSql="only"
            query={query}
            render={({ sqlQuery, loadingState, error }) => {
              if (error) {
                return <FatalError error={error} />;
              }

              // in the case of a compareDateRange query the SQL will be the same
              const [query] = Array.isArray(sqlQuery) ? sqlQuery : [sqlQuery];
              const value = query && format(query.sql());

              return (
                <>
                  <PrismCode code={value} />
                  <SqlEmitter
                    loading={loadingState.isLoading}
                    sql={value}
                    onChange={({ sql, loading }) => {
                      this.setState({
                        sql: {
                          loading,
                          value: sql,
                        },
                      });
                    }}
                  />
                </>
              );
            }}
          />
        );
      } else if (showCode === 'cache') {
        return <CachePane query={query} />;
      }
      return render({ framework, error });
    };

    let title;

    if (showCode === 'code') {
      title = (
        <SectionRow style={{ alignItems: 'center' }}>
          <div>Code</div>

          <Button
            data-testid="copy-code-btn"
            icon={<CopyOutlined />}
            size="small"
            disabled={Boolean(error)}
            onClick={async () => {
              await copyToClipboard(codeExample);
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
            data-testid="copy-cube-query-btn"
            icon={<CopyOutlined />}
            size="small"
            onClick={async () => {
              await copyToClipboard(JSON.stringify(query, null, 2));
              playgroundAction('Copy Query to Clipboard');
            }}
            type="primary"
          >
            Copy to Clipboard
          </Button>
        </SectionRow>
      );
    } else if (showCode === 'sql') {
      title = (
        <SectionRow>
          <div>SQL</div>

          {!sql.loading && sql.value ? (
            <Button
              data-testid="copy-sql-btn"
              icon={<CopyOutlined />}
              size="small"
              onClick={async () => {
                await copyToClipboard(sql.value, 'The SQL has been copied');
                playgroundAction('Copy SQL to Clipboard');
              }}
              type="primary"
            >
              Copy to Clipboard
            </Button>
          ) : null}
        </SectionRow>
      );
    } else if (showCode === 'cache') {
      title = 'Cache';
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

type SqlEmitterOnChangeProps = {
  sql?: string;
  loading: boolean;
};

type SqlEmitterProps = {
  loading: boolean;
  sql?: string;
  onChange: (props: SqlEmitterOnChangeProps) => void;
};

function SqlEmitter({ sql, loading, onChange }: SqlEmitterProps) {
  useEffect(() => {
    onChange({ sql, loading });
  }, [sql, loading]);

  return null;
}

export default withRouter(ChartContainer);
