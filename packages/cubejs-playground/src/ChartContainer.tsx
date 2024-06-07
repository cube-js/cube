import {
  CodeOutlined,
  CodeSandboxOutlined,
  CopyOutlined,
  DownOutlined,
  QuestionCircleOutlined,
  ThunderboltOutlined
} from '@ant-design/icons';
import { ChartType, Meta, Query, ResultSet } from '@cubejs-client/core';
import { Dropdown, Menu } from 'antd';
import { getParameters } from 'codesandbox-import-utils/lib/api/define';
import { Component, FunctionComponent, Suspense } from 'react';
import { Redirect, RouteComponentProps, withRouter } from 'react-router-dom';
import styled from 'styled-components';

import PrismCode from './PrismCode';
import { Button, Card, CubeLoader } from './atoms';
import { SectionRow } from './components';
import { FatalError } from './components/Error/FatalError';
import { playgroundAction } from './events';
import { loadable } from './loadable';
import { GraphQLIcon } from './shared/icons/GraphQLIcon';
import { codeSandboxDefinition, copyToClipboard } from './utils';

const GraphiQLSandbox = loadable(
  () => import('./components/GraphQL/GraphiQLSandbox')
);

const SqlQueryTab = loadable(() => import('./components/SqlQueryTab'));

const CachePane = loadable(() => import('./components/CachePane'));

const frameworkToTemplate = {
  react: 'create-react-app',
  angular: 'angular-cli',
  vue: 'vue-cli',
};

const StyledCard: any = styled(Card)`
  min-height: 420px;

  .ant-card-head {
    position: sticky;
    top: 0;
    z-index: 1;
    background: white;
  }

  .ant-card-body {
    max-width: 100%;
    overflow: auto;
    position: relative;
  }
`;

type UnsupportedPlaceholder = FunctionComponent<{ framework: string }>;
type FrameworkDescriptor = {
  id: string;
  title: string;
  docsLink?: string;
  placeholder?: UnsupportedPlaceholder;
  scaffoldingSupported?: boolean;
};

const UnsupportedFrameworkPlaceholder: UnsupportedPlaceholder = ({
  framework,
}) => (
  <h2 style={{ padding: 24, textAlign: 'center' }}>
    We do not support&nbsp; Vanilla JavaScript &nbsp;code generation here yet.
    <br />
    Please refer to&nbsp;
    <a
      href="https://cube.dev/docs/@cubejs-client-core"
      target="_blank"
      rel="noopener noreferrer"
      onClick={() =>
        playgroundAction('Unsupported Framework Docs', { framework })
      }
    >
      Vanilla JavaScript &nbsp;docs
    </a>
    &nbsp;to see on how to use it with Cube.
  </h2>
);

const BIPlaceholder: UnsupportedPlaceholder = () => (
  <h2 style={{ padding: 24, textAlign: 'center' }}>
    You can connect Cube to any Business Intelligence tool through the Cube SQL
    API.
    <br />
    Please refer to&nbsp;
    <a
      href="https://cube.dev/docs/backend/sql"
      target="_blank"
      rel="noopener noreferrer"
      onClick={() => playgroundAction('BI Docs')}
    >
      Cube SQL &nbsp;docs
    </a>
    &nbsp;to learn more.
  </h2>
);

export const frameworks: FrameworkDescriptor[] = [
  {
    id: 'react',
    title: 'React',
    scaffoldingSupported: true,
  },
  {
    id: 'angular',
    title: 'Angular',
    scaffoldingSupported: true,
  },
  {
    id: 'vue',
    title: 'Vue',
    scaffoldingSupported: true,
  },
  {
    id: 'vanilla',
    title: 'Vanilla JavaScript',
    placeholder: UnsupportedFrameworkPlaceholder,
  },
  {
    id: 'bi',
    title: 'BI',
    placeholder: BIPlaceholder,
  },
];

type ChartContainerProps = {
  query: Query;
  meta: Meta;
  hideActions: boolean;
  chartType: ChartType;
  isGraphQLSupported: boolean;
  error?: Error;
  resultSet?: ResultSet;
  [k: string]: any;
};

type ChartContainerState = {
  activeTab: string;
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
      const { __cubejsPlayground } =
        props.iframeRef.current.contentWindow || {};

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
      activeTab: 'chart',
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
      activeTab,
      addingToDashboard,
      chartRendererError,
      sql,
    } = this.state;
    const {
      isChartRendererReady,
      resultSet,
      error,
      render,
      hideActions,
      query,
      chartingLibrary,
      setChartLibrary,
      chartLibraries,
      framework,
      setFramework,
      meta,
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
                disabled={!!frameworkItem?.placeholder || isFetchingMeta}
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
              type={activeTab === 'chart' ? 'primary' : 'default'}
              disabled={!!frameworkItem?.placeholder || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show Chart');
                this.setState({ activeTab: 'chart' });
              }}
            >
              Chart
            </Button>

            <Button
              data-testid="json-query-btn"
              size="small"
              type={activeTab === 'query' ? 'primary' : 'default'}
              disabled={!!frameworkItem?.placeholder || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show Query');
                this.setState({
                  activeTab: 'query',
                });
              }}
            >
              JSON Query
            </Button>

            <Button
              data-testid="graphiql-btn"
              icon={<GraphQLIcon />}
              size="small"
              type={activeTab === 'graphiql' ? 'primary' : 'default'}
              disabled={!!frameworkItem?.placeholder || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show GraphiQL');
                this.setState({ activeTab: 'graphiql' });
              }}
            >
              GraphiQL
            </Button>

            <Button
              data-testid="code-btn"
              icon={<CodeOutlined />}
              size="small"
              type={activeTab === 'code' ? 'primary' : 'default'}
              disabled={!!frameworkItem?.placeholder || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show Code');
                this.setState({ activeTab: 'code' });
              }}
            >
              Code
            </Button>

            <Button
              data-testid="sql-btn"
              icon={<QuestionCircleOutlined />}
              size="small"
              type={activeTab === 'generated-sql' ? 'primary' : 'default'}
              disabled={!!frameworkItem?.placeholder || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show SQL');
                this.setState({ activeTab: 'generated-sql' });
              }}
            >
              Generated SQL
            </Button>

            <Button
              data-testid="cache-btn"
              icon={<ThunderboltOutlined />}
              size="small"
              type={activeTab === 'cache' ? 'primary' : 'default'}
              disabled={!!frameworkItem?.placeholder || isFetchingMeta}
              onClick={() => {
                playgroundAction('Show Cache');
                this.setState({
                  activeTab: 'cache',
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
            disabled={!!frameworkItem?.placeholder || isFetchingMeta}
            onClick={() => playgroundAction('Open Code Sandbox')}
          >
            Edit
          </Button>
        </SectionRow>
      </form>
    );

    const queryText = JSON.stringify(query, null, 2);

    const renderChart = () => {
      if (frameworkItem?.placeholder) {
        const Placeholder = frameworkItem.placeholder;
        return <Placeholder framework={framework} />;
      } else if (activeTab === 'code') {
        if (error) {
          return <FatalError error={error} />;
        }

        return <PrismCode code={codeExample} />;
      } else if (activeTab === 'query') {
        return <PrismCode code={queryText} />;
      } else if (activeTab === 'generated-sql') {
        return (
          <Suspense
            fallback={
              <div style={{ height: 363 }}>
                <CubeLoader />
              </div>
            }
          >
            <SqlQueryTab
              query={query}
              onChange={(sql) => {
                this.setState({ sql });
              }}
            />
          </Suspense>
        );
      } else if (activeTab === 'cache') {
        return (
          <Suspense
            fallback={
              <div style={{ height: 363 }}>
                <CubeLoader />
              </div>
            }
          >
            <CachePane query={query} />
          </Suspense>
        );
      } else if (activeTab === 'graphiql' && meta) {
        if (!this.props.isGraphQLSupported) {
          return <div>GraphQL API is supported since version 0.29.0</div>;
        }

        return (
          <Suspense
            fallback={
              <div style={{ height: 363 }}>
                <CubeLoader />
              </div>
            }
          >
            <GraphiQLSandbox
              apiUrl={this.props.apiUrl}
              query={query}
              meta={meta}
            />
          </Suspense>
        );
      }

      return null;
    };

    let title;

    if (activeTab === 'code') {
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
    } else if (activeTab === 'query') {
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
    } else if (activeTab === 'generated-sql') {
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
    } else if (activeTab === 'cache') {
      title = 'Cache';
    } else if (activeTab === 'graphiql') {
      title = 'GraphQL API';
    } else {
      title = 'Chart';
    }

    return hideActions ? (
      render({ resultSet, error })
    ) : (
      <StyledCard title={title} extra={extra}>
        {renderChart()}
        {activeTab === 'chart' ? (
          render({ framework, error })
        ) : (
          <div style={{ display: 'none' }}>{render({ framework, error })}</div>
        )}
      </StyledCard>
    );
  }
}

export default withRouter(ChartContainer);
