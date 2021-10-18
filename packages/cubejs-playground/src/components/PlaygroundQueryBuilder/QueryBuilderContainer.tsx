import { LockOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { CubeProvider } from '@cubejs-client/react';
import { Card, Space } from 'antd';
import { useLayoutEffect } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { Button, CubeLoader } from '../../atoms';
import { useCubejsApi, useSecurityContext } from '../../hooks';
// import { LightningIcon } from '../../shared/icons/LightningIcon';
import { ChartRendererStateProvider } from '../QueryTabs/ChartRendererStateProvider';
import { QueryTabs, QueryTabsProps } from '../QueryTabs/QueryTabs';
import {
  RollupDesignerContext,
  useRollupDesignerContext,
} from '../RollupDesigner';
import {
  PlaygroundQueryBuilder,
  PlaygroundQueryBuilderProps,
} from './components/PlaygroundQueryBuilder';

const StyledCard = styled(Card)`
  border-radius: 0;
  border-bottom: 1px;
  min-height: 100%;
  background: var(--layout-body-background);

  & .ant-card-body {
    padding: 0;
  }
`;

type QueryBuilderContainerProps = {
  apiUrl: string | null;
  token: string | null;
} & Pick<
  PlaygroundQueryBuilderProps,
  | 'defaultQuery'
  | 'initialVizState'
  | 'schemaVersion'
  | 'dashboardSource'
  | 'onVizStateChanged'
  | 'onSchemaChange'
  | 'extra'
> &
  Pick<QueryTabsProps, 'onTabChange'>;

export function QueryBuilderContainer({
  apiUrl,
  token,
  ...props
}: QueryBuilderContainerProps) {
  const { token: securityContextToken, setIsModalOpen } = useSecurityContext();

  const currentToken = securityContextToken || token;

  useLayoutEffect(() => {
    if (apiUrl && currentToken) {
      window['__cubejsPlayground'] = {
        ...window['__cubejsPlayground'],
        apiUrl,
        token: currentToken,
      };
    }
  }, [apiUrl, currentToken]);

  const cubejsApi = useCubejsApi(apiUrl, currentToken);

  if (!cubejsApi) {
    return <CubeLoader />;
  }

  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <RollupDesignerContext apiUrl={apiUrl!}>
        <ChartRendererStateProvider>
          <StyledCard bordered={false}>
            <QueryTabsRenderer
              apiUrl={apiUrl!}
              token={currentToken!}
              dashboardSource={props.dashboardSource}
              securityContextToken={securityContextToken}
              onTabChange={props.onTabChange}
              extra={props.extra}
              onSecurityContextModalOpen={() => setIsModalOpen(true)}
            />
          </StyledCard>
        </ChartRendererStateProvider>
      </RollupDesignerContext>
    </CubeProvider>
  );
}

type QueryTabsRendererProps = {
  apiUrl: string;
  token: string;
  securityContextToken: string | null;
  onSecurityContextModalOpen: () => void;
} & Pick<
  PlaygroundQueryBuilderProps,
  | 'schemaVersion'
  | 'dashboardSource'
  | 'onVizStateChanged'
  | 'onSchemaChange'
  | 'extra'
> &
  Pick<QueryTabsProps, 'onTabChange'>;

function QueryTabsRenderer({
  apiUrl,
  token,
  securityContextToken,
  dashboardSource,
  schemaVersion,
  onSecurityContextModalOpen,
  ...props
}: QueryTabsRendererProps) {
  const { location } = useHistory();
  const { setQuery, toggleModal } = useRollupDesignerContext();

  const params = new URLSearchParams(location.search);
  const query = JSON.parse(params.get('query') || 'null');

  return (
    <QueryTabs
      query={query}
      sidebar={
        <Space direction="horizontal">
          <Button
            data-testid="security-context-btn"
            icon={<LockOutlined />}
            size="small"
            type={securityContextToken ? 'primary' : 'default'}
            onClick={onSecurityContextModalOpen}
          >
            {securityContextToken ? 'Edit' : 'Add'} Security Context
          </Button>

          <Button
            data-testid="rd-btn"
            icon={<ThunderboltOutlined />}
            size="small"
            onClick={() => toggleModal()}
          >
            Add Rollup to Schema
          </Button>
        </Space>
      }
      onTabChange={(tab) => {
        props.onTabChange?.(tab);
        setQuery(tab.query);
      }}
    >
      {({ id, query, chartType }, saveTab) => (
        <PlaygroundQueryBuilder
          queryId={id}
          apiUrl={apiUrl}
          cubejsToken={token}
          initialVizState={{
            query,
            chartType,
          }}
          dashboardSource={dashboardSource}
          schemaVersion={schemaVersion}
          extra={props.extra}
          onVizStateChanged={(vizState) => {
            saveTab({
              query: vizState.query || {},
              chartType: vizState.chartType,
            });
            props.onVizStateChanged?.(vizState);

            if (vizState.query) {
              setQuery(vizState.query);
            }
          }}
        />
      )}
    </QueryTabs>
  );
}
