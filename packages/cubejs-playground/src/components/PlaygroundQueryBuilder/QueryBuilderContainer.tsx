import { LockOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { CubeProvider } from '@cubejs-client/react';
import { Card, Space } from 'antd';
import { useLayoutEffect } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { Button, CubeLoader } from '../../atoms';
import { useAppContext, useCubejsApi, useSecurityContext } from '../../hooks';
import { useCloud } from '../../playground';
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

type QueryBuilderContainerProps = Pick<
  PlaygroundQueryBuilderProps,
  | 'defaultQuery'
  | 'initialVizState'
  | 'schemaVersion'
  | 'dashboardSource'
  | 'extra'
  | 'onVizStateChanged'
  | 'onSchemaChange'
> &
  Pick<QueryTabsProps, 'onTabChange'>;

export function QueryBuilderContainer(props: QueryBuilderContainerProps) {
  const { apiUrl } = useAppContext();
  const {
    currentToken,
    token: securityContextToken,
    setIsModalOpen,
  } = useSecurityContext();

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
              onVizStateChanged={props.onVizStateChanged}
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
  const { isAddRollupButtonVisible } = useCloud();

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

          {isAddRollupButtonVisible ? (
            <Button
              data-testid="rd-btn"
              icon={<ThunderboltOutlined />}
              size="small"
              onClick={() => toggleModal()}
            >
              Add Rollup to Schema
            </Button>
          ) : null}
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
