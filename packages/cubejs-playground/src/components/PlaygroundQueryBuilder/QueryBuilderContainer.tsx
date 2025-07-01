import { LockIcon, ThunderboltIcon, Panel, Space, Button } from '@cube-dev/ui-kit';
import { CubeProvider } from '@cubejs-client/react';
import { Card } from 'antd';
import { useLayoutEffect } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { CubeLoader } from '../../atoms';
import { useCloud } from '../../cloud';
import { useAppContext, useCubejsApi, useSecurityContext } from '../../hooks';
import {
  RollupDesignerContext,
  useRollupDesignerContext,
} from '../../rollup-designer';
import { ChartRendererStateProvider } from '../QueryTabs/ChartRendererStateProvider';
import { QueryTabs, QueryTabsProps } from '../QueryTabs/QueryTabs';
import {
  QueryBuilder,
  QueryBuilderProps,
  RequestStatusProps,
} from '../../QueryBuilderV2/index';
import Vizard from '../Vizard/Vizard';

import { PreAggregationStatus } from './components/index';

const StyledCard = styled(Card)`
  border-radius: 0;
  border-bottom: 1px;
  min-height: 100%;
  background: var(--layout-body-background);

  & .ant-card-body {
    padding: 0;
  }
`;

function RequestStatusComponent({
  isAggregated,
  external,
  extDbType,
  preAggregationType,
}: RequestStatusProps) {
  return (
    <Space direction="vertical" gap="0" placeItems="end" margin="-1x 0">
      <PreAggregationStatus
        preAggregationType={preAggregationType}
        isAggregated={isAggregated}
        external={external}
        extDbType={extDbType}
      />
    </Space>
  );
}

type QueryBuilderContainerProps = Pick<
  QueryBuilderProps,
  | 'defaultQuery'
  | 'initialVizState'
  | 'schemaVersion'
  | 'extra'
  | 'onSchemaChange'
  | 'onQueryChange'
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
    <CubeProvider cubeApi={cubejsApi}>
      <RollupDesignerContext apiUrl={apiUrl!}>
        <ChartRendererStateProvider>
          <StyledCard bordered={false}>
            <QueryTabsRenderer
              apiUrl={apiUrl!}
              token={currentToken!}
              securityContextToken={securityContextToken}
              extra={props.extra}
              schemaVersion={props.schemaVersion}
              onSchemaChange={props.onSchemaChange}
              onQueryChange={props.onQueryChange}
              onTabChange={props.onTabChange}
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
  QueryBuilderProps,
  'schemaVersion' | 'onSchemaChange' | 'onQueryChange' | 'extra'
> &
  Pick<QueryTabsProps, 'onTabChange'>;

function QueryTabsRenderer({
  apiUrl,
  token,
  onQueryChange,
  securityContextToken,
  onSecurityContextModalOpen,
  ...props
}: QueryTabsRendererProps) {
  const { location } = useHistory();
  const { setQuery, toggleModal, isLoading } = useRollupDesignerContext();
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
            isLoading={isLoading}
            icon={<LockIcon />}
            size="small"
            type={securityContextToken ? 'primary' : 'secondary'}
            onPress={onSecurityContextModalOpen}
          >
            {securityContextToken ? 'Edit' : 'Add'} Security Context
          </Button>

          {isAddRollupButtonVisible == null || isAddRollupButtonVisible ? (
            <Button
              data-testid="rd-btn"
              isLoading={isLoading}
              icon={<ThunderboltIcon />}
              size="small"
              onPress={() => toggleModal()}
            >
              Add Rollup to Data Model
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
        <Panel key={id} height="(100vh - 12.5x) (100vh - 12.5x)" fill="#white">
          <QueryBuilder
            apiUrl={apiUrl}
            apiToken={token}
            defaultQuery={query}
            defaultChartType={chartType}
            schemaVersion={props.schemaVersion}
            extra={props.extra ?? null}
            RequestStatusComponent={RequestStatusComponent}
            VizardComponent={Vizard}
            onSchemaChange={props.onSchemaChange}
            onQueryChange={(data) => {
              saveTab(data);
              onQueryChange?.(data);
            }}
          />
        </Panel>
      )}
    </QueryTabs>
  );
}
