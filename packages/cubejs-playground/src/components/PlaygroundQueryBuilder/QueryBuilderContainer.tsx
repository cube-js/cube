import { LockOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { CubeProvider } from '@cubejs-client/react';
import { Card, Space } from 'antd';
import { useLayoutEffect } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { Button, CubeLoader } from '../../atoms';
import { useCloud } from '../../cloud';
import {
  useAppContext,
  useCubejsApi,
  useSecurityContext
} from '../../hooks';
import {
  RollupDesignerContext,
  useRollupDesignerContext,
} from '../../rollup-designer';
import { ChartRendererStateProvider } from '../QueryTabs/ChartRendererStateProvider';
import { QueryTabs, QueryTabsProps } from '../QueryTabs/QueryTabs';
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
              securityContextToken={securityContextToken}
              extra={props.extra}
              schemaVersion={props.schemaVersion}
              onSchemaChange={props.onSchemaChange}
              onTabChange={props.onTabChange}
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
  | 'onVizStateChanged'
  | 'onSchemaChange'
  | 'extra'
> &
  Pick<QueryTabsProps, 'onTabChange'>;

function QueryTabsRenderer({
  apiUrl,
  token,
  securityContextToken,
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

          {isAddRollupButtonVisible == null || isAddRollupButtonVisible ? (
            <Button
              data-testid="rd-btn"
              icon={<ThunderboltOutlined />}
              size="small"
              onClick={() => toggleModal()}
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
        <PlaygroundQueryBuilder
          queryId={id}
          apiUrl={apiUrl}
          cubejsToken={token}
          initialVizState={{
            query,
            chartType,
          }}
          schemaVersion={props.schemaVersion}
          extra={props.extra}
          onSchemaChange={props.onSchemaChange}
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
