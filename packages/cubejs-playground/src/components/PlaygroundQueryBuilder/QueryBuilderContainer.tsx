import { LockOutlined } from '@ant-design/icons';
import { CubeProvider } from '@cubejs-client/react';
import { Card, Space } from 'antd';
import { useLayoutEffect } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { Button } from '../../atoms';
import { useCubejsApi, useSecurityContext } from '../../hooks';
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
> &
  Pick<QueryTabsProps, 'onTabChange'>;

export function QueryBuilderContainer({
  apiUrl,
  token,
  dashboardSource,
  ...props
}: QueryBuilderContainerProps) {
  const { location, push } = useHistory();

  const params = new URLSearchParams(location.search);
  const query = JSON.parse(params.get('query') || 'null');

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

  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <ChartRendererStateProvider>
        <StyledCard bordered={false}>
          <QueryTabs
            query={query}
            sidebar={
              <Space direction="horizontal">
                <Button
                  data-testid="security-context-btn"
                  icon={<LockOutlined />}
                  size="small"
                  type={securityContextToken ? 'primary' : 'default'}
                  onClick={() => setIsModalOpen(true)}
                >
                  {securityContextToken ? 'Edit' : 'Add'} Security Context
                </Button>
              </Space>
            }
            onTabChange={(tab) => {
              props.onTabChange?.(tab);
            }}
          >
            {({ id, query, chartType }, saveTab) => (
              <PlaygroundQueryBuilder
                queryId={id}
                apiUrl={apiUrl!}
                cubejsToken={currentToken!}
                initialVizState={{
                  query,
                  chartType,
                }}
                dashboardSource={dashboardSource}
                schemaVersion={props.schemaVersion}
                onVizStateChanged={(vizState) => {
                  saveTab({
                    query: vizState.query || {},
                    chartType: vizState.chartType,
                  });
                  props.onVizStateChanged?.(vizState);
                }}
              />
            )}
          </QueryTabs>
        </StyledCard>
      </ChartRendererStateProvider>
    </CubeProvider>
  );
}
