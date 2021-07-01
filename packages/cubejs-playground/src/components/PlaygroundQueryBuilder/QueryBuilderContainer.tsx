import { useLayoutEffect, useMemo } from 'react';
import { Card, Space } from 'antd';
import styled from 'styled-components';
import { CloudOutlined, LockOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router';
import { CubeProvider } from '@cubejs-client/react';

import { Button } from '../../atoms';
import LivePreviewBar from '../LivePreviewContext/LivePreviewBar';
import {
  useCubejsApi,
  useLivePreviewContext,
  useSecurityContext,
} from '../../hooks';
import DashboardSource from '../../DashboardSource';
import {
  PlaygroundQueryBuilder,
  PlaygroundQueryBuilderProps,
} from './components/PlaygroundQueryBuilder';
import { QueryTabs } from '../QueryTabs/QueryTabs';

const StyledCard = styled(Card)`
  border-radius: 0;
  border-bottom: 1px;
  min-height: 100%;

  & .ant-card-body {
    padding: 0;
  }
`;

type QueryBuilderContainerProps = {
  apiUrl?: string;
  token?: string;
} & Pick<
  PlaygroundQueryBuilderProps,
  | 'defaultQuery'
  | 'initialVizState'
  | 'schemaVersion'
  | 'dashboardSource'
  | 'onVizStateChanged'
  | 'onSchemaChange'
>;

export function QueryBuilderContainer({
  apiUrl,
  token,
  dashboardSource,
  ...props
}: QueryBuilderContainerProps) {
  const { location } = useHistory();
  const params = new URLSearchParams(location.search);
  const query = JSON.parse(params.get('query') || '{}');

  const { token: securityContextToken, setIsModalOpen } = useSecurityContext();
  const livePreviewContext = useLivePreviewContext();

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
      <StyledCard bordered={false}>
        <QueryTabs
          query={query}
          sidebar={
            <Space direction="horizontal">
              <Button.Group>
                <Button
                  data-testid="security-context-btn"
                  icon={<LockOutlined />}
                  size="small"
                  type={securityContextToken ? 'primary' : 'default'}
                  onClick={() => setIsModalOpen(true)}
                >
                  {securityContextToken ? 'Edit' : 'Add'} Security Context
                </Button>

                {livePreviewContext && !livePreviewContext.livePreviewDisabled && (
                  <Button
                    data-testid="live-preview-btn"
                    icon={<CloudOutlined />}
                    size="small"
                    type={
                      livePreviewContext.statusLivePreview.active
                        ? 'primary'
                        : 'default'
                    }
                    onClick={() =>
                      livePreviewContext.statusLivePreview.active
                        ? livePreviewContext.stopLivePreview()
                        : livePreviewContext.startLivePreview()
                    }
                  >
                    {livePreviewContext.statusLivePreview.active
                      ? 'Stop'
                      : 'Start'}{' '}
                    Live Preview
                  </Button>
                )}
              </Button.Group>

              {livePreviewContext?.statusLivePreview.active && (
                <LivePreviewBar />
              )}
            </Space>
          }
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
    </CubeProvider>
  );
}
