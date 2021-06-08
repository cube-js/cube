import { useMemo } from 'react';
import { Card, Space } from 'antd';
import styled from 'styled-components';
import { CloudOutlined, LockOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router';

import { Button } from '../../atoms';
import LivePreviewBar from '../LivePreviewContext/LivePreviewBar';
import { useLivePreviewContext, useSecurityContext } from '../../hooks';
import DashboardSource from '../../DashboardSource';
import { PlaygroundQueryBuilder } from './components/PlaygroundQueryBuilder';
import { QueryTabs } from '../QueryTabs/QueryTabs';

const StyledCard: typeof Card = styled(Card)`
  border-radius: 0;
  border-bottom: 1px;
  min-height: 100%;

  & .ant-card-body {
    padding: 0;
  }
`;

type QueryBuilderContainerProps = {
  apiUrl: string;
  token: string;
  schemaVersion: number;
};

export function QueryBuilderContainer({
  apiUrl,
  token,
  schemaVersion,
}: QueryBuilderContainerProps) {
  const dashboardSource = useMemo(() => new DashboardSource(), []);

  const { push, location } = useHistory();
  const params = new URLSearchParams(location.search);
  const query = JSON.parse(params.get('query') || '{}');

  const { token: securityContextToken, setIsModalOpen } = useSecurityContext();
  const livePreviewContext = useLivePreviewContext();

  return (
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

            {livePreviewContext?.statusLivePreview.active && <LivePreviewBar />}
          </Space>
        }
      >
        {({ id, query }, saveTab) => (
          <PlaygroundQueryBuilder
            queryId={id}
            apiUrl={apiUrl}
            cubejsToken={token}
            defaultQuery={query}
            dashboardSource={dashboardSource}
            schemaVersion={schemaVersion}
            onVizStateChanged={({ query }) => {
              push(`/build?query=${JSON.stringify(query)}`);
              saveTab({ query: query || {} });
            }}
          />
        )}
      </QueryTabs>
    </StyledCard>
  );
}
