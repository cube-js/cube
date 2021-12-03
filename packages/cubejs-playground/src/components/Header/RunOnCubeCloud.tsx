import { CloudFilled } from '@ant-design/icons';
import { Button, Card, Dropdown, Typography } from 'antd';
import { useState } from 'react';

import { Box, Flex } from '../../grid';
import { useLivePreviewContext } from '../../hooks';
import { copyToClipboard } from '../../utils';
import { CopiableInput } from '../CopiableInput';
import { StatusIcon } from '../LivePreviewContext/LivePreviewBar';
import { LivePreviewStatus } from '../LivePreviewContext/LivePreviewContextProvider';
import { StyledMenuButton } from './Menu';

export function RunOnCubeCloud() {
  const livePreviewContext = useLivePreviewContext();
  const [loading, setLoading] = useState<boolean>(false);

  if (!livePreviewContext || livePreviewContext?.livePreviewDisabled) {
    return null;
  }

  const { active, status, uploading } = livePreviewContext.statusLivePreview;

  const button = (
    <StyledMenuButton
      data-testid="live-preview-btn"
      onClick={() => {
        if (!active) {
          livePreviewContext.startLivePreview();
        }
      }}
    >
      {active ? (
        <StatusIcon status={status} uploading={uploading} />
      ) : (
        <CloudFilled />
      )}
      {!active ? 'Run' : 'Running'} on Cube Cloud
    </StyledMenuButton>
  );

  if (!active) {
    return button;
  }

  return (
    <Dropdown
      overlay={
        <LivePreviewOverlay
          livePreviewStatus={livePreviewContext.statusLivePreview}
          apiUrl={livePreviewContext.credentials?.apiUrl || ''}
          loading={loading}
          onStopClick={async () => {
            setLoading(true);
            await livePreviewContext.stopLivePreview();
            setLoading(false);
          }}
        />
      }
      trigger={['click']}
    >
      {button}
    </Dropdown>
  );
}

type LivePreviewOverlayProps = {
  livePreviewStatus: LivePreviewStatus;
  apiUrl: string;
  loading: boolean;
  onStopClick: () => void;
};

function LivePreviewOverlay({
  livePreviewStatus,
  apiUrl,
  loading,
  onStopClick,
}: LivePreviewOverlayProps) {
  const { url, deploymentId } = livePreviewStatus;

  return (
    <Card style={{ maxWidth: 600 }}>
      <Flex direction="column" gap={2}>
        <Typography.Paragraph>
          Playground uses the following API URL to execute queries on Cloud. You
          can use this API to test queries in your application.{' '}
          <Typography.Link href="https://cube.dev/docs/cloud" target="_blank">
            Learn more
          </Typography.Link>{' '}
          on developing and testing with Cube Cloud.
        </Typography.Paragraph>

        <CopiableInput value={apiUrl} onCopyClick={copyToClipboard} />

        <Box>
          <Flex justifyContent="space-between">
            <Button
              type="primary"
              target="_blank"
              href={`${url}/deployments/${deploymentId}/history`}
            >
              Inspect queries
            </Button>

            <Button
              type="primary"
              danger
              loading={loading}
              onClick={onStopClick}
            >
              Stop
            </Button>
          </Flex>
        </Box>
      </Flex>
    </Card>
  );
}
