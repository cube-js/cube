import { CaretDownOutlined, CloudFilled } from '@ant-design/icons';
import { Button, Card, Dropdown, Typography } from 'antd';
import { useState } from 'react';

import { Box, Flex } from '../../grid';
import { useLivePreviewContext } from '../../hooks';
import { copyToClipboard } from '../../utils';
import { CopiableInput } from '../CopiableInput';
import { StatusIcon } from '../LivePreviewContext/LivePreviewBar';
import { LivePreviewStatus } from '../LivePreviewContext/LivePreviewContextProvider';

export function RunOnCubeCloud() {
  const livePreviewContext = useLivePreviewContext();
  const [loading, setLoading] = useState<boolean>(false);

  if (!livePreviewContext || livePreviewContext?.livePreviewDisabled) {
    return null;
  }

  const { active, status, uploading } = livePreviewContext.statusLivePreview;

  return (
    <Button.Group
      style={{
        float: 'right',
        margin: 8,
      }}
    >
      <Button
        data-testid="live-preview-btn"
        ghost
        icon={
          active ? (
            <StatusIcon status={status} uploading={uploading} />
          ) : (
            <CloudFilled />
          )
        }
        onClick={() => {
          if (!active) {
            livePreviewContext.startLivePreview();
          }
        }}
      >
        {!active ? 'Run on Cube Cloud' : 'Live Preview'}
      </Button>

      {livePreviewContext.statusLivePreview.active ? (
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
          <Button ghost>
            <CaretDownOutlined />
          </Button>
        </Dropdown>
      ) : null}
    </Button.Group>
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
          Playground users the following API URL to execute queries on Cloud.
          You can use this API to test queries in your application. Learn more
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
              Inspect querires
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
