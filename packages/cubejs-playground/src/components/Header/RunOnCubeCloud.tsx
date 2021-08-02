import { CaretDownOutlined, CloudFilled } from '@ant-design/icons';
import { Button, Card, Dropdown, Input, Typography } from 'antd';
import { useState } from 'react';

import { Box, Flex } from '../../grid';
import { useLivePreviewContext } from '../../hooks';
import { StatusIcon } from '../LivePreviewContext/LivePreviewBar';

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
  apiUrl: string;
  loading: boolean;
  onStopClick: () => void;
};

function LivePreviewOverlay({
  apiUrl,
  loading,
  onStopClick,
}: LivePreviewOverlayProps) {
  return (
    <Card style={{ maxWidth: 600 }}>
      <Flex direction="column" gap={2}>
        <Typography.Paragraph>
          Playground users the following API URL to execute queries on Cloud.
          You can use this API to test queries in your application. Learn more
          on developing and testing with Cube Cloud.
        </Typography.Paragraph>

        <Input value={apiUrl} />

        <Box>
          <Flex justifyContent="space-between">
            <Button>Inspect querires</Button>

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
