import { Space, Typography } from 'antd';
import { LoadingOutlined, CheckCircleOutlined } from '@ant-design/icons';

import { useLivePreviewContext } from '../../hooks';

export function StatusIcon({ status, uploading }) {
  const statusMap = {
    loading: <LoadingOutlined spin />,
    inProgress: <LoadingOutlined spin />,
    running: <CheckCircleOutlined />,
  };

  return uploading ? statusMap.loading : statusMap[status] || statusMap.loading;
}

const LivePreviewBar = () => {
  const livePreviewContext = useLivePreviewContext();

  return (
    <Space>
      <Typography.Text strong>Live preview mode</Typography.Text>

      <StatusIcon
        status={livePreviewContext?.statusLivePreview.status}
        uploading={livePreviewContext?.statusLivePreview.uploading}
      />
      <Typography.Text>
        {livePreviewContext?.statusLivePreview.deploymentUrl}
      </Typography.Text>
    </Space>
  );
};

export default LivePreviewBar;
