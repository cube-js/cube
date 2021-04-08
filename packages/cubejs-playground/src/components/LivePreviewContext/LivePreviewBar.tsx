import { Space, Typography } from 'antd';
import { LoadingOutlined, CheckCircleOutlined } from '@ant-design/icons';

import { Card } from '../../atoms';
import { useLivePreviewContext } from '../../hooks';

const StatusIcon = ({ status, uploading }) => {
  const statusMap = {
    loading: <LoadingOutlined spin />,
    inProgress: <LoadingOutlined spin />,
    running: <CheckCircleOutlined />,
  };

  return uploading ? statusMap.loading : statusMap[status] || statusMap.loading;
};

const LivePreviewBar = () => {
  const livePreviewContext = useLivePreviewContext();
  return (
    <Card
      bordered={false}
      style={{
        borderRadius: 0,
        borderBottom: 1,
      }}
    >
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
    </Card>
  );
};

export default LivePreviewBar;
