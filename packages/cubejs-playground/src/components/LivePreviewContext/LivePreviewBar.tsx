import { Card } from '../../atoms';
import { useLivePreviewContext } from '../../hooks';
import { Space, Typography } from 'antd';
import { LoadingOutlined, CheckCircleOutlined } from '@ant-design/icons';

const StatusIcon = ({ status, uploading }) => {
  const statusMap = {
    'loading': <LoadingOutlined spin />,
    'inProgress': <LoadingOutlined spin />,
    'running': <CheckCircleOutlined />
  };

  return uploading ? statusMap.loading : (statusMap[status] || statusMap.loading);
};

const LivePreviewBar = () => {
  const { statusLivePreview } = useLivePreviewContext();
  return (
    <Card
      bordered={false}
      style={{
        borderRadius: 0,
        borderBottom: 1,
      }}
    >
      <Space>
        <Typography.Text strong>
          Live preview mode
        </Typography.Text>

        <StatusIcon {...statusLivePreview} />
        <Typography.Text>{statusLivePreview.deploymentUrl}</Typography.Text>
      </Space>
    </Card>
  );
};

export default LivePreviewBar