import { Button, Space, Typography } from 'antd';
import Icon, { SearchOutlined, LoadingOutlined } from '@ant-design/icons';
import { useEffect, useState } from 'react';
import { useCloud, FetchRequestFromApmResult } from '../../../playground/cloud';

type RequestApmStatusProps = {
  requestId: string;
};

export function RequestApmStatus({ requestId }: RequestApmStatusProps) {
  const [isLoading, setIsLoading] = useState(true);
  const [prevRequestId, setPrevRequestId] = useState<string>();
  const [result, setResult] = useState<FetchRequestFromApmResult>();
  const { isCloud, fetchRequestFromApm } = useCloud();

  useEffect(() => {
    if (isCloud && requestId && fetchRequestFromApm) {
      setIsLoading(true);
      fetchRequestFromApm(requestId, prevRequestId)
        .then((res) => {
          console.log('requestId', res, requestId);
          if (res.request) {
            setResult(res);
          }
          if (res.request || res.error !== 'Canceled') {
            setIsLoading(false);
          }
        })
        .catch((err) => {
          setIsLoading(false);
        });
      setPrevRequestId(requestId);
    }
  }, [requestId]);

  if (!isCloud) {
    return null;
  }

  if (isLoading) {
    return <LoadingOutlined spin />;
  }

  return result?.request ? (
    <Button type="link" onClick={() => result.request?.onClick()}>
      <Space>
        <Space size={4}>
          <Icon component={() => <SearchOutlined />} />
        </Space>

        <Typography.Text>{result.request.duration}</Typography.Text>
      </Space>
    </Button>
  ) : null;
}
