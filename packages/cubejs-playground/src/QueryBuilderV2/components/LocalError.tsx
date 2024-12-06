import { Alert, Space } from '@cube-dev/ui-kit';
import { useMemo } from 'react';

type Props = {
  error: Error;
};

export function LocalError({ error }: Props) {
  const message = useMemo(() => {
    return error?.message ?? String(error) ?? 'Something went wrong.';
  }, [error]);

  return (
    <Alert theme="danger">
      <Space direction="vertical">
        {Array.isArray(message) ? <span>{message.join(', ')}</span> : <span>{message}</span>}
      </Space>
    </Alert>
  );
}
