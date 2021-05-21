import styled from 'styled-components';
import { Space, Typography } from 'antd';
import Icon from '@ant-design/icons';

import { LightningIcon } from '../../../shared/icons/LightningIcon';

type PreAggregationStatusProps = {
  timeElapsed: number;
  isAggregated: boolean;
};

const Badge = styled.div`
  display: flex;
  align-items: center;
  padding: 2px 4px;
  border-radius: 4px;
  background: var(--warning-bg-color);
`;

export function PreAggregationStatus({
  isAggregated,
}: PreAggregationStatusProps) {
  // hide it for the time being
  // const renderTime = () => (
  //   <Typography.Text strong style={{ color: 'rgba(20, 20, 70, 0.85)' }}>
  //     {formatNumber(timeElapsed)} ms
  //   </Typography.Text>
  // );

  return (
    <Space style={{ marginLeft: 'auto' }}>
      {isAggregated && (
        <Badge>
          <Space size={4}>
            <Icon
              style={{ color: 'var(--warning-color)' }}
              component={() => <LightningIcon />}
            />
          </Space>
        </Badge>
      )}

      {isAggregated ? (
        <Typography.Text>
          Query was accelerated with pre-aggregation
        </Typography.Text>
      ) : (
        <Typography.Link
          href="https://cube.dev/docs/caching/pre-aggregations/getting-started"
          target="_blank"
        >
          Query was not accelerated with pre-aggregation {'->'}
        </Typography.Link>
      )}
    </Space>
  );
}
