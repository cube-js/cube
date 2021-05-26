import styled from 'styled-components';
import { Button, Modal, Space, Typography } from 'antd';
import { useState } from 'react';
import { TransformedQuery } from '@cubejs-client/core';
import Icon from '@ant-design/icons';

import { LightningIcon } from '../../../shared/icons/LightningIcon';
import { PreAggregationHelper } from './PreAggregationHelper';

const Badge = styled.div`
  display: flex;
  align-items: center;
  padding: 2px 4px;
  border-radius: 4px;
  background: var(--warning-bg-color);
`;

type PreAggregationStatusProps = {
  timeElapsed: number;
  isAggregated: boolean;
  transformedQuery?: TransformedQuery;
};

export function PreAggregationStatus({
  isAggregated,
  transformedQuery,
}: PreAggregationStatusProps) {
  const [isModalOpen, setIsModalOpen] = useState<boolean>(false);
  // hide it for the time being
  // const renderTime = () => (
  //   <Typography.Text strong style={{ color: 'rgba(20, 20, 70, 0.85)' }}>
  //     {formatNumber(timeElapsed)} ms
  //   </Typography.Text>
  // );

  return (
    <>
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
          <Button type="link" onClick={() => setIsModalOpen(true)}>
            Query was not accelerated with pre-aggregation {'->'}
          </Button>
        )}
      </Space>

      <Modal
        title="Pre-aggregation"
        visible={isModalOpen}
        footer={null}
        bodyStyle={{
          paddingTop: 16,
        }}
        onCancel={() => {
          setIsModalOpen(false);
        }}
      >
        {transformedQuery ? (
          <PreAggregationHelper transformedQuery={transformedQuery} />
        ) : null}
      </Modal>
    </>
  );
}
