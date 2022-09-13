import Icon, { ThunderboltFilled } from '@ant-design/icons';
import { Query } from '@cubejs-client/core';
import { AvailableMembers } from '@cubejs-client/react';
import { Alert, Button, Space, Typography } from 'antd';
import styled from 'styled-components';

import { useServerCoreVersionGte } from '../../../hooks';
import { useRollupDesignerContext } from '../../../rollup-designer';
import { QueryStatus } from './PlaygroundQueryBuilder';

const Badge = styled.div`
  display: flex;
  align-items: center;
  padding: 2px 4px;
  border-radius: 4px;
  background: var(--warning-bg-color);
`;

type PreAggregationStatusProps = QueryStatus & {
  apiUrl: string;
  availableMembers: AvailableMembers;
  query: Query;
};

export function PreAggregationStatus({
  isAggregated,
  external,
  extDbType,
  preAggregationType,
}: PreAggregationStatusProps) {
  const isVersionGte = useServerCoreVersionGte('0.28.4');
  const { toggleModal } = useRollupDesignerContext();

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
                component={() => <ThunderboltFilled />}
              />
            </Space>
          </Badge>
        )}

        {isAggregated ? (
          <Typography.Text>
            Query was accelerated with pre-aggregation
          </Typography.Text>
        ) : isVersionGte ? (
          <Button
            data-testid="not-pre-agg-query-btn"
            type="link"
            onClick={() => toggleModal()}
          >
            Query was not accelerated with pre-aggregation {'->'}
          </Button>
        ) : null}

        {isAggregated && external && extDbType !== 'cubestore' ? (
          <Alert
            message="Consider migrating your pre-aggregations to Cube Store for better performance with larger datasets"
            type="warning"
          />
        ) : null}

        {isAggregated && !external && preAggregationType !== 'originalSql' ? (
          <Alert
            message={
              <>
                For optimized performance, consider using <b>external</b>{' '}
                {preAggregationType} pre-aggregation, rather than the source
                database (internal)
              </>
            }
            type="warning"
          />
        ) : null}
      </Space>
    </>
  );
}
