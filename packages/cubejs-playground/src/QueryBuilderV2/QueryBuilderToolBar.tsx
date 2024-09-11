import { useMemo } from 'react';
import { SerializedResult } from '@cubejs-client/core';
import { Button, Flex, Space, tasty, TooltipProvider } from '@cube-dev/ui-kit';
import { PlayCircleOutlined, ReloadOutlined } from '@ant-design/icons';

import { QueryBuilderError } from './QueryBuilderError';
import { useQueryBuilderContext } from './context';
import { PreAggregationAlerts } from './components/PreAggregationAlerts';

const StopIcon = tasty({
  styles: {
    position: 'relative',
    width: '16px',
    height: '16px',

    '&::before': {
      content: '""',
      display: 'block',
      position: 'absolute',
      top: '2px',
      left: '2px',
      width: '12px',
      height: '12px',
      fill: '#danger',
    },
  },
});

export function QueryBuilderToolBar() {
  const {
    runQuery,
    isVerifying,
    verificationError,
    isLoading,
    error,
    resultSet,
    isQueryTouched,
    isQueryEmpty,
    isApiBlocked,
    stopQuery,
    RequestStatusComponent,
  } = useQueryBuilderContext();

  const {
    requestId,
    external,
    dbType,
    extDbType,
    usedPreAggregations = {},
  } = useMemo(() => {
    if (resultSet) {
      const { loadResponse } = resultSet?.serialize();

      return loadResponse.results[0] || {};
    }

    return {} as SerializedResult['loadResponse'];
  }, [resultSet]);

  // @ts-ignore
  const preAggregationType = Object.values(usedPreAggregations || {})[0]?.type;

  const isAggregated = Object.keys(usedPreAggregations).length > 0;

  return (
    <Flex flow="column" padding="1x" gap="1x">
      <Space height="min-content" placeContent="space-between">
        <Space gap="1x">
          <TooltipProvider
            title={
              <>
                <kbd>⌘</kbd> + <kbd>Enter</kbd> <span style={{ padding: '0 16px' }}>OR</span>{' '}
                <kbd>Ctrl</kbd> + <kbd>Enter</kbd>
              </>
            }
          >
            <Button
              qa="RunQueryButton"
              type="primary"
              size="small"
              isDisabled={isQueryEmpty || !!verificationError || isVerifying || isApiBlocked}
              isLoading={isLoading}
              icon={
                !isQueryEmpty && (isLoading || !isQueryTouched) ? (
                  <ReloadOutlined />
                ) : (
                  <PlayCircleOutlined />
                )
              }
              onPress={runQuery}
            >
              Run Query
            </Button>
          </TooltipProvider>
          {isLoading ? (
            <Button
              qa="StopQueryButton"
              theme="danger"
              size="small"
              icon={<StopIcon />}
              onPress={stopQuery}
            >
              Stop
            </Button>
          ) : null}
        </Space>
        {requestId && RequestStatusComponent ? (
          <RequestStatusComponent
            requestId={requestId}
            isAggregated={isAggregated}
            preAggregationType={preAggregationType}
            external={external}
            dbType={dbType}
            extDbType={extDbType}
            error={error ?? undefined}
          />
        ) : undefined}
      </Space>
      <PreAggregationAlerts />
      <QueryBuilderError />
    </Flex>
  );
}
