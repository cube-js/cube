import React, { useEffect } from 'react';
import { Alert, Spin } from 'antd';

import { dispatchChartEvent } from '../../utils';
import useDeepCompareMemoize from '../../hooks/deep-compare-memoize';
import useSlowQuery from '../../hooks/slow-query';

export default function ChartRenderer({
  iframeRef,
  framework,
  isChartRendererReady,
  chartingLibrary,
  chartType,
  query,
  pivotConfig,
  onChartRendererReadyChange,
}) {
  const slowQuery = useSlowQuery();
  
  useEffect(() => {
    return () => {
      onChartRendererReadyChange(false);
    };
    // eslint-disable-next-line
  }, []);

  useEffect(() => {
    if (isChartRendererReady && iframeRef.current) {
      dispatchChartEvent(iframeRef.current.contentDocument, {
        pivotConfig,
        query,
        chartType,
        chartingLibrary,
      });
    }
    // eslint-disable-next-line
  }, useDeepCompareMemoize([iframeRef, isChartRendererReady, pivotConfig, query, chartType]));

  return (
    <div>
      {slowQuery ? <Alert message="Warning: slow query" type="warning" /> : null}
      
      {!isChartRendererReady ? <Spin /> : null}

      <iframe
        ref={iframeRef}
        style={{
          width: '100%',
          minHeight: 400,
          border: 'none',
          visibility: isChartRendererReady ? 'visible' : 'hidden',
        }}
        title="Chart renderer"
        src={`/chart-renderers/${framework}/index.html`}
      />
    </div>
  );
}
