import React, { useEffect, useRef, useState } from 'react';

import { dispatchChartEvent } from '../../PlaygroundQueryBuilder';
import useDeepCompareMemoize from '../../hooks/deep-compare-memoize';

// const buildPath = '/chart-renderers/react';

// async function getEmbeds() {
//   try {
//     const html = await (await fetch(`${buildPath}/index.html`)).text();
//     const styles = [];

//     const reg = /<script src="([^"]*)"/g;
//     const scripts = (html.match(reg) || []).map((e) => ({
//       src: buildPath + e.replace(reg, '$1').replace(/^\.\//, '/'),
//     }));
//     const [, body] = html.match(/<script>(.*?)<\/script>/);
//     scripts.push({
//       body: body.replace(/\.\/static/, `${buildPath}/static`),
//     });

//     return {
//       scripts,
//       styles,
//     };
//   } catch (error) {
//     console.error(error);
//     return {};
//   }
// }

export default function ChartRenderer({
  iframeRef,
  framework,
  isChartRendererReady,
  chartingLibrary,
  chartType,
  query,
  pivotConfig,
  onChartRendererReady
}) {
  // const iframeRef = useRef();
  // const [ready, setReady] = useState(false);

  useEffect(() => {
    if (iframeRef.current) {
      iframeRef.current.contentWindow.addEventListener('cubejsChartReady', () => {
        console.log('onChartRendererReady >>> ready!!!');
        // setReady(true);
        onChartRendererReady(true);
      });
    }
  }, [iframeRef]);

  useEffect(() => {
    if (isChartRendererReady && iframeRef.current) {
      dispatchChartEvent(iframeRef.current.contentDocument, {
        pivotConfig,
        query,
        chartType,
        chartingLibrary,
      });
    }
  }, useDeepCompareMemoize([iframeRef, isChartRendererReady, pivotConfig, query, chartType]));

  return (
    <>
      <button onClick={() => console.log(new Date())}>console</button>
      <button
        onClick={() => {
          dispatchChartEvent(iframeRef.current?.contentDocument, {
            pivotConfig,
            query,
            chartType,
            chartingLibrary,
          });
        }}
      >
        displatch
      </button>

      <iframe
        ref={iframeRef}
        style={{
          width: '100%',
          minHeight: 400,
          border: 'none',
        }}
        title="Angular Charts"
        // src="./chart-renderers/angular/index.html"
        src="./chart-renderers/react/index.html"
      />
    </>
  );
}
