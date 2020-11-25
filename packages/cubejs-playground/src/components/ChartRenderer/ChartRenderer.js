import React, { useEffect, useState } from 'react';
import { Helmet } from 'react-helmet';
import { dispatchChartEvent } from '../../PlaygroundQueryBuilder';
import useDeepCompareMemoize from '../../hooks/deep-compare-memoize';

const buildPath = '/chart-renderers/build';

async function getEmbeds() {
  return fetch(`${buildPath}/index.html`)
    .then((response) => response.text())
    .then((html) => {
      const styles = [];

      const reg = /<script src="([^"]*)"/g;
      const scripts = (html.match(reg) || []).map((e) => ({
        src: buildPath + e.replace(reg, '$1').replace(/^\.\//, '/'),
      }));
      const [, body] = html.match(/<script>(.*?)<\/script>/);
      scripts.push({
        body: body.replace(/\.\/static/, `${buildPath}/static`),
      });

      return {
        scripts,
        styles,
      };
    });
}

export default function ChartRenderer({
  framework,
  chartingLibrary,
  chartType,
  query,
  pivotConfig,
}) {
  const [ready, setReady] = useState(false);
  const [loading, setLoading] = useState(true);
  const [embeds, setEmbeds] = useState({});

  useEffect(() => {
    document.body.addEventListener('cubejsChartReady', async () => {
      setReady(true);
      setEmbeds(await getEmbeds());
      setLoading(false);
    });
  }, []);

  useEffect(() => {
    if (ready) {
      dispatchChartEvent({
        pivotConfig,
        query,
        chartType,
        chartingLibrary,
      });
    }
  }, useDeepCompareMemoize([ready, pivotConfig, query, chartType]));

  return (
    <>
      <div id="root">
        {loading ? (
          <p>Loading...</p>
        ) : (
          <Helmet>
            {embeds.scripts.map(({ body }) =>
              body ? <script>{body}</script> : null
            )}
            {embeds.scripts.map(({ src }) =>
              src ? <script src={src} /> : null
            )}
          </Helmet>
        )}
      </div>
    </>
  );
}
