import React, { useEffect, useState } from 'react';
import { Helmet } from 'react-helmet';
import { dispatchChartEvent } from '../../PlaygroundQueryBuilder';
import useDeepCompareMemoize from '../../hooks/deep-compare-memoize';

const buildPath = '/chart-renderers/react';

async function getEmbeds() {
  try {
    const html = await (await fetch(`${buildPath}/index.html`)).text();
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
  } catch (error) {
    console.error(error);
    return {};
  }
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
    getEmbeds().then((embeds) => {
      setEmbeds(embeds);
      setLoading(false);
    });

    document.body.addEventListener('cubejsChartReady', async () => {
      console.log('READY!');
      setReady(true);
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
