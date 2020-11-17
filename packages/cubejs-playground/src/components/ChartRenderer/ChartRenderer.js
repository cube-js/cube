import React, { useEffect, useState } from 'react';
import { Helmet } from 'react-helmet';

const buildPath = '/chart-renderers/build';

async function getEmbeds(framework = 'react') {
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

function updateState(detail) {
  const event = new CustomEvent('cubejs', {
    detail,
  });
  event.initEvent('cubejs', true);
  document.body.dispatchEvent(event);
}

export default function ChartRenderer({
  framework,
  chartingLibrary,
  chartType,
  query,
  pivotConfig,
}) {
  const [loading, setLoading] = useState(true);
  const [embeds, setEmbeds] = useState({});

  useEffect(() => {
    getEmbeds().then((embeds) => {
      console.log('???', embeds);
      setEmbeds(embeds);
      setLoading(false);
    });
  }, []);

  useEffect(() => {
    console.log('triggerEvent', {
      pivotConfig,
      query,
      chartType,
      chartingLibrary,
    });
    updateState({
      pivotConfig,
      query,
      chartType,
      chartingLibrary,
    });
  }, [pivotConfig, query, chartType]);

  return (
    <>
      <button
        onClick={() => {
          const event = new CustomEvent('cubejs', {
            detail: {
              query: {
                yo: Math.random(),
              },
            },
          });
          event.initEvent('cubejs', true);
          document.body.dispatchEvent(event);
        }}
      >
        Change Chart Type
      </button>
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
