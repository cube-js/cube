import { PivotConfig, Query } from '@cubejs-client/core';
import { useCallback, useEffect, useState } from 'react';
import { Root, Grid } from '@cube-dev/ui-kit';

import { useAppFiles } from './app-files';
import { useAppName } from './app-name';
import { CodeViewer } from './CodeViewer';
import { Tabs } from './components/Tabs';
import { Preview } from './Preview';

import { Setup } from './Setup';
import { validateVisualParams } from './helpers';
import { AllParams, ChartType, Config } from './types';

const DEFAULT_VALUES: AllParams = {
  ...validateVisualParams({
    visualization: 'line',
  }),
  useWebSockets: true,
  useSubscription: true,
};

type Section = 'code' | 'preview';

if (!location.hash) {
  location.hash = encodeURIComponent(
    btoa(
      JSON.stringify({
        apiUrl: import.meta.env.VITE_CUBE_API_URL || '',
        apiToken: import.meta.env.VITE_CUBE_API_TOKEN || '',
        query: JSON.parse(import.meta.env.VITE_CUBE_QUERY || '{}'),
        pivotConfig: JSON.parse(import.meta.env.VITE_CUBE_PIVOT_CONFIG || '{}'),
      })
    )
  );
}

export interface VizardProps {
  apiToken: string | null;
  apiUrl: string | null;
  query: Query;
  pivotConfig: PivotConfig;
}

export function Vizard() {
  let params;

  try {
    params = JSON.parse(
      atob(decodeURIComponent(location.hash.slice(1)))
    ) as VizardProps;
  } catch (e) {
    throw new Error('Invalid params');
  }

  const { apiUrl, apiToken, query, pivotConfig } = params;

  const [section, setSection] = useState<Section>('code');
  const [genParams, setGenParams] = useState<AllParams>(DEFAULT_VALUES);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(false);
  }, []);

  const appName = useAppName({
    visualization: genParams.visualization,
    library: genParams.library,
    language: genParams.language,
    framework: genParams.framework,
  });

  const config = {
    apiUrl,
    apiToken,
    query,
    pivotConfig,
    chartType: genParams.visualization as ChartType,
    useWebSockets: genParams.useWebSockets,
    useSubscription: genParams.useSubscription,
  } as unknown as Config;

  const appFiles = useAppFiles({
    appName,
    apiUrl,
    apiToken,
    query,
    pivotConfig,
    chartType: genParams.visualization as ChartType,
    useWebSockets: genParams.useWebSockets,
    useSubscription: genParams.useSubscription,
  });

  const onParamsChange = useCallback((data: AllParams) => {
    setGenParams(data);
  }, []);

  if (loading) {
    return null;
  }

  return (
    <Root
      publicUrl="/vizard"
      styles={{
        display: 'grid',
        gridTemplateColumns: '1fr 300px',
        width: '100vw',
        height: '100vh',
      }}
    >
      <Grid
        styles={{
          width: '100%',
          height: 'max 100vh',
          gridAutoFlow: 'column',
          gridColumns: '1fr',
          gridRows: 'auto 1fr',
          border: 'right',
        }}
      >
        <Grid>
          <Tabs
            activeKey={section}
            onChange={(key: string) => setSection(key as Section)}
          >
            <Tabs.Tab id="code" title="Code" />
            <Tabs.Tab id="preview" title="Preview" />
          </Tabs>
        </Grid>
        {section === 'code' ? (
          <CodeViewer appName={appName} files={appFiles} />
        ) : null}
        {section === 'preview' ? (
          <Preview appName={appName} config={config} />
        ) : null}
      </Grid>
      <Setup
        apiUrl={apiUrl}
        apiToken={apiToken}
        query={query}
        data={DEFAULT_VALUES}
        onChange={onParamsChange}
      />
    </Root>
  );
}
