import { CubeProvider } from '@cubejs-client/react';
import cube, { PivotConfig, Query } from '@cubejs-client/core';
import { ChartViewer } from './ChartViewer.tsx';
import { extractHashConfig } from './config';
import { QueryRenderer } from './QueryRenderer.tsx';
import { ChartType, Config } from './types';

function App() {
  const { apiUrl, apiToken, query, pivotConfig, chartType } = extractHashConfig(
    {
      apiUrl: import.meta.env.VITE_CUBE_API_URL || '',
      apiToken: import.meta.env.VITE_CUBE_API_TOKEN || '',
      query: JSON.parse(import.meta.env.VITE_CUBE_QUERY || '{}') as Query,
      pivotConfig: JSON.parse(
        import.meta.env.VITE_CUBE_PIVOT_CONFIG || '{}'
      ) as PivotConfig,
      chartType: import.meta.env.VITE_CHART_TYPE as ChartType,
      websockets: import.meta.env.VITE_CUBE_API_USE_WEBSOCKETS === 'true',
      subscription: import.meta.env.VITE_CUBE_API_USE_SUBSCRIPTION === 'true',
    } as Config
  );

  const cubeApi = cube(apiToken, { apiUrl });

  return (
    <>
      <CubeProvider cubeApi={cubeApi}>
        <QueryRenderer query={query}>
          {({ resultSet, isLoading, error }) => {
            if (isLoading) {
              return <div>Loading...</div>;
            }

            if (error) {
              return <div>{error.toString()}</div>;
            }

            if (!resultSet) {
              return <div>NO RESULTS</div>;
            }

            return (
              <ChartViewer
                chartType={chartType}
                resultSet={resultSet}
                pivotConfig={pivotConfig}
              />
            );
          }}
        </QueryRenderer>
      </CubeProvider>
    </>
  );
}

export default App;
