import { CubeProvider } from '@cubejs-client/react';
import cube, { PivotConfig, Query } from '@cubejs-client/core';
import WebSocketTransport from '@cubejs-client/ws-transport';
import { QueryRenderer } from './QueryRenderer';
import { ChartViewer } from './ChartViewer';

export type ChartType = 'area' | 'bar' | 'doughnut' | 'line' | 'pie';

function App() {
  const apiUrl = import.meta.env.VITE_CUBE_API_URL || '';
  const apiToken = import.meta.env.VITE_CUBE_API_TOKEN || '';
  const apiUseWebSockets =
    import.meta.env.VITE_CUBE_API_USE_WEBSOCKETS === 'true' || false;
  const apiUseSubscription =
    import.meta.env.VITE_CUBE_API_USE_SUBSCRIPTION === 'true' || false;
  const query = JSON.parse(import.meta.env.VITE_CUBE_QUERY || '{}') as Query;
  const pivotConfig = JSON.parse(
    import.meta.env.VITE_CUBE_PIVOT_CONFIG || '{}'
  ) as PivotConfig;
  const chartType = import.meta.env.VITE_CHART_TYPE || ('line' as ChartType);

  let transport = undefined;

  if (apiUseWebSockets) {
    transport = new WebSocketTransport({ authorization: apiToken, apiUrl });
  }

  const cubeApi = cube(apiToken, { apiUrl, transport });

  return (
    <>
      <CubeProvider cubeApi={cubeApi}>
        <QueryRenderer query={query} subscribe={apiUseSubscription}>
          {({ resultSet }) => (
            <ChartViewer
              chartType={chartType}
              resultSet={resultSet}
              pivotConfig={pivotConfig}
            />
          )}
        </QueryRenderer>
      </CubeProvider>
    </>
  );
}

export default App;
