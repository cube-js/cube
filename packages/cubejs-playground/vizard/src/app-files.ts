import { useMemo } from 'react';

import apps from './apps.json';
import { ApiParams, FileSystemTree, Config, ConnectionParams } from './types';

interface UseAppFilesProps extends ConnectionParams, ApiParams, Config {
  appName: string;
}

export function useAppFiles({
  appName,
  useSubscription,
  useWebSockets,
  chartType,
  apiUrl,
  apiToken,
  query,
  pivotConfig,
}: UseAppFilesProps): FileSystemTree {
  return useMemo(() => {
    const appFiles = apps[
      appName as keyof typeof apps
    ] as unknown as FileSystemTree;

    appFiles['.env.local'] = {
      file: {
        contents: `VITE_CUBE_API_URL=${apiUrl}\nVITE_CUBE_API_TOKEN=${apiToken}\nVITE_CUBE_QUERY=${JSON.stringify(query)}\nVITE_CUBE_PIVOT_CONFIG=${JSON.stringify(pivotConfig)}\nVITE_CHART_TYPE=${chartType}\nVITE_CUBE_API_USE_WEBSOCKETS=${useWebSockets ? 'true' : 'false'}\nVITE_CUBE_API_USE_SUBSCRIPTION=${useSubscription ? 'true' : 'false'}`,
      },
    };

    return appFiles as FileSystemTree;
  }, [
    appName,
    chartType,
    apiToken,
    apiUrl,
    useSubscription,
    useWebSockets,
    JSON.stringify(query),
    JSON.stringify(pivotConfig),
  ]);
}
