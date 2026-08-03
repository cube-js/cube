import { PivotConfig, Query } from '@cubejs-client/core';
import { useMemo } from 'react';

export interface VizardProps {
  apiToken: string | null;
  apiUrl: string | null;
  aiApiEnabled: boolean;
  query: Query;
  pivotConfig: PivotConfig;
}

export default function Vizard(props: VizardProps) {
  const { apiUrl, apiToken, query, pivotConfig } = props;

  const configHash = useMemo(() => {
    return encodeURIComponent(
      btoa(
        JSON.stringify({
          apiUrl,
          apiToken,
          query,
          pivotConfig,
        })
      )
    );
  }, [apiUrl, apiToken, JSON.stringify(query), JSON.stringify(pivotConfig)]);

  return (
    <iframe
      src={`/vizard/index.html#${configHash}`}
      style={{ height: 'calc(90vw - 80px)', border: 'none' }}
    ></iframe>
  );
}
