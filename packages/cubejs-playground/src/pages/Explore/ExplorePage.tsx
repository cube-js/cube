import { useMemo } from 'react';
import { useHistory } from 'react-router';

import { QueryBuilderContainer } from '../../components/PlaygroundQueryBuilder/QueryBuilderContainer';
import DashboardSource from '../../DashboardSource';
import {
  useAppContext,
  useDeepEffect,
  useLivePreviewContext,
  useSecurityContext,
} from '../../hooks';

export function buildApiUrl(
  apiUrl: string,
  basePath: string = '/cubejs-api'
): string {
  return `${apiUrl}${basePath}/v1`;
}

export function ExplorePage() {
  const { push } = useHistory();
  const dashboardSource = useMemo(() => new DashboardSource(), []);
  const livePreviewContext = useLivePreviewContext();

  const { apiUrl, token, schemaVersion, setContext, playgroundContext } =
    useAppContext();
  const { token: securityContextToken } = useSecurityContext();

  const { basePath, cubejsToken } = playgroundContext;

  useDeepEffect(() => {
    if (
      basePath &&
      (livePreviewContext === null ||
        !livePreviewContext.statusLivePreview.active)
    ) {
      setContext({
        token: securityContextToken || cubejsToken,
        apiUrl: buildApiUrl(
          window.location.href.split('#')[0].replace(/\/$/, ''),
          basePath
        ),
      });
    } else if (
      livePreviewContext?.statusLivePreview.active &&
      livePreviewContext.credentials
    ) {
      const { token, apiUrl } = livePreviewContext.credentials;
      setContext({
        apiUrl: buildApiUrl(apiUrl, basePath),
        token,
      });
    }
  }, [basePath, livePreviewContext, cubejsToken, securityContextToken]);

  function setQueryParam({ query }: { query?: Object}) {
    if (query) {
      push({ search: `?query=${JSON.stringify(query)}` });
    }
  }

  return (
    <QueryBuilderContainer
      apiUrl={apiUrl}
      token={token}
      schemaVersion={schemaVersion}
      dashboardSource={dashboardSource}
      onVizStateChanged={setQueryParam}
      onTabChange={setQueryParam}
    />
  );
}
