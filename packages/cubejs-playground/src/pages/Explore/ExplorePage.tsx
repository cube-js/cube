import { useEffect, useMemo, useState } from 'react';
import { useHistory } from 'react-router';

import { useAppContext, useSecurityContext } from '../../hooks';
import { QueryBuilderContainer } from '../../components/PlaygroundQueryBuilder/QueryBuilderContainer';
import { LivePreviewContextProvider } from '../../components/LivePreviewContext/LivePreviewContextProvider';
import DashboardSource from '../../DashboardSource';

type LivePreviewContext = {
  apiUrl: string;
  token: string;
};

export function buildApiUrl(
  apiUrl: string,
  basePath: string = '/cubejs-api'
): string {
  return `${apiUrl}${basePath}/v1`;
}

export function ExplorePage() {
  const { push } = useHistory();

  const dashboardSource = useMemo(() => new DashboardSource(), []);

  const { setContext, playgroundContext } = useAppContext();
  const { token } = useSecurityContext();
  const [livePreviewContext, setLivePreviewContext] =
    useState<LivePreviewContext | null>(null);

  const [schemaVersion, updateSchemaVersion] = useState<number>(0);
  const [apiUrl, setApiUrl] = useState<string>('');

  useEffect(() => {
    if (playgroundContext && livePreviewContext === null) {
      setDefaultApiUrl();
    }
  }, [playgroundContext, livePreviewContext]);

  function setDefaultApiUrl() {
    setApiUrl(
      buildApiUrl(
        window.location.href.split('#')[0].replace(/\/$/, ''),
        playgroundContext?.basePath
      )
    );
  }

  function handleChangeLivePreview({
    token,
    apiUrl,
  }: {
    token: string | null;
    apiUrl: string | null;
  }) {
    if (token && apiUrl) {
      setLivePreviewContext({
        token,
        apiUrl,
      });
      setApiUrl(buildApiUrl(apiUrl, playgroundContext?.basePath));
    } else {
      setLivePreviewContext(null);
      setDefaultApiUrl();
    }

    updateSchemaVersion((value) => value + 1);
  }

  const currentToken =
    livePreviewContext?.token || token || playgroundContext?.cubejsToken;

  useEffect(() => {
    setContext({ token: currentToken });
  }, [currentToken])

  return (
    <LivePreviewContextProvider
      disabled={
        playgroundContext?.livePreview == null || !playgroundContext.livePreview
      }
      onChange={handleChangeLivePreview}
    >
      <QueryBuilderContainer
        apiUrl={apiUrl}
        token={currentToken}
        schemaVersion={schemaVersion}
        dashboardSource={dashboardSource}
        onVizStateChanged={({ query }) => {
          push(`/build?query=${JSON.stringify(query)}`);
        }}
      />
    </LivePreviewContextProvider>
  );
}
