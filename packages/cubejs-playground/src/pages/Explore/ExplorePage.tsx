import { CubeProvider } from '@cubejs-client/react';
import { useEffect, useLayoutEffect, useState } from 'react';

import { useCubejsApi, useSecurityContext } from '../../hooks';
import { QueryBuilderContainer } from '../../components/PlaygroundQueryBuilder/QueryBuilderContainer';
import { LivePreviewContextProvider } from '../../components/LivePreviewContext/LivePreviewContextProvider';
import { useAppContext } from '../../components/AppContext';

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
  const { playgroundContext } = useAppContext();
  const { token } = useSecurityContext();
  const [livePreviewContext, setLivePreviewContext] =
    useState<LivePreviewContext | null>(null);

  const [schemaVersion, updateSchemaVersion] = useState<number>(0);
  const [apiUrl, setApiUrl] = useState<string | null>(null);

  useEffect(() => {
    if (playgroundContext && livePreviewContext === null) {
      setDefaultApiUrl();
    }
  }, [playgroundContext, livePreviewContext]);

  function setDefaultApiUrl() {
    setApiUrl(
      buildApiUrl(
        playgroundContext?.apiUrl ||
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

  useLayoutEffect(() => {
    if (apiUrl && currentToken) {
      window['__cubejsPlayground'] = {
        ...window['__cubejsPlayground'],
        apiUrl,
        token: currentToken,
      };
    }
  }, [currentToken, apiUrl]);

  const cubejsApi = useCubejsApi(apiUrl, currentToken);

  if (!cubejsApi || !apiUrl || !currentToken) {
    return null;
  }

  return (
    <LivePreviewContextProvider
      disabled={
        playgroundContext?.livePreview == null || !playgroundContext.livePreview
      }
      onChange={handleChangeLivePreview}
    >
      <CubeProvider cubejsApi={cubejsApi}>
        <QueryBuilderContainer
          apiUrl={apiUrl}
          token={currentToken}
          schemaVersion={schemaVersion}
        />
      </CubeProvider>
    </LivePreviewContextProvider>
  );
}
