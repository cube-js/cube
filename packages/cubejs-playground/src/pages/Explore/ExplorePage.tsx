import { CubeProvider } from '@cubejs-client/react';
import { useEffect, useLayoutEffect, useMemo, useState } from 'react';
import { useHistory } from 'react-router';
import { fetch } from 'whatwg-fetch';

import DashboardSource from '../../DashboardSource';
import { useCubejsApi, useSecurityContext } from '../../hooks';
import PlaygroundQueryBuilder from '../../PlaygroundQueryBuilder';
import { LivePreviewContextProvider } from '../../components/LivePreviewContext/LivePreviewContextProvider';

type TPlaygroundContext = {
  apiUrl: string;
  cubejsToken: string;
  basePath: string;
  livePreview?: boolean;
};

type TLivePreviewContext = {
  apiUrl: string;
  token: string;
};

export function ExplorePage() {
  const dashboardSource = useMemo(() => new DashboardSource(), []);

  const { push, location } = useHistory();
  const { token } = useSecurityContext();

  const [schemaVersion, updateSchemaVersion] = useState<number>(0);
  const [apiUrl, setApiUrl] = useState<string | null>(null);
  const [
    playgroundContext,
    setPlaygroundContext,
  ] = useState<TPlaygroundContext | null>(null);
  const [
    livePreviewContext,
    setLivePreviewContext,
  ] = useState<TLivePreviewContext | null>(null);

  const currentToken =
    livePreviewContext?.token || token || playgroundContext?.cubejsToken;

  const cubejsApi = useCubejsApi(apiUrl, currentToken);

  const changeApiUrl = (apiUrl, basePath = '/cubejs-api') => {
    setApiUrl(`${apiUrl}${basePath}/v1`);
  };

  const fetchPlaygroundContext = async () => {
    const res = await fetch('/playground/context');
    const result = await res.json();
    setPlaygroundContext(result);
  };

  const handleChangeLivePreview = ({ token, apiUrl }) => {
    if (token && apiUrl) {
      setLivePreviewContext({
        token,
        apiUrl,
      });
      changeApiUrl(apiUrl, playgroundContext?.basePath);
    } else {
      setApiUrl(null);
      setLivePreviewContext(null);
    }

    updateSchemaVersion((value) => value + 1);
  };

  useEffect(() => {
    fetchPlaygroundContext();
  }, []);

  useLayoutEffect(() => {
    if (apiUrl && currentToken) {
      window['__cubejsPlayground'] = {
        ...window['__cubejsPlayground'],
        apiUrl,
        token: currentToken,
      };
    }
  }, [currentToken, apiUrl]);

  useLayoutEffect(() => {
    if (playgroundContext && livePreviewContext === null) {
      changeApiUrl(
        playgroundContext.apiUrl ||
          window.location.href.split('#')[0].replace(/\/$/, ''),
        playgroundContext.basePath
      );
    }
  }, [playgroundContext, livePreviewContext]);

  if (!cubejsApi || !apiUrl) {
    return null;
  }

  const params = new URLSearchParams(location.search);
  const query = JSON.parse(params.get('query') || '{}');

  return (
    <LivePreviewContextProvider
      disabled={
        playgroundContext?.livePreview == null || !playgroundContext.livePreview
      }
      onChange={handleChangeLivePreview}
    >
      <CubeProvider cubejsApi={cubejsApi}>
        <PlaygroundQueryBuilder
          defaultQuery={query}
          apiUrl={apiUrl}
          cubejsToken={currentToken as string}
          dashboardSource={dashboardSource}
          schemaVersion={schemaVersion}
          onVizStateChanged={({ query }) =>
            push(`/build?query=${JSON.stringify(query)}`)
          }
        />
      </CubeProvider>
    </LivePreviewContextProvider>
  );
}
