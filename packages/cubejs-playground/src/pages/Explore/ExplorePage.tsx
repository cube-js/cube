import { CubeProvider } from '@cubejs-client/react';
import { useEffect, useLayoutEffect, useMemo, useState } from 'react';
import { useHistory } from 'react-router';
import { fetch } from 'whatwg-fetch';

import DashboardSource from '../../DashboardSource';
import { useCubejsApi, useSecurityContext, useLivePreviewContext } from '../../hooks';
import PlaygroundQueryBuilder from '../../PlaygroundQueryBuilder';
import LivePreviewContextProvider from '../../components/LivePreviewContext/LivePreviewContextProvider';

export default function ExplorePage() {
  const { push, location } = useHistory();
  const { token } = useSecurityContext();
  const { statusLivePreview, createTokenWithPayload } = useLivePreviewContext();

  const [apiUrl, setApiUrl] = useState<string | null>(null);
  const [playgroundContext, setPlaygroundContext] = useState<any>(null);

  const dashboardSource = useMemo(() => new DashboardSource(), []);
  const cubejsApi = useCubejsApi(
    apiUrl,
    token || playgroundContext?.cubejsToken
  );

  const fetchPlaygroundContext = async () => {
    const res = await fetch('/playground/context');
    const result = await res.json();
    setPlaygroundContext(result);
  };

  const handleChangeLivePreview = ({ token, apiUrl }) => {
    if (token && apiUrl) {
      setPlaygroundContext({
        ...playgroundContext,
        apiUrl,
        cubejsToken: token.token
      });
    } else {
      fetchPlaygroundContext();
    }
  }

  useEffect(() => {
    fetchPlaygroundContext();
  }, []);

  useLayoutEffect(() => {
    if (playgroundContext) {
      const basePath = playgroundContext.basePath || '/cubejs-api';
      let apiUrl =
        playgroundContext.apiUrl ||
        window.location.href.split('#')[0].replace(/\/$/, '');
      apiUrl = `${apiUrl}${basePath}/v1`;

      setApiUrl(apiUrl);

      window['__cubejsPlayground'] = {
        ...window['__cubejsPlayground'],
        apiUrl,
        token: token || playgroundContext.cubejsToken,
      };
    }
  }, [token, playgroundContext]);

  if (!cubejsApi || !apiUrl) {
    return null;
  }

  const params = new URLSearchParams(location.search);
  const query = (params.get('query') && JSON.parse(params.get('query') || '')) || {};

  return (
    <LivePreviewContextProvider disabled={!playgroundContext.livePreview} onChange={handleChangeLivePreview}>
      <CubeProvider cubejsApi={cubejsApi}>
        <PlaygroundQueryBuilder
          defaultQuery={query}
          apiUrl={apiUrl}
          cubejsToken={token || playgroundContext.cubejsToken}
          dashboardSource={dashboardSource}
          onVizStateChanged={({ query }) =>
            push(`/build?query=${JSON.stringify(query)}`)
          }
        />
      </CubeProvider>
    </LivePreviewContextProvider>
  );
}
