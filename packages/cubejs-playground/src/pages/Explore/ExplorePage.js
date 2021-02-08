import { CubeProvider } from '@cubejs-client/react';
import { useEffect, useLayoutEffect, useMemo, useState } from 'react';
import { useHistory } from 'react-router';
import { fetch } from 'whatwg-fetch';

import DashboardSource from '../../DashboardSource';
import { useCubejsApi, useSecurityContext } from '../../hooks';
import PlaygroundQueryBuilder from '../../PlaygroundQueryBuilder';

export default function ExplorePage() {
  const { push, location } = useHistory();
  const { token } = useSecurityContext();
  
  const [apiUrl, setApiUrl] = useState(null);
  const [playgroundContext, setPlaygroundContext] = useState(null);

  const dashboardSource = useMemo(() => new DashboardSource(), []);
  const cubejsApi = useCubejsApi(apiUrl, token || playgroundContext?.cubejsToken);

  useEffect(() => {
    (async () => {
      const res = await fetch('/playground/context');
      const result = await res.json();

      setPlaygroundContext(result);
    })();
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

  if (!cubejsApi) {
    return null;
  }

  const params = new URLSearchParams(location.search);
  const query = (params.get('query') && JSON.parse(params.get('query'))) || {};

  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <PlaygroundQueryBuilder
        query={query}
        setQuery={(q) => push(`/build?query=${JSON.stringify(q)}`)}
        apiUrl={apiUrl}
        cubejsToken={token || playgroundContext.cubejsToken}
        dashboardSource={dashboardSource}
      />
    </CubeProvider>
  );
}
