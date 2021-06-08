import { useLayoutEffect } from 'react';
import { CubeProvider } from '@cubejs-client/react';

import PlaygroundWrapper from '../PlaygroundWrapper';
import { TSecurityContextContextProps } from '../../components/SecurityContext/SecurityContextProvider';
import { useCubejsApi, useSecurityContext } from '../../hooks';
import {
  PlaygroundQueryBuilder,
  TPlaygroundQueryBuilderProps,
} from '../../components/PlaygroundQueryBuilder/components/PlaygroundQueryBuilder';

type QueryBuilderProps = {
  apiUrl: string;
  token: string;
  tokenKey?: string;
} & Pick<
  TPlaygroundQueryBuilderProps,
  | 'defaultQuery'
  | 'initialVizState'
  | 'schemaVersion'
  | 'queryVersion'
  | 'onVizStateChanged'
  | 'onSchemaChange'
> &
  Pick<TSecurityContextContextProps, 'getToken'>;

export function QueryBuilder({ apiUrl, token, ...props }: QueryBuilderProps) {
  return (
    <PlaygroundWrapper tokenKey={props.tokenKey} getToken={props.getToken}>
      <QueryBuilderContainer apiUrl={apiUrl} token={token} {...props} />
    </PlaygroundWrapper>
  );
}

type QueryBuilderContainerProps = Omit<
  QueryBuilderProps,
  'tokenKey' | 'getToken'
>;

function QueryBuilderContainer({
  apiUrl,
  token,
  ...props
}: QueryBuilderContainerProps) {
  const { token: securityContextToken } = useSecurityContext();
  const currentToken = securityContextToken || token;
  const cubejsApi = useCubejsApi(apiUrl, currentToken);

  useLayoutEffect(() => {
    if (apiUrl && currentToken) {
      // @ts-ignore
      window.__cubejsPlayground = {
        // @ts-ignore
        ...window.__cubejsPlayground,
        apiUrl,
        token: currentToken,
      };
    }
  }, [apiUrl, currentToken]);

  if (!cubejsApi) {
    return null;
  }

  return (
    <CubeProvider cubejsApi={cubejsApi}>
      <PlaygroundQueryBuilder
        apiUrl={apiUrl}
        cubejsToken={currentToken}
        // todo: !!!
        queryId="???f"
        initialVizState={{
          query: props.defaultQuery,
          ...props.initialVizState,
        }}
        schemaVersion={props.schemaVersion}
        queryVersion={props.queryVersion}
        onVizStateChanged={props.onVizStateChanged}
        onSchemaChange={props.onSchemaChange}
      />
    </CubeProvider>
  );
}
