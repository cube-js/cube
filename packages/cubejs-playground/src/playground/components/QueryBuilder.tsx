import { useLayoutEffect } from 'react';
import { CubeProvider } from '@cubejs-client/react';

import PlaygroundWrapper from '../PlaygroundWrapper';
import PlaygroundQueryBuilder, {
  TPlaygroundQueryBuilderProps,
} from '../../PlaygroundQueryBuilder';
import { TSecurityContextContextProps } from '../../components/SecurityContext/SecurityContextProvider';
import { useCubejsApi, useSecurityContext } from '../../hooks';

type TQueryBuilderProps = {
  apiUrl: string;
  token: string;
  tokenKey?: string;
} & Pick<
  TPlaygroundQueryBuilderProps,
  | 'defaultQuery'
  | 'initialVizState'
  | 'schemaVersion'
  | 'onVizStateChanged'
  | 'onSchemaChange'
> &
  Pick<TSecurityContextContextProps, 'getToken'>;

export function QueryBuilder({ apiUrl, token, ...props }: TQueryBuilderProps) {
  return (
    <PlaygroundWrapper tokenKey={props.tokenKey} getToken={props.getToken}>
      <QueryBuilderContainer apiUrl={apiUrl} token={token} {...props} />
    </PlaygroundWrapper>
  );
}

type TQueryBuilderContainerProps = Omit<
  TQueryBuilderProps,
  'tokenKey' | 'getToken'
>;

function QueryBuilderContainer({
  apiUrl,
  token,
  ...props
}: TQueryBuilderContainerProps) {
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
        initialVizState={{
          query: props.defaultQuery,
          ...props.initialVizState,
        }}
        schemaVersion={props.schemaVersion}
        onVizStateChanged={props.onVizStateChanged}
        onSchemaChange={props.onSchemaChange}
      />
    </CubeProvider>
  );
}
