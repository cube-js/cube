import PlaygroundWrapper from '../PlaygroundWrapper';
import PlaygroundQueryBuilder, {
  TPlaygroundQueryBuilderProps,
} from '../../PlaygroundQueryBuilder';
import { TSecurityContextContextProps } from '../../components/SecurityContext/SecurityContextProvider';

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
    <PlaygroundWrapper
      apiUrl={apiUrl}
      token={token}
      tokenKey={props.tokenKey}
      getToken={props.getToken}
    >
      <PlaygroundQueryBuilder
        apiUrl={apiUrl}
        cubejsToken={token}
        initialVizState={{
          query: props.defaultQuery,
          ...props.initialVizState,
        }}
        schemaVersion={props.schemaVersion}
        onVizStateChanged={props.onVizStateChanged}
        onSchemaChange={props.onSchemaChange}
      />
    </PlaygroundWrapper>
  );
}
