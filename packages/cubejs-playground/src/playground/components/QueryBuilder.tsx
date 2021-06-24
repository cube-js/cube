import { PlaygroundWrapper } from './PlaygroundWrapper';
import {
  SecurityContextProps,
  SecurityContextProviderProps,
} from '../../components/SecurityContext/SecurityContextProvider';
import { PlaygroundQueryBuilderProps } from '../../components/PlaygroundQueryBuilder/components/PlaygroundQueryBuilder';
import { QueryBuilderContainer } from '../../components/PlaygroundQueryBuilder/QueryBuilderContainer';

type QueryBuilderProps = {
  token: string;
  identifier?: string;
} & Pick<
  PlaygroundQueryBuilderProps,
  | 'apiUrl'
  | 'defaultQuery'
  | 'initialVizState'
  | 'schemaVersion'
  | 'onVizStateChanged'
  | 'onSchemaChange'
> &
  Pick<SecurityContextProps, 'onTokenPayloadChange'> &
  Pick<SecurityContextProviderProps, 'tokenUpdater'>;

export function QueryBuilder({
  token,
  identifier,
  ...props
}: QueryBuilderProps) {
  return (
    <PlaygroundWrapper
      identifier={identifier}
      token={token}
      tokenUpdater={props.tokenUpdater}
      onTokenPayloadChange={props.onTokenPayloadChange}
    >
      <QueryBuilderContainer token={token} {...props} />
    </PlaygroundWrapper>
  );
}
