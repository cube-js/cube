import { PlaygroundWrapper } from './PlaygroundWrapper';
import { SecurityContextContextProps } from '../../components/SecurityContext/SecurityContextProvider';
import {
  PlaygroundQueryBuilderProps,
} from '../../components/PlaygroundQueryBuilder/components/PlaygroundQueryBuilder';
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
  Pick<SecurityContextContextProps, 'getToken'>;

export function QueryBuilder({ token, identifier, ...props }: QueryBuilderProps) {
  return (
    <PlaygroundWrapper identifier={identifier} getToken={props.getToken}>
      <QueryBuilderContainer token={token} {...props} />
    </PlaygroundWrapper>
  );
}
