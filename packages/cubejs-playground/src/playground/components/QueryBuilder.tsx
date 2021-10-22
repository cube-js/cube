import equals from 'fast-deep-equal';
import { memo } from 'react';
import { PlaygroundContext } from '../../components/AppContext';

import { PlaygroundWrapper } from './PlaygroundWrapper';
import {
  SecurityContextProps,
  SecurityContextProviderProps,
} from '../../components/SecurityContext/SecurityContextProvider';
import { PlaygroundQueryBuilderProps } from '../../components/PlaygroundQueryBuilder/components/PlaygroundQueryBuilder';
import { QueryBuilderContainer } from '../../components/PlaygroundQueryBuilder/QueryBuilderContainer';
import { QueryTabsProps } from '../../components/QueryTabs/QueryTabs';

type QueryBuilderProps = {
  token: string;
  identifier?: string;
  playgroundContext?: Partial<PlaygroundContext>;
} & Pick<
  PlaygroundQueryBuilderProps,
  | 'apiUrl'
  | 'defaultQuery'
  | 'initialVizState'
  | 'schemaVersion'
  | 'onVizStateChanged'
  | 'onSchemaChange'
  | 'extra'
> &
  Pick<SecurityContextProps, 'onTokenPayloadChange'> &
  Pick<SecurityContextProviderProps, 'tokenUpdater'> &
  Pick<QueryTabsProps, 'onTabChange'>;

function QueryBuilderComponent({
  token,
  identifier,
  ...props
}: QueryBuilderProps) {
  return (
    <PlaygroundWrapper
      identifier={identifier}
      token={token}
      tokenUpdater={props.tokenUpdater}
      playgroundContext={props.playgroundContext}
      onTokenPayloadChange={props.onTokenPayloadChange}
    >
      <QueryBuilderContainer token={token} {...props} />
    </PlaygroundWrapper>
  );
}

export const QueryBuilder = memo(
  QueryBuilderComponent,
  (prevProps, nextProps) => {
    return equals(prevProps, nextProps);
  }
);
