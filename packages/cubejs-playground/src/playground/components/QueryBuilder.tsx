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

export const QueryBuilder = memo(
  function QueryBuilder(props: QueryBuilderProps) {
    return (
      <PlaygroundWrapper
        identifier={props.identifier}
        token={props.token}
        apiUrl={props.apiUrl}
        tokenUpdater={props.tokenUpdater}
        playgroundContext={props.playgroundContext}
        onTokenPayloadChange={props.onTokenPayloadChange}
      >
        <QueryBuilderContainer {...props} />
      </PlaygroundWrapper>
    );
  },
  (prevProps, nextProps) => {
    return equals(prevProps, nextProps);
  }
);
