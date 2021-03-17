import { Query } from '@cubejs-client/core';
import { VizState } from '@cubejs-client/react';

import PlaygroundWrapper from '../PlaygroundWrapper';
import PlaygroundQueryBuilder from '../../PlaygroundQueryBuilder';

type TQueryBuilderProps = {
  apiUrl: string;
  token: string;
  defaultQuery?: Query;
  initialVizState?: VizState;
  getToken?: (payload: string) => Promise<string>;
  onVizStateChanged?: (vizState: VizState) => void;
};

export default function QueryBuilder({
  apiUrl,
  token,
  ...props
}: TQueryBuilderProps) {
  return (
    <PlaygroundWrapper apiUrl={apiUrl} token={token} getToken={props.getToken}>
      <PlaygroundQueryBuilder
        apiUrl={apiUrl}
        cubejsToken={token}
        initialVizState={{
          query: props.defaultQuery,
          ...props.initialVizState,
        }}
        onVizStateChanged={(vizState) => props.onVizStateChanged?.(vizState)}
      />
    </PlaygroundWrapper>
  );
}
