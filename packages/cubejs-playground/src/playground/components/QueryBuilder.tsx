import { Query } from '@cubejs-client/core';
import { SchemaChangeProps, VizState } from '@cubejs-client/react';

import PlaygroundWrapper from '../PlaygroundWrapper';
import PlaygroundQueryBuilder from '../../PlaygroundQueryBuilder';

type TQueryBuilderProps = {
  apiUrl: string;
  token: string;
  defaultQuery?: Query;
  initialVizState?: VizState;
  getToken?: (payload: string) => Promise<string>;
  schemaVersion?: number;
  onVizStateChanged?: (vizState: VizState) => void;
  onSchemaChange?: (props: SchemaChangeProps) => void;
};

export function QueryBuilder({
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
        schemaVersion={props.schemaVersion}
        onVizStateChanged={(vizState) => props.onVizStateChanged?.(vizState)}
        onSchemaChange={props.onSchemaChange}
      />
    </PlaygroundWrapper>
  );
}
