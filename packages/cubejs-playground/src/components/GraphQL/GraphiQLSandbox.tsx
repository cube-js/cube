import { Meta, Query } from '@cubejs-client/core';
import { createGraphiQLFetcher } from '@graphiql/toolkit';
import GraphiQL from 'graphiql';
import gqlParser from 'prettier/parser-graphql';
import { format } from 'prettier/standalone';
import { useMemo } from 'react';
import 'graphiql/graphiql.min.css';

import useDeepMemo from '../../hooks/deep-memo';
import { metaToTypes } from '../../utils';
import { CubeGraphQLConverter } from './CubeGraphQLConverter';

type GraphiQLSandboxProps = {
  query: Query
  meta: Meta
}

const fetcher = createGraphiQLFetcher({
  url: 'http://localhost:4000/cubejs-api/graphql'
});

export default function GraphiQLSandbox({ query, meta }: GraphiQLSandboxProps) {
  const types = useMemo(() => {
    return metaToTypes(meta);
  }, [meta]);
  
  const gqlQuery = useDeepMemo(() => {
    if (!types) {
      return '';
    }
    
    const converter = new CubeGraphQLConverter({ ...query }, types)
    
    return format(converter.convert(), {
      parser: 'graphql',
      plugins: [gqlParser]
    });
  }, [query, types])
  
  return (
    <div style={{ height: 1000 }}>
      <GraphiQL query={gqlQuery} fetcher={fetcher} />
    </div>
  );
}
