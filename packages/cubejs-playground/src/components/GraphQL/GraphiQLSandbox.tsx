import { Meta, Query } from '@cubejs-client/core';
import { createGraphiQLFetcher } from '@graphiql/toolkit';
import GraphiQL from 'graphiql';
import gqlParser from 'prettier/parser-graphql';
import { format } from 'prettier/standalone';
import styled from 'styled-components';
import { useMemo } from 'react';
import 'graphiql/graphiql.min.css';

import useDeepMemo from '../../hooks/deep-memo';
import { metaToTypes } from '../../utils';
import { CubeGraphQLConverter } from './CubeGraphQLConverter';
import { useSecurityContext, useToken } from '../../hooks';

const Wrapper = styled.div`
  margin-top: -15px;
  height: 400px;
  border-radius: 8px;
  overflow: hidden;

  .graphiql-container {
    .topBar {
      background: none;
      padding: 24px 0 24px;
    }

    .docExplorerShow {
      border-left: none;
      background: none;
    }

    .CodeMirror-scroll,
    .CodeMirror-lines {
      background: white;
    }
    
    .doc-explorer-title-bar {
      height: 50px;
    }
  }
`;

type GraphiQLSandboxProps = {
  apiUrl: string;
  query: Query;
  meta: Meta;
};

export default function GraphiQLSandbox({
  apiUrl,
  query,
  meta,
}: GraphiQLSandboxProps) {
  const { token: securityContextToken } = useSecurityContext();
  const playgroundToken = useToken();
  
  const token = securityContextToken || playgroundToken;

  const fetcher = useMemo(() => {
    return createGraphiQLFetcher({
      url: apiUrl.replace('/v1', '/graphql'),
      headers: token
        ? {
            authorization: token,
          }
        : {},
    });
  }, [apiUrl, token]);

  const types = useMemo(() => {
    return metaToTypes(meta);
  }, [meta]);

  const gqlQuery = useDeepMemo(() => {
    if (!types) {
      return '';
    }

    try {
      const converter = new CubeGraphQLConverter(query, types);
      const gqlQuery = converter.convert();

      try {
        return format(gqlQuery, {
          parser: 'graphql',
          plugins: [gqlParser],
        });
      } catch (_) {
        return gqlQuery;
      }
    } catch (error) {
      return `# ${error}\n`;
    }
  }, [query, types]);

  return (
    <Wrapper>
      <GraphiQL query={gqlQuery} fetcher={fetcher} />
    </Wrapper>
  );
}
