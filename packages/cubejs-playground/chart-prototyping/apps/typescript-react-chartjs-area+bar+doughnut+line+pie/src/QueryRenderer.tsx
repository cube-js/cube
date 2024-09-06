import { ReactNode } from 'react';
import { Query, ResultSet } from '@cubejs-client/core';
import { useCubeQuery } from '@cubejs-client/react';

interface QueryRendererProps {
  query?: Query;
  subscribe: boolean;
  children?: (props: { resultSet: ResultSet }) => ReactNode;
}

export function QueryRenderer(props: QueryRendererProps) {
  const { children, query, subscribe } = props;
  const { resultSet, isLoading, error } = useCubeQuery(query, { subscribe });

  if (isLoading) {
    return <>Loading...</>;
  }

  if (error) {
    return <>{error.toString()}</>;
  }

  if (!resultSet) {
    return <>Empty result set</>;
  }

  return children?.({ resultSet }) || null;
}