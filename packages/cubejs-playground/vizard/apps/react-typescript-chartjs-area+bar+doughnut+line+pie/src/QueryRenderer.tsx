import { ReactNode } from 'react';
import { useCubeQuery } from '@cubejs-client/react';
import { Query, ResultSet } from '@cubejs-client/core';

interface QueryRendererProps {
  query?: Query;
  children?: (props: {
    resultSet: ResultSet;
  }) => ReactNode;
  subscribe?: boolean;
}

export function QueryRenderer(props: QueryRendererProps) {
  const { children, query, subscribe } = props;
  const { resultSet, isLoading, error } = useCubeQuery(query ?? {}, { subscribe, skip: !query });

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
