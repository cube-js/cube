import {useCubeQuery} from '@cubejs-client/react';
import {ReactNode} from 'react';
import {Query, ResultSet} from '@cubejs-client/core';

interface QueryRendererProps {
  query?: Query;
  children?: (props: {
    resultSet: ResultSet | null,
    isLoading: boolean,
    error: Error | null,
    refetch: () => void
  }) => ReactNode;
}

export function QueryRenderer(props: QueryRendererProps) {
  const {children, query} = props;
  const {resultSet, isLoading, error, refetch} = useCubeQuery(query || {}, {
    skip: !query,
    resetResultSetOnChange: true,
  });

  return children?.({resultSet, isLoading, error, refetch}) || null;
}
