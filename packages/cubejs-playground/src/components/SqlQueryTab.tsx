import { useEffect } from 'react';
import { Query } from '@cubejs-client/core';
import { QueryRenderer } from '@cubejs-client/react';
import { format } from 'sql-formatter';

import PrismCode from '../PrismCode';
import { FatalError } from '../atoms';

type SqlEmitterOnChangeProps = {
  sql?: string;
  loading: boolean;
};

type SqlEmitterProps = {
  loading: boolean;
  sql?: string;
  onChange: (props: SqlEmitterOnChangeProps) => void;
};

function SqlEmitter({ sql, loading, onChange }: SqlEmitterProps) {
  useEffect(() => {
    onChange({ sql, loading });
  }, [sql, loading]);

  return null;
}

type SqlQueryTabProps = {
  query: Query;
  onChange: (sql: { loading: boolean; value?: string }) => void;
};

export default function SqlQueryTab({ query, onChange }: SqlQueryTabProps) {
  return (
    <QueryRenderer
      loadSql="only"
      query={query}
      render={({ sqlQuery, loadingState, error }) => {
        if (error) {
          return <FatalError error={error} />;
        }

        // in the case of a compareDateRange query the SQL will be the same
        const [query] = Array.isArray(sqlQuery) ? sqlQuery : [sqlQuery];
        const value = query && format(query.sql());

        return (
          <>
            <PrismCode code={value} />
            <SqlEmitter
              loading={loadingState.isLoading}
              sql={value}
              onChange={({ sql, loading }) => {
                onChange({
                  loading,
                  value: sql,
                });
              }}
            />
          </>
        );
      }}
    />
  );
}
