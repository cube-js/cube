import { Alert, Block } from '@cube-dev/ui-kit';
import sqlFormatter from 'sql-formatter';

import { CubeSQLConverter } from './utils/cube-sql-converter';
import { useDeepMemo } from './hooks';
import { useQueryBuilderContext } from './context';
import { CopyButton } from './components/CopyButton';
import { TabPaneWithToolbar } from './components/TabPaneWithToolbar';
import { ScrollableCodeContainer } from './components/ScrollableCodeContainer';

export function QueryBuilderSQL() {
  const { queryHash, meta, isQueryEmpty, verificationError, dryRunResponse } =
    useQueryBuilderContext();

  return useDeepMemo(() => {
    // todo: fix types of normalizedQueries (e.g. order is always an array)
    const [query] = dryRunResponse?.normalizedQueries || [];

    if (isQueryEmpty) {
      return (
        <Block padding="1x">
          <Alert>Compose a query to see an SQL query.</Alert>
        </Block>
      );
    }

    if (!query) {
      return (
        <Block padding="1x">
          <Alert>Unable to generate an SQL query.</Alert>
        </Block>
      );
    }

    if (!isQueryEmpty && meta) {
      if (verificationError) {
        return (
          <Block padding="1x">
            <Alert theme="danger">{verificationError.toString()}</Alert>
          </Block>
        );
      }

      let sqlQuery = '';

      try {
        // @ts-ignore
        const converter = new CubeSQLConverter(query, meta.meta);

        sqlQuery = sqlFormatter.format(converter.buildQuery());
      } catch (e: any) {
        return (
          <Block padding="1x">
            <Alert theme="danger">{e.message}</Alert>
          </Block>
        );
      }

      return (
        <TabPaneWithToolbar
          actions={
            <CopyButton type="secondary" value={sqlQuery}>
              Copy
            </CopyButton>
          }
        >
          <ScrollableCodeContainer value={sqlQuery} />
        </TabPaneWithToolbar>
      );
    } else {
      return (
        <Block padding="1x">
          <Alert theme="note">Compose a query to see an SQL query.</Alert>
        </Block>
      );
    }
  }, [dryRunResponse, queryHash, verificationError, meta]);
}
