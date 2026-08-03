import { Alert, Block } from '@cube-dev/ui-kit';
import { useMemo } from 'react';

import { useQueryBuilderContext } from './context';
import { CopyButton } from './components/CopyButton';
import { TabPaneWithToolbar } from './components/TabPaneWithToolbar';
import { ScrollableCodeContainer } from './components/ScrollableCodeContainer';

export function QueryBuilderRest() {
  const { query, isQueryEmpty, queryHash } = useQueryBuilderContext();

  return useMemo(() => {
    const stringifiedQuery = JSON.stringify(query, null, 2);

    return !query || isQueryEmpty ? (
      <Block padding="1x">
        <Alert theme="note">Compose a query to see a JSON query.</Alert>
      </Block>
    ) : (
      <TabPaneWithToolbar
        actions={
          <CopyButton type="secondary" value={stringifiedQuery || ''}>
            Copy
          </CopyButton>
        }
      >
        <ScrollableCodeContainer value={stringifiedQuery || ''} />
      </TabPaneWithToolbar>
    );
  }, [queryHash, isQueryEmpty]);
}
