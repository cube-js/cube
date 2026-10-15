import { Alert, Block, Card, PrismCode, Title } from '@cube-dev/ui-kit';
import cube, { Query } from '@cubejs-client/core';
import { useEffect, useMemo, ReactNode } from 'react';

import { QueryBuilderContext } from './context';
import { useLocalStorage } from './hooks';
import { useQueryBuilder } from './hooks/query-builder';
import { QueryBuilderInternals } from './QueryBuilderInternals';
import { QueryBuilderProps } from './types';
import { useCommitPress } from './utils/use-commit-press';

export function QueryBuilder(
  props: Omit<QueryBuilderProps, 'apiUrl'> & {
    displayPrivateItems?: boolean;
    apiUrl: string | null;
    children?: ReactNode;
  }
) {
  const {
    apiUrl,
    apiToken,
    defaultChartType,
    defaultPivotConfig,
    onQueryChange,
    defaultQuery,
    shouldRunDefaultQuery,
    schemaVersion,
    tracking,
    isApiBlocked,
    apiVersion,
    memberViewType,
    VizardComponent,
    RequestStatusComponent,
    openSqlRunner,
    displayPrivateItems,
    disableSidebarResizing,
  } = props;

  const cubeApi = useMemo(() => (apiUrl && apiToken && apiToken !== 'undefined'
    ? cube(apiToken, {
      apiUrl,
    })
    : undefined),
  [apiUrl, apiToken]);

  const [storedTimezones] = useLocalStorage<string[]>('QueryBuilder:timezones', []);

  function queryValidator(query: Query) {
    const queryCopy = JSON.parse(JSON.stringify(query));

    // add the last stored timezone if the query is empty
    const [lastStoredTimezone] = storedTimezones;

    if (JSON.stringify(queryCopy) === '{}' && lastStoredTimezone) {
      queryCopy.timezone = lastStoredTimezone;
    }

    return queryCopy;
  }

  const {
    runQuery,
    cubes,
    isCubeJoined,
    usedCubes,
    getCubeByName,
    meta,
    loadMeta,
    metaError,
    richMetaError,
    selectCube,
    selectedCube,
    ...otherProps
  } = useQueryBuilder({
    cubeApi,
    defaultQuery,
    defaultChartType,
    defaultPivotConfig,
    schemaVersion,
    onQueryChange,
    memberViewType,
    tracking,
    queryValidator,
    displayPrivateItems,
  });

  useEffect(() => {
    if (defaultQuery && shouldRunDefaultQuery && meta) {
      runQuery();
    }
  }, [shouldRunDefaultQuery, meta]);

  useCommitPress(() => runQuery(),
    true);

  if (!apiToken || !cubeApi || !apiUrl) {
    return null;
  }

  return (
    <QueryBuilderContext.Provider
      value={{
        runQuery,
        cubes,
        isCubeJoined,
        meta,
        loadMeta,
        metaError,
        richMetaError,
        selectedCube,
        selectCube,
        usedCubes,
        getCubeByName,
        tracking,
        isApiBlocked,
        apiToken,
        apiUrl,
        apiVersion,
        VizardComponent,
        RequestStatusComponent,
        openSqlRunner,
        disableSidebarResizing,
        ...otherProps,
      }}
    >
      {!meta && (
        <Block flexGrow={1} padding="2x">
          {!metaError ? (
            <Card>Loading meta information...</Card>
          ) : (
            <Alert theme="danger">
              <Title level={5}>Unable to load meta data.</Title>
              <PrismCode code={metaError} />
            </Alert>
          )}
        </Block>
      )}
      {!!meta && (props.children || <QueryBuilderInternals />)}
    </QueryBuilderContext.Provider>
  );
}
