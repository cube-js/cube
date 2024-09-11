import { useEffect, useMemo } from 'react';
import cube, { Query } from '@cubejs-client/core';
import { Alert, Block, Card, PrismCode, Title } from '@cube-dev/ui-kit';

import { useLocalStorage } from './hooks';
import { QueryBuilderProps } from './types';
import { QueryBuilderContext } from './context';
import { useQueryBuilder } from './hooks/query-builder';
import { QueryBuilderInternals } from './QueryBuilderInternals';
import { useCommitPress } from './utils/use-commit-press';

export function QueryBuilder(props: Omit<QueryBuilderProps, 'apiUrl'> & { apiUrl: string | null }) {
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
    VizardComponent,
    RequestStatusComponent,
    openSqlRunner,
  } = props;

  const cubeApi = useMemo(() => {
    return apiUrl && apiToken && apiToken !== 'undefined'
      ? cube(apiToken, {
          apiUrl,
        })
      : undefined;
  }, [apiUrl, apiToken]);

  const [storedTimezones] = useLocalStorage<string[]>('QueryBuilder:timezones', []);

  function queryValidator(query: Query) {
    const queryCopy = JSON.parse(JSON.stringify(query));

    if (typeof queryCopy.limit !== 'number' || queryCopy.limit < 1 || queryCopy.limit > 50_000) {
      queryCopy.limit = 5_000;
    }

    /**
     * @TODO: Add support for offset
     */
    delete queryCopy.offset;

    if (!queryCopy.timezone && storedTimezones[0]) {
      queryCopy.timezone = storedTimezones[0];
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
    tracking,
    queryValidator,
  });

  useEffect(() => {
    if (defaultQuery && shouldRunDefaultQuery && meta) {
      void runQuery();
    }
  }, [shouldRunDefaultQuery, meta]);

  useCommitPress(() => {
    return runQuery();
  }, true);

  return apiToken && cubeApi && apiUrl ? (
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
        ...otherProps,
      }}
    >
      {!meta ? (
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
      ) : (
        <QueryBuilderInternals />
      )}
    </QueryBuilderContext.Provider>
  ) : null;
}
