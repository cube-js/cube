import { PlayCircleOutlined } from '@ant-design/icons';
import { ApolloClient, ApolloLink, gql, HttpLink, InMemoryCache, useQuery } from '@apollo/client';
import { RetryLink } from '@apollo/client/link/retry';
import { Alert, Block, Button, Grid, LockIcon, tasty, TooltipProvider } from '@cube-dev/ui-kit';
import { useEffect, useMemo, useState } from 'react';

import { useQueryBuilderContext } from './context';
import { convertJsonQueryToGraphQL } from './utils';
import { CopyButton } from './components/CopyButton';
import { TabPaneWithToolbar } from './components/TabPaneWithToolbar';
import { ScrollableCodeContainer } from './components/ScrollableCodeContainer';

const retryLink = new RetryLink({
  delay: {
    initial: 500,
    max: Infinity,
    jitter: true,
  },
  attempts: {
    max: 5,
    retryIf: (error) => !!error,
  },
});

const Container = tasty({
  styles: {
    position: 'relative',
    placeSelf: 'stretch',
  },
});

// Remove all keys that starts with "__" from the object recursively
function cleanServiceKeys(obj: any) {
  if (obj && typeof obj === 'object') {
    Object.keys(obj).forEach((key) => {
      if ((key as string).startsWith('__')) {
        delete obj[key];
      } else {
        cleanServiceKeys(obj[key]);
      }
    });
  }

  return obj;
}

export function QueryBuilderGraphQL() {
  const {
    query,
    isQueryTouched,
    queryHash,
    isQueryEmpty,
    apiUrl,
    apiToken,
    meta,
    hasPrivateMembers,
  } = useQueryBuilderContext();
  const [isFetching, setIsFetching] = useState(false);

  const gqlQuery = useMemo(() => {
    if (isQueryEmpty || !meta) {
      return 'query { __typename }'; // Empty query
    }

    return convertJsonQueryToGraphQL({ meta, query });
  }, [queryHash, isQueryEmpty, meta]);

  const gqlClient = useMemo(() => {
    const httpLink = new HttpLink({
      uri: apiUrl.replace('/v1', '/graphql'),
      headers: {
        Authorization: apiToken || '',
      },
    });

    return new ApolloClient({
      cache: new InMemoryCache(),
      link: ApolloLink.from([retryLink, httpLink]),
    });
  }, [apiUrl, apiToken]);

  const {
    data: rawData,
    loading: isLoading,
    error: queryError,
  } = useQuery(gql(gqlQuery), {
    client: gqlClient,
    skip: !isFetching,
    fetchPolicy: 'network-only',
  });

  const cleanedRawData = useMemo(() => {
    if (rawData) {
      return cleanServiceKeys(JSON.parse(JSON.stringify(rawData)));
    }

    return rawData;
  }, [rawData]);

  useEffect(() => {
    if (isQueryTouched) {
      setIsFetching(false);
    }
  }, [queryHash]);

  return useMemo(() => {
    let fetchButton =
      !rawData && !queryError ? (
        <Button
          isLoading={isLoading}
          isDisabled={hasPrivateMembers}
          icon={hasPrivateMembers ? <LockIcon /> : <PlayCircleOutlined />}
          size="small"
          onPress={() => setIsFetching(true)}
        >
          {isFetching && !rawData && !queryError ? 'Fetching...' : 'Fetch Raw Response'}
        </Button>
      ) : null;

    if (hasPrivateMembers && fetchButton) {
      fetchButton = (
        <TooltipProvider
          activeWrap
          title="Unable to fetch the raw response because the query contains private members."
          width={300}
        >
          {fetchButton}
        </TooltipProvider>
      );
    }

    return !query || isQueryEmpty ? (
      <Block padding="1x">
        <Alert theme="note">Compose a query to see a GraphQL query.</Alert>
      </Block>
    ) : (
      <TabPaneWithToolbar
        actions={
          <CopyButton type="secondary" value={gqlQuery || ''}>
            Copy
          </CopyButton>
        }
        extraActions={fetchButton}
      >
        <Grid
          columns={rawData ? 'minmax(0, 1fr) minmax(0, 1fr)' : '1fr'}
          placeSelf="stretch"
          gap="1bw"
          fill="#border"
        >
          <Container>
            <ScrollableCodeContainer value={gqlQuery || ''} />
          </Container>
          {rawData || queryError ? (
            <Container>
              <ScrollableCodeContainer
                value={
                  queryError
                    ? // @ts-ignore
                      (queryError?.networkError?.result?.error ?? queryError.toString())
                    : JSON.stringify(cleanedRawData, null, 2)
                }
              />
            </Container>
          ) : null}
        </Grid>
      </TabPaneWithToolbar>
    );
  }, [
    cleanedRawData,
    gqlQuery,
    hasPrivateMembers,
    isFetching,
    query,
    isQueryEmpty,
    rawData,
    isLoading,
  ]);
}
