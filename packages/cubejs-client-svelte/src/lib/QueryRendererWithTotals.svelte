<script lang="ts">
  import type {
    CacheMode,
    CubeApi,
    DeeplyReadonly,
    Query,
    ResultSet,
  } from '@cubejs-client/core';
  import type { Snippet } from 'svelte';

  import QueryRenderer from './QueryRenderer.svelte';
  import type { QueryRendererState } from './types';

  interface Props {
    query: DeeplyReadonly<Query>;
    cubeApi?: CubeApi;
    skip?: boolean;
    resetResultSetOnChange?: boolean;
    castNumerics?: boolean;
    cache?: CacheMode;
    baseRequestId?: string;
    children?: Snippet<
      [QueryRendererState<Record<string, ResultSet> | null>]
    >;
  }

  let {
    query,
    cubeApi,
    skip = false,
    resetResultSetOnChange = true,
    castNumerics,
    cache,
    baseRequestId,
    children,
  }: Props = $props();

  const buildQueries = () => ({
    main: query,
    totals: {
      ...query,
      dimensions: [],
      timeDimensions: query.timeDimensions?.map((value) => ({
        ...value,
        granularity: undefined,
      })),
    },
  });
</script>

<QueryRenderer
  queries={buildQueries()}
  {cubeApi}
  {skip}
  {resetResultSetOnChange}
  {castNumerics}
  {cache}
  {baseRequestId}
>
  {#snippet children(state)}
    {@render children?.(state as QueryRendererState<Record<string, ResultSet> | null>)}
  {/snippet}
</QueryRenderer>
