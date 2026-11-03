<script
  lang="ts"
  generics="TQueries extends NamedQueries | undefined = undefined"
>
  import type {
    CacheMode,
    CubeApi,
    ResultSet,
  } from '@cubejs-client/core';
  import { onDestroy, type Snippet } from 'svelte';
  import { writable } from 'svelte/store';

  import { createQueryRenderer } from './stores/createQueryRenderer';
  import type {
    LoadSql,
    NamedQueries,
    QueryInput,
    QueryRendererInput,
    QueryRendererOptions,
    QueryRendererState,
  } from './types';

  type RenderState = QueryRendererState<
    ResultSet | Record<string, ResultSet> | null
  >;

  interface Props {
    query?: TQueries extends NamedQueries ? never : QueryInput;
    queries?: TQueries;
    cubeApi?: CubeApi;
    loadSql?: LoadSql;
    skip?: boolean;
    subscribe?: boolean;
    resetResultSetOnChange?: boolean;
    castNumerics?: boolean;
    cache?: CacheMode;
    baseRequestId?: string;
    children?: Snippet<
      [
        QueryRendererState<
          TQueries extends NamedQueries
            ? Record<string, ResultSet> | null
            : ResultSet | null
        >,
      ]
    >;
  }

  let {
    query,
    queries,
    cubeApi,
    loadSql = false,
    skip = false,
    subscribe = false,
    resetResultSetOnChange = true,
    castNumerics,
    cache,
    baseRequestId,
    children,
  }: Props = $props();

  const toInput = (): QueryRendererInput => {
    if ((query === undefined) === (queries === undefined)) {
      throw new Error(
        'QueryRenderer requires exactly one of query or queries.'
      );
    }

    return queries !== undefined
      ? { queries }
      : { query: query as QueryInput };
  };

  const toOptions = (): QueryRendererOptions => ({
    cubeApi,
    loadSql,
    skip,
    subscribe,
    resetResultSetOnChange,
    castNumerics,
    cache,
    baseRequestId,
  });

  const getChildren = () =>
    children as unknown as Snippet<[RenderState]> | undefined;

  const inputStore = writable<QueryRendererInput>(toInput());
  const optionsStore = writable<QueryRendererOptions>(toOptions());
  const renderer = createQueryRenderer(inputStore, optionsStore);

  $effect(() => {
    inputStore.set(toInput());
  });

  $effect(() => {
    optionsStore.set(toOptions());
  });

  onDestroy(() => {
    void renderer.destroy();
  });
</script>

{@render getChildren()?.({
  ...$renderer,
  loadingState: { isLoading: $renderer.isLoading },
  refetch: renderer.refetch,
})}
