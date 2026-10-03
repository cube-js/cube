<script lang="ts">
  import type {
    CacheMode,
    ChartType,
    CubeApi,
    DeeplyReadonly,
    Query,
  } from '@cubejs-client/core';
  import { onDestroy, type Snippet } from 'svelte';
  import { writable } from 'svelte/store';

  import { createQueryBuilder } from './stores/createQueryBuilder';
  import type {
    QueryBuilderOptions,
    QueryBuilderRenderProps,
    SchemaChangeEvent,
    StateChangeHeuristics,
    VizState,
  } from './types';

  interface Props {
    cubeApi?: CubeApi;
    defaultQuery?: DeeplyReadonly<Query>;
    defaultChartType?: ChartType;
    initialVizState?: Partial<VizState>;
    disableHeuristics?: boolean;
    stateChangeHeuristics?: StateChangeHeuristics;
    onVizStateChanged?: (vizState: Readonly<VizState>) => void;
    executeQuery?: boolean;
    subscribe?: boolean;
    resetResultSetOnChange?: boolean;
    castNumerics?: boolean;
    cache?: CacheMode;
    baseRequestId?: string;
    schemaVersion?: number | string;
    onSchemaChange?: (event: SchemaChangeEvent) => void;
    children?: Snippet<[QueryBuilderRenderProps]>;
  }

  let {
    cubeApi,
    defaultQuery = {},
    defaultChartType = 'line',
    initialVizState,
    disableHeuristics = false,
    stateChangeHeuristics,
    onVizStateChanged,
    executeQuery = true,
    subscribe = false,
    resetResultSetOnChange = false,
    castNumerics,
    cache,
    baseRequestId,
    schemaVersion,
    onSchemaChange,
    children,
  }: Props = $props();

  const toOptions = (): QueryBuilderOptions => ({
    cubeApi,
    defaultQuery,
    defaultChartType,
    initialVizState,
    disableHeuristics,
    stateChangeHeuristics,
    onVizStateChanged,
    executeQuery,
    subscribe,
    resetResultSetOnChange,
    castNumerics,
    cache,
    baseRequestId,
    schemaVersion,
    onSchemaChange,
  });

  const optionsStore = writable<QueryBuilderOptions>(toOptions());
  const builder = createQueryBuilder(optionsStore);

  $effect(() => {
    optionsStore.set(toOptions());
  });

  onDestroy(() => {
    void builder.destroy();
  });
</script>

{@render children?.({ ...$builder, ...builder.actions })}
