<script lang="ts">
  import type { CubeApi } from '@cubejs-client/core';
  import type { Snippet } from 'svelte';
  import { writable } from 'svelte/store';

  import { setCubeContext } from './context';
  import type { CubeProviderOptions } from './types';

  function initializeStore<T>(getValue: () => T) {
    return writable(getValue());
  }

  interface Props {
    cubeApi: CubeApi;
    options?: CubeProviderOptions;
    children?: Snippet;
  }

  let { cubeApi, options = {}, children }: Props = $props();

  const cubeApiStore = initializeStore(() => cubeApi);
  const optionsStore = initializeStore<Readonly<CubeProviderOptions>>(
    () => options
  );

  setCubeContext({
    cubeApi: { subscribe: cubeApiStore.subscribe },
    options: { subscribe: optionsStore.subscribe },
  });

  $effect(() => {
    cubeApiStore.set(cubeApi);
  });

  $effect(() => {
    optionsStore.set(options);
  });
</script>

{@render children?.()}
