<script lang="ts">
  import type { CubeApi } from '@cubejs-client/core';

  import { CubeProvider, QueryRenderer } from '../../src/lib';

  interface Props {
    cubeApi: CubeApi;
  }

  let { cubeApi }: Props = $props();
</script>

<CubeProvider {cubeApi} options={{ castNumerics: true }}>
  <QueryRenderer query={{ measures: ['Orders.count'] }}>
    {#snippet children({ resultSet, error, isLoading })}
      <output data-testid="status">
        {error?.message ?? (resultSet ? 'loaded' : isLoading ? 'loading' : 'idle')}
      </output>
    {/snippet}
  </QueryRenderer>
</CubeProvider>
