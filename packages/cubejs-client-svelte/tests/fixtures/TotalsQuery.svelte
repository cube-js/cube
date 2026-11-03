<script lang="ts">
  import type { CubeApi } from '@cubejs-client/core';

  import { QueryRendererWithTotals } from '../../src/lib';

  interface Props {
    cubeApi: CubeApi;
  }

  let { cubeApi }: Props = $props();
</script>

<QueryRendererWithTotals
  {cubeApi}
  query={{
    measures: ['Orders.count'],
    dimensions: ['Orders.status'],
    timeDimensions: [
      { dimension: 'Orders.createdAt', granularity: 'day' },
    ],
  }}
>
  {#snippet children({ resultSet })}
    <output data-testid="totals-status">
      {resultSet?.main && resultSet.totals ? 'loaded totals' : 'idle'}
    </output>
  {/snippet}
</QueryRendererWithTotals>
