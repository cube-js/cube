import type { LoadMethodOptions } from '@cubejs-client/core';
import { cleanup, render, screen } from '@testing-library/svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';

import ProviderQuery from './fixtures/ProviderQuery.svelte';
import TotalsQuery from './fixtures/TotalsQuery.svelte';
import { cubeApi, resultSet } from './helpers';

afterEach(() => cleanup());

describe('Svelte components', () => {
  it('resolves the core client and defaults through CubeProvider context', async () => {
    const load = vi.fn().mockResolvedValue(resultSet('provider'));

    render(ProviderQuery, {
      props: { cubeApi: cubeApi({ load }) },
    });

    expect(await screen.findByText('loaded')).toBeTruthy();
    expect(load).toHaveBeenCalledOnce();
    expect((load.mock.calls[0][1] as LoadMethodOptions).castNumerics).toBe(
      true
    );
  });

  it('composes main and ungrouped totals queries', async () => {
    const load = vi.fn().mockResolvedValue(resultSet('totals'));

    render(TotalsQuery, {
      props: { cubeApi: cubeApi({ load }) },
    });

    expect(await screen.findByText('loaded totals')).toBeTruthy();
    expect(load).toHaveBeenCalledTimes(2);
    expect(load.mock.calls[0][0]).toEqual({
      measures: ['Orders.count'],
      dimensions: ['Orders.status'],
      timeDimensions: [
        { dimension: 'Orders.createdAt', granularity: 'day' },
      ],
    });
    expect(load.mock.calls[1][0]).toEqual({
      measures: ['Orders.count'],
      dimensions: [],
      timeDimensions: [
        { dimension: 'Orders.createdAt', granularity: undefined },
      ],
    });
  });
});
