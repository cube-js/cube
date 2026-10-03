import {
  Meta,
  type CubeApi,
  type DryRunResponse,
  type Query,
  type ResultSet,
  type TCubeDimension,
  type TCubeMeasure,
  type TCubeSegment,
} from '@cubejs-client/core';

export interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(reason?: unknown): void;
}

export function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });

  return { promise, resolve, reject };
}

export async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

export function resultSet(id: string): ResultSet {
  return { id } as unknown as ResultSet;
}

export function cubeApi(
  methods: Record<string, unknown>
): CubeApi {
  return methods as unknown as CubeApi;
}

export const ORDERS_COUNT: TCubeMeasure = {
  name: 'Orders.count',
  title: 'Orders Count',
  shortTitle: 'Count',
  type: 'number',
  aggType: 'count',
  cumulative: false,
  cumulativeTotal: false,
  drillMembers: [],
  drillMembersGrouped: { measures: [], dimensions: [] },
};

export const ORDERS_TOTAL: TCubeMeasure = {
  ...ORDERS_COUNT,
  name: 'Orders.total',
  title: 'Orders Total',
  shortTitle: 'Total',
  aggType: 'number',
};

export const ORDERS_STATUS: TCubeDimension = {
  name: 'Orders.status',
  title: 'Orders Status',
  shortTitle: 'Status',
  type: 'string',
  suggestFilterValues: true,
};

export const ORDERS_CREATED_AT: TCubeDimension = {
  name: 'Orders.createdAt',
  title: 'Orders Created At',
  shortTitle: 'Created At',
  type: 'time',
  suggestFilterValues: true,
  granularities: [{ name: 'fiscal_week', title: 'Fiscal week' }],
};

export const ORDERS_COMPLETED: TCubeSegment = {
  name: 'Orders.completed',
  title: 'Completed orders',
  shortTitle: 'Completed',
};

export function ordersMeta(): Meta {
  return new Meta({
    cubes: [
      {
        name: 'Orders',
        title: 'Orders',
        type: 'cube',
        public: true,
        measures: [ORDERS_COUNT, ORDERS_TOTAL],
        dimensions: [ORDERS_STATUS, ORDERS_CREATED_AT],
        segments: [ORDERS_COMPLETED],
        folders: [],
        nestedFolders: [],
        hierarchies: [],
      },
    ],
  });
}

export function dryRunResponse(
  query: Query,
  queryOrder: DryRunResponse['queryOrder'] = []
): DryRunResponse {
  return {
    queryType: 'regularQuery',
    normalizedQueries: [query],
    pivotQuery: { ...query, queryType: 'regularQuery' },
    queryOrder,
    transformedQueries: [],
  };
}
