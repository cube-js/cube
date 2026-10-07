import { ApiGateway } from '../src';
import { NormalizedQuery } from '../src/types/query';
import { AdapterApiMock, DataSourceStorageMock, compilerApi } from './mocks';

class TestApiGateway extends ApiGateway {
  public resolverCalls = 0;

  public checkMemberPaths(query: NormalizedQuery, resolve?: (path: string) => any) {
    return super.checkMemberPaths(query, resolve);
  }

  public getNormalizedQueries(query: Record<string, any>, context: any) {
    return super.getNormalizedQueries(query, context);
  }

  protected async memberPathResolver(context: any, api?: any) {
    this.resolverCalls++;
    return super.memberPathResolver(context, api);
  }
}

const resolved: Record<string, object> = {
  'orders.count': { fullPath: 'orders.count', instancePath: 'orders', member: 'count', aliased: false },
  'orders.customer.city': { fullPath: 'orders.customer.city', instancePath: 'orders.customer', member: 'city', aliased: true },
  'orders.products.name': {
    fullPath: 'orders.products.name', instancePath: 'products', member: 'name', targetPath: 'products.name', aliased: false,
  },
  'products.orders.customer.city': {
    fullPath: 'products.orders.customer.city', instancePath: 'orders.customer', member: 'city', targetPath: 'users.city', aliased: true,
  },
  'orders.created_at.day': {
    fullPath: 'orders.created_at', instancePath: 'orders', member: 'created_at', granularity: 'day', aliased: false,
  },
};
const resolve = (path: string) => resolved[path] ?? null;

describe('Member paths through joins', () => {
  const gateway = new TestApiGateway('secret', compilerApi, async () => new AdapterApiMock(), () => undefined, {
    standalone: true,
    dataSourceStorage: new DataSourceStorageMock(),
    basePath: '/cubejs-api',
    refreshScheduler: {},
  });

  it('accepts plain members, alias paths and granularities', () => {
    const query = {
      measures: ['orders.count'],
      dimensions: ['orders.customer.city'],
      order: [['orders.created_at.day', 'asc']],
    } as unknown as NormalizedQuery;
    expect(gateway.checkMemberPaths(query, resolve)).toBe(query);
  });

  it.each([
    ['dimensions', { dimensions: ['orders.products.name'] }],
    ['filters', { filters: [{ or: [{ member: 'orders.products.name', operator: 'set' }] }] }],
    ['order', { order: [['orders.products.name', 'asc']] }],
  ])('rejects a join path with no alias in %s', (_, query) => {
    expect(() => gateway.checkMemberPaths(query as unknown as NormalizedQuery, resolve))
      .toThrow(/'orders.products.name' goes through joins that are not join aliases. Query it as 'products.name'/);
  });

  it('rejects a cube hop before an alias', () => {
    const query = { dimensions: ['products.orders.customer.city'] } as unknown as NormalizedQuery;
    expect(() => gateway.checkMemberPaths(query, resolve)).toThrow(/Query it as 'orders.customer.city'/);
  });

  it('reads the data model only for a query with a path longer than cube.member', async () => {
    const context = { securityContext: {}, requestId: 'test' } as any;
    gateway.resolverCalls = 0;
    await gateway.getNormalizedQueries({ measures: ['Foo.bar'], dimensions: ['Foo.id'] }, context);
    expect(gateway.resolverCalls).toBe(0);

    await gateway.getNormalizedQueries({ measures: ['Foo.bar'], dimensions: ['Foo.time.day'] }, context);
    expect(gateway.resolverCalls).toBe(1);
  });
});
