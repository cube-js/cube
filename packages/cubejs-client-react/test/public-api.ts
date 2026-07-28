/**
 * Compile-time conformance check between the implementation in `src` and the
 * published, hand-written declarations in `index.d.ts`.
 *
 * `index.d.ts` is what consumers get (`package.json#types`), so the
 * implementation must stay assignable to it. Nothing here runs — `yarn tsc` is
 * the test.
 *
 * Pre-existing mismatches that are deliberately not asserted:
 *  - `isQueryPresent` is declared but not exported by `src/index.ts`
 *    (it is exported by `@cubejs-client/core`).
 *  - `QueryRendererWithTotals` is exported but not declared.
 *  - `useCubeSql` is declared to return a dry-run response while it resolves
 *    with a `SqlQuery`, so only its parameters are asserted below.
 *  - Class components are compared through their props and state rather than
 *    as a whole, because `Component#setState` is generic over `keyof S` and
 *    the implementation state is a superset of the declared one.
 */
import type * as Declared from '@cubejs-client/react';
import type * as Impl from '../src/index';

type Assert<T extends true> = T;

type Extends<Source, Target> = Source extends Target ? true : false;

type ImplQueryRenderer = InstanceType<typeof Impl.QueryRenderer>;
type DeclaredQueryRenderer = InstanceType<typeof Declared.QueryRenderer>;

type ImplQueryBuilder = InstanceType<typeof Impl.QueryBuilder>;
type DeclaredQueryBuilder = InstanceType<typeof Declared.QueryBuilder>;

// Components and context
export type CubeProviderConforms = Assert<Extends<typeof Impl.CubeProvider, typeof Declared.CubeProvider>>;
export type CubeContextConforms = Assert<Extends<typeof Impl.CubeContext, typeof Declared.CubeContext>>;

export type QueryRendererPropsConform = Assert<
  Extends<ImplQueryRenderer['props'], DeclaredQueryRenderer['props']>
>;
export type QueryRendererStateConforms = Assert<
  Extends<ImplQueryRenderer['state'], DeclaredQueryRenderer['state']>
>;
export type QueryBuilderPropsConform = Assert<
  Extends<ImplQueryBuilder['props'], DeclaredQueryBuilder['props']>
>;
export type QueryBuilderStateConforms = Assert<
  Extends<ImplQueryBuilder['state'], DeclaredQueryBuilder['state']>
>;

// Hooks
export type UseCubeQueryConforms = Assert<Extends<typeof Impl.useCubeQuery, typeof Declared.useCubeQuery>>;
export type UseCubeMetaConforms = Assert<Extends<typeof Impl.useCubeMeta, typeof Declared.useCubeMeta>>;
export type UseDryRunConforms = Assert<Extends<typeof Impl.useDryRun, typeof Declared.useDryRun>>;
export type UseLazyDryRunConforms = Assert<Extends<typeof Impl.useLazyDryRun, typeof Declared.useLazyDryRun>>;
export type UseCubeSqlAcceptsDeclaredArgs = Assert<
  Extends<Parameters<typeof Declared.useCubeSql>, Parameters<typeof Impl.useCubeSql>>
>;
