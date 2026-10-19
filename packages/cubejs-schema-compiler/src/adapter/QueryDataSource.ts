import type { BaseQueryOptions, Compilers } from './BaseQuery';
import { JoinPathNotFoundError, UserError } from '../compiler/UserError';

type QueryMembers = Pick<BaseQueryOptions, 'measures' | 'dimensions' | 'segments' | 'timeDimensions' | 'filters' | 'joinHints'>;

type MemberExpression = {
  cubeName: string;
  expression: (...args: Array<unknown>) => unknown;
};

type CubeReferences = {
  cubes: string[];
  members: string[][];
};

const NO_REFERENCES: CubeReferences = { cubes: [], members: [] };

function isMemberExpression(member: unknown): member is MemberExpression {
  return typeof member === 'object' && member !== null &&
    typeof (member as MemberExpression).cubeName === 'string' &&
    typeof (member as MemberExpression).expression === 'function';
}

export class QueryDataSourceResolver {
  protected readonly compilers: Compilers;

  protected readonly cubes = new Set<string>();

  protected readonly visitedMembers = new Set<string>();

  public constructor(compilers: Compilers) {
    this.compilers = compilers;
  }

  public resolve(query: QueryMembers): string | undefined {
    for (const member of [...(query.measures || []), ...(query.dimensions || []), ...(query.segments || [])]) {
      this.addMember(member);
    }

    for (const timeDimension of query.timeDimensions || []) {
      this.addMember(timeDimension?.dimension);
    }

    this.addFilters(query.filters || []);

    for (const hint of query.joinHints || []) {
      for (const cube of Array.isArray(hint) ? hint : [hint]) {
        this.cubes.add(cube);
      }
    }

    const { cubeEvaluator } = this.compilers;
    // The `dataSource` getter collects the cubes behind a view's members, never the view
    const cubes = [...this.cubes].filter(cube => !cubeEvaluator.cubeFromPath(cube).isView);
    if (!cubes.length) {
      return undefined;
    }

    const dataSources = new Set(cubes.map(cube => cubeEvaluator.cubeDataSource(cube)));
    if (dataSources.size === 1 && this.hasCrossDataSourceJoins()) {
      // Joins between data sources can put a cube of another one on the join path
      for (const cube of this.joinedCubes(cubes)) {
        dataSources.add(cubeEvaluator.cubeDataSource(cube));
      }
    }

    return dataSources.size === 1 ? [...dataSources][0] : undefined;
  }

  protected joinedCubes(cubes: string[]): string[] {
    try {
      const join = this.compilers.joinGraph.buildJoin(cubes);
      return join ? [join.root, ...join.joins.flatMap(j => [j.originalFrom, j.originalTo])] : [];
    } catch (e) {
      // As `BaseQuery.prebuildJoin()`: Tesseract plans a multi-fact query without one join,
      // and the legacy planner throws the same error when it builds the query
      if (e instanceof JoinPathNotFoundError) {
        return [];
      }
      throw e;
    }
  }

  protected addFilters(filters: any[]) {
    for (const filter of filters) {
      if (filter.and || filter.or) {
        this.addFilters(filter.and || filter.or);
      } else {
        this.addMember(filter.member || filter.dimension);
      }
    }
  }

  protected addMember(member: unknown) {
    if (isMemberExpression(member)) {
      this.cubes.add(member.cubeName);
      this.addReferences(this.collectReferences(member.cubeName, [member.expression]));
    } else if (typeof member === 'string') {
      const resolved = this.compilers.cubeEvaluator.resolveMemberPath(member);
      if (!resolved) {
        throw new UserError(`'${member}' not found`);
      }
      this.addModelMember(resolved.targetCube, resolved.member);
    }
  }

  protected addModelMember(cube: string, member: string) {
    const path = `${cube}.${member}`;
    if (this.visitedMembers.has(path)) {
      return;
    }
    this.visitedMembers.add(path);
    this.cubes.add(cube);

    const references: CubeReferences = this.compilers.compiler.compilerCache.cache(
      ['resolveQueryDataSource', 'memberReferences', path],
      () => this.memberReferences(cube, member)
    );
    this.addReferences(references);
  }

  protected addReferences({ cubes, members }: CubeReferences) {
    for (const cube of cubes) {
      this.cubes.add(cube);
    }

    for (const [cube, member] of members) {
      this.addModelMember(cube, member);
    }
  }

  protected memberReferences(cube: string, member: string): CubeReferences {
    const { cubeEvaluator } = this.compilers;
    const evaluatedCube = cubeEvaluator.cubeFromPath(cube);
    const definition: any = evaluatedCube.measures?.[member] ||
      evaluatedCube.dimensions?.[member] ||
      evaluatedCube.segments?.[member];
    if (!definition) {
      return NO_REFERENCES;
    }

    const sqlFns = [
      definition.sql,
      ...(definition.filters || []).map((filter: any) => filter.sql),
      ...(definition.case?.when || []).map((when: any) => when.sql),
      definition.case?.else?.label?.sql,
      definition.latitude?.sql,
      definition.longitude?.sql,
    ].filter(fn => typeof fn === 'function');

    return this.collectReferences(cube, sqlFns);
  }

  protected collectReferences(cube: string, sqlFns: Array<(...args: Array<unknown>) => unknown>): CubeReferences {
    const references: CubeReferences = { cubes: [], members: [] };

    for (const fn of sqlFns) {
      const { cubeReferencesUsed, pathReferencesUsed } = this.compilers.cubeEvaluator.collectUsedCubeReferences(cube, fn);
      references.cubes.push(...cubeReferencesUsed);
      references.members.push(...pathReferencesUsed);
    }
    return references;
  }

  protected hasCrossDataSourceJoins(): boolean {
    return this.compilers.compiler.compilerCache.cache(
      ['resolveQueryDataSource', 'crossDataSourceJoins'],
      // Wrapped: the cache treats a falsy value as a miss
      () => [this.findCrossDataSourceJoin()]
    )[0];
  }

  protected findCrossDataSourceJoin(): boolean {
    const { cubeEvaluator } = this.compilers;
    return cubeEvaluator.cubeNames().some(cube => {
      const dataSource = cubeEvaluator.cubeDataSource(cube);
      return (cubeEvaluator.cubeFromPath(cube).joins || []).some(
        (join: { name: string }) => cubeEvaluator.cubeDataSource(join.name) !== dataSource
      );
    });
  }
}

/**
 * The data source of a query, from the data model alone: the dialect to build the query with
 * depends on it. `undefined` when the query has no members, or when its cubes span several
 * data sources, as a rollupJoin does.
 */
export function resolveQueryDataSource(compilers: Compilers, query: QueryMembers): string | undefined {
  return new QueryDataSourceResolver(compilers).resolve(query);
}
