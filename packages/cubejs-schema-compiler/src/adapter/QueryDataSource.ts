import type { BaseQueryOptions, Compilers } from './BaseQuery';

type QueryMembers = Pick<BaseQueryOptions, 'measures' | 'dimensions' | 'segments' | 'timeDimensions' | 'filters' | 'joinHints' | 'subqueryJoins'>;

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

/**
 * Picks the data source of a query from the data model alone, before a dialect is chosen.
 *
 * It's a prediction: `undefined` when the query has no members, or when the cubes it can see
 * span several data sources. The `dataSource` getter of the built query stays the source of
 * truth and is compared against it, so a cube this misses costs another build, not wrong SQL.
 */
export function predictQueryDataSource(compilers: Compilers, query: QueryMembers): string | undefined {
  try {
    return new QueryDataSourcePredictor(compilers).predict(query);
  } catch (e) {
    // The build reports the error of an invalid query in its own terms
    return undefined;
  }
}

export class QueryDataSourcePredictor {
  protected readonly compilers: Compilers;

  protected readonly cubes = new Set<string>();

  protected readonly visitedMembers = new Set<string>();

  public constructor(compilers: Compilers) {
    this.compilers = compilers;
  }

  public predict(query: QueryMembers): string | undefined {
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
    for (const subqueryJoin of query.subqueryJoins || []) {
      this.addMember(subqueryJoin.on);
    }

    const { cubeEvaluator } = this.compilers;
    // The `dataSource` getter collects the cubes behind a view's members, never the view
    const cubes = [...this.cubes].filter(cube => !cubeEvaluator.cubeFromPath(cube).isView);
    if (!cubes.length) {
      return undefined;
    }
    if (this.hasCrossDataSourceJoins()) {
      // Joins between data sources can put a cube of another one on the join path
      const join = this.compilers.joinGraph.buildJoin(cubes);
      if (!join) {
        return undefined;
      }
      cubes.push(join.root, ...join.joins.flatMap(j => [j.originalFrom, j.originalTo]));
    }

    const dataSources = new Set(cubes.map(cube => cubeEvaluator.cubeDataSource(cube)));
    return dataSources.size === 1 ? [...dataSources][0] : undefined;
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
        throw new Error(`Unknown member: ${member}`);
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
      ['predictQueryDataSource', 'memberReferences', path],
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
      ['predictQueryDataSource', 'crossDataSourceJoins'],
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
