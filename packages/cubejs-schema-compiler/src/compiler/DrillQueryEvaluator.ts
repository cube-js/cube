import { UserError } from './UserError';
import {
  DrillNameResolver,
  DrillQueryModel,
  ResolvedDrillQuery,
  drillQueryTargets,
  resolveCubeName,
  resolveDrillQuery,
  resolveViewName,
} from './DrillQueries';
import type { CubeDefinitionExtended } from './CubeSymbols';
import type { CubeEvaluator } from './CubeEvaluator';
import type { CubeValidator } from './CubeValidator';
import type { ErrorReporter } from './ErrorReporter';
import type { JoinGraph } from './JoinGraph';
import type { CompilerInterface } from './PrepareCompiler';

/**
 * Validates the drill queries and member sets of cubes and views, and
 * resolves the drill queries each measure offers. Runs after the join graph
 * is built, as a view drill query must name members that can be joined.
 */
export class DrillQueryEvaluator implements CompilerInterface {
  protected byMeasure: Map<string, ResolvedDrillQuery[]> = new Map();

  public constructor(
    protected readonly cubeValidator: CubeValidator,
    protected readonly cubeEvaluator: CubeEvaluator,
    protected readonly joinGraph: JoinGraph,
  ) {
  }

  /** The drill queries a measure offers, with qualified member names */
  public drillQueries(measurePath: string): ResolvedDrillQuery[] {
    return this.byMeasure.get(measurePath) || [];
  }

  public compile(_cubes: unknown, errorReporter: ErrorReporter): void {
    this.byMeasure = new Map();
    const model = this.cubeEvaluator.drillQueryModel();

    for (const cube of this.cubeEvaluator.cubeList.filter(c => this.cubeValidator.isCubeValid(c))) {
      const reporter = errorReporter.inContext(`${cube.name} cube`);
      const resolve: DrillNameResolver = cube.isView
        ? (name, allowSets) => resolveViewName(model, cube.name, name, allowSets)
        : (name, allowSets) => resolveCubeName(model, cube.name, name, allowSets);

      this.checkMemberSets(cube, model, resolve, reporter);

      for (const [measureName, measure] of Object.entries<any>(cube.measures || {})) {
        const own = this.ownDrillQueries(cube, measureName, measure, model, resolve, reporter);
        const inherited: ResolvedDrillQuery[] = measure.inheritedDrillQueries || [];
        const drillQueries = [
          ...inherited.map(query => own.find(o => o.name === query.name) ?? query),
          ...own.filter(query => !inherited.some(i => i.name === query.name)),
        ];

        if (drillQueries.length) {
          this.byMeasure.set(`${cube.name}.${measureName}`, drillQueries);
        }
      }
    }
  }

  protected checkMemberSets(
    cube: CubeDefinitionExtended,
    model: DrillQueryModel,
    resolve: DrillNameResolver,
    reporter: ErrorReporter
  ) {
    const seen = new Set<string>();

    for (const set of model.memberSets(cube.name)) {
      if (seen.has(set.name)) {
        reporter.error(`Member set '${cube.name}.${set.name}' is defined more than once`);
      } else if (model.member(cube.name, set.name) || (cube as any).hierarchies?.[set.name]) {
        reporter.error(`Member set '${cube.name}.${set.name}' has the name of a member of '${cube.name}'`);
      } else {
        try {
          resolve(set.name, true);
        } catch (e: any) {
          reporter.error(`Member set '${cube.name}.${set.name}': ${e.message}`);
        }
      }
      seen.add(set.name);
    }
  }

  protected ownDrillQueries(
    cube: CubeDefinitionExtended,
    measureName: string,
    measure: any,
    model: DrillQueryModel,
    resolve: DrillNameResolver,
    reporter: ErrorReporter
  ): ResolvedDrillQuery[] {
    if (!Array.isArray(measure.drillQueries)) {
      return [];
    }

    const measurePath = `${cube.name}.${measureName}`;
    const seen = new Set<string>();
    const result: ResolvedDrillQuery[] = [];

    for (const definition of measure.drillQueries) {
      if (seen.has(definition.name)) {
        reporter.error(`Measure '${measurePath}' has more than one drill query named '${definition.name}'`);
      } else {
        try {
          const resolved = resolveDrillQuery(model, definition, resolve);
          if (cube.isView) {
            this.checkJoinable(cube.name, measureName, resolved, model);
          }
          result.push(resolved);
        } catch (e: any) {
          reporter.error(`Measure '${measurePath}': ${e.message}`);
        }
      }
      seen.add(definition.name);
    }

    return result;
  }

  /**
   * A view drill query runs as one query: the cubes under its members must
   * join with the cubes under the measure.
   */
  protected checkJoinable(viewName: string, measureName: string, query: ResolvedDrillQuery, model: DrillQueryModel) {
    const cubes = new Set<string>(this.underlyingCubes(viewName, 'measures', measureName, model));

    for (const target of drillQueryTargets(query)) {
      const [owner, member] = target.name.split('.');
      this.underlyingCubes(owner, target.kind, member, model).forEach(c => cubes.add(c));
    }

    if (cubes.size < 2) {
      return;
    }

    try {
      this.joinGraph.buildJoin([...cubes]);
    } catch {
      throw new UserError(
        `Drill query '${query.name}' names members that can't be joined: there is no join path between ${[...cubes].map(c => `'${c}'`).join(', ')}`
      );
    }
  }

  protected underlyingCubes(owner: string, kind: 'dimensions' | 'measures', memberName: string, model: DrillQueryModel): string[] {
    if (!model.isView(owner)) {
      return [owner];
    }

    const sql = (this.cubeEvaluator.symbols[owner]?.cubeObj() as any)?.[kind]?.[memberName]?.sql;
    if (typeof sql !== 'function') {
      return [];
    }

    const { pathReferencesUsed } = this.cubeEvaluator.collectUsedCubeReferences(owner, sql);
    return pathReferencesUsed.map(([cube]) => cube).filter(c => model.isView(c) === false);
  }
}
