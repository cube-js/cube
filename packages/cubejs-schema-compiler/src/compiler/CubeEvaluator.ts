/* eslint-disable no-restricted-syntax */
import R from 'ramda';

import { CubeSymbols } from './CubeSymbols';
import { UserError } from './UserError';
import { BaseQuery } from '../adapter';
import type { CubeValidator } from './CubeValidator';
import type { ErrorReporter } from './ErrorReporter';

export type SegmentDefinition = {
  type: string,
  sql: Function,
  primaryKey?: true,
  ownedByCube: boolean,
  fieldType?: string,
};

export type DimensionDefinition = {
  type: string,
  sql: Function,
  primaryKey?: true,
  ownedByCube: boolean,
  fieldType?: string,
};

export type MeasureDefinition = {
  type: string,
  sql: Function,
  ownedByCube: boolean,
  rollingWindow?: any
  filters?: any
  primaryKey?: true,
  drillFilters?: any
};

export class CubeEvaluator extends CubeSymbols {
  public evaluatedCubes: Record<string, any> = {};

  public primaryKeys: Record<string, any> = {};

  public byFileName: Record<string, any> = {};

  public constructor(
    protected readonly cubeValidator: CubeValidator
  ) {
    super(true);
  }

  public compile(cubes: any, errorReporter: ErrorReporter) {
    super.compile(cubes, errorReporter);
    const validCubes = this.cubeList.filter(cube => this.cubeValidator.isCubeValid(cube));

    Object.values(validCubes).map((cube) => this.prepareCube(cube, errorReporter));

    this.evaluatedCubes = R.fromPairs(validCubes.map(v => [v.name, v]));
    this.byFileName = R.groupBy(v => v.fileName, validCubes);
    this.primaryKeys = R.fromPairs(
      validCubes.map((v) => {
        const primaryKeyNamesToSymbols = R.compose(
          R.map((d: any) => d[0]),
          R.filter((d: any) => d[1].primaryKey),
          R.toPairs
        )(v.dimensions || {});
        return [v.name, primaryKeyNamesToSymbols];
      })
    );
  }

  protected prepareCube(cube, errorReporter: ErrorReporter) {
    this.prepareJoins(cube, errorReporter);
    this.preparePreAggregations(cube, errorReporter);
    this.prepareMembers(cube.measures, cube, errorReporter);
    this.prepareMembers(cube.dimensions, cube, errorReporter);
    this.prepareMembers(cube.segments, cube, errorReporter);
  }

  protected prepareJoins(cube: any, _errorReporter: ErrorReporter) {
    if (cube.joins) {
      // eslint-disable-next-line no-restricted-syntax
      for (const join of Object.values(cube.joins) as any[]) {
        // eslint-disable-next-line default-case
        switch (join.relationship) {
          case 'belongs_to':
          case 'many_to_one':
          case 'manyToOne':
            join.relationship = 'belongsTo';
            break;
          case 'has_many':
          case 'one_to_many':
          case 'oneToMany':
            join.relationship = 'hasMany';
            break;
          case 'has_one':
          case 'one_to_one':
          case 'oneToOne':
            join.relationship = 'hasOne';
            break;
        }
      }
    }
  }

  protected preparePreAggregations(cube: any, errorReporter: ErrorReporter) {
    if (cube.preAggregations) {
      // eslint-disable-next-line no-restricted-syntax
      for (const preAggregation of Object.values(cube.preAggregations) as any) {
        if (preAggregation.timeDimension) {
          preAggregation.timeDimensionReference = preAggregation.timeDimension;
          delete preAggregation.timeDimension;
        }

        if (preAggregation.dimensions) {
          preAggregation.dimensionReferences = preAggregation.dimensions;
          delete preAggregation.dimensions;
        }

        if (preAggregation.measures) {
          preAggregation.measureReferences = preAggregation.measures;
          delete preAggregation.measures;
        }

        if (preAggregation.segments) {
          preAggregation.segmentReferences = preAggregation.segments;
          delete preAggregation.segments;
        }

        if (preAggregation.rollups) {
          preAggregation.rollupReferences = preAggregation.rollups;
          delete preAggregation.rollups;
        }

        if (preAggregation.buildRangeStart) {
          if (preAggregation.refreshRangeStart) {
            errorReporter.warning({
              message: 'You specified both buildRangeStart and refreshRangeStart, buildRangeStart will be used.',
              loc: null,
            });
          }

          preAggregation.refreshRangeStart = preAggregation.buildRangeStart;
          delete preAggregation.buildRangeStart;
        }

        if (preAggregation.buildRangeEnd) {
          if (preAggregation.refreshRangeEnd) {
            errorReporter.warning({
              message: 'You specified both buildRangeEnd and refreshRangeEnd, buildRangeEnd will be used.',
              loc: null,
            });
          }

          preAggregation.refreshRangeEnd = preAggregation.buildRangeEnd;
          delete preAggregation.buildRangeEnd;
        }
      }
    }
  }

  protected prepareMembers(members: any, cube: any, errorReporter: ErrorReporter) {
    members = members || {};

    for (const memberName of Object.keys(members)) {
      let ownedByCube = true;
      let aliasMember;

      const member = members[memberName];
      if (member.sql && !member.subQuery) {
        const funcArgs = this.funcArguments(member.sql);
        const { cubeReferencesUsed, evaluatedSql, pathReferencesUsed } = this.collectUsedCubeReferences(cube.name, member.sql);
        // We won't check for FILTER_PARAMS here as it shouldn't affect ownership and it should obey the same reference rules.
        // To affect ownership FILTER_PARAMS can be declared as `${FILTER_PARAMS.Foo.bar.filter(`${Foo.bar}`)}`.
        // It isn't owned if there are non {CUBE} references
        if (funcArgs.length > 0 && cubeReferencesUsed.length === 0) {
          ownedByCube = false;
        }
        // Aliases one to one some another member as in case of views
        if (!ownedByCube && !member.filters && BaseQuery.isCalculatedMeasureType(member.type) && pathReferencesUsed.length === 1 && this.pathFromArray(pathReferencesUsed[0]) === evaluatedSql) {
          aliasMember = this.pathFromArray(pathReferencesUsed[0]);
        }
        const foreignCubes = cubeReferencesUsed.filter(usedCube => usedCube !== cube.name);
        if (foreignCubes.length > 0) {
          errorReporter.error(`Member '${cube.name}.${memberName}' references foreign cubes: ${foreignCubes.join(', ')}. Please split and move this definition to corresponding cubes.`);
        }
      }

      if (ownedByCube && cube.isView) {
        errorReporter.error(`View '${cube.name}' defines own member '${cube.name}.${memberName}'. Please move this member definition to one of the cubes.`);
      }

      members[memberName] = { ...members[memberName], ownedByCube };
      if (aliasMember) {
        members[memberName].aliasMember = aliasMember;
      }
    }
  }

  public cubesByFileName(fileName) {
    return this.byFileName[fileName] || [];
  }

  public timeDimensionPathsForCube(cube: any) {
    return R.compose(
      R.map(nameToDefinition => `${cube}.${nameToDefinition[0]}`),
      R.toPairs,
      // @ts-ignore
      R.filter((d: any) => d.type === 'time')
      // @ts-ignore
    )(this.evaluatedCubes[cube].dimensions || {});
  }

  public measuresForCube(cube) {
    return this.cubeFromPath(cube).measures || {};
  }

  public preAggregationsForCube(path: string) {
    return this.cubeFromPath(path).preAggregations || {};
  }

  /**
   * Returns pre-aggregations filtered by the spcified selector.
   * @param {{
   *  scheduled: boolean,
   *  dataSource: Array<string>,
   *  cubes: Array<string>,
   *  preAggregationIds: Array<string>
   * }} filter pre-aggregations selector
   * @returns {*}
   */
  public preAggregations(filter) {
    const { scheduled, dataSources, cubes, preAggregationIds } = filter || {};
    const idFactory = ({ cube, preAggregationName }) => `${cube}.${preAggregationName}`;

    return Object.keys(this.evaluatedCubes)
      .filter((cube) => (
        (
          !cubes ||
          (cubes && cubes.length === 0) ||
          cubes.includes(cube)
        ) && (
          !dataSources ||
          (dataSources && dataSources.length === 0) ||
          dataSources.includes(
            this.evaluatedCubes[cube].dataSource || 'default'
          )
        )
      ))
      .map(cube => {
        const preAggregations = this.preAggregationsForCube(cube);
        return Object.keys(preAggregations)
          .filter(
            preAggregationName => (
              !scheduled ||
              preAggregations[preAggregationName].scheduledRefresh
            ) && (
              !preAggregationIds ||
              (preAggregationIds && preAggregationIds.length === 0) ||
              preAggregationIds.includes(idFactory({
                cube, preAggregationName
              }))
            )
          )
          .map(preAggregationName => {
            const { indexes, refreshKey } = preAggregations[preAggregationName];
            return {
              id: idFactory({ cube, preAggregationName }),
              preAggregationName,
              preAggregation: preAggregations[preAggregationName],
              cube,
              references: this.evaluatePreAggregationReferences(cube, preAggregations[preAggregationName]),
              refreshKey,
              indexesReferences: indexes && Object.keys(indexes).reduce((obj, indexName) => {
                obj[indexName] = {
                  columns: this.evaluateReferences(
                    cube,
                    indexes[indexName].columns,
                    { originalSorting: true }
                  ),
                  type: indexes[indexName].type
                };
                return obj;
              }, {})
            };
          });
      })
      .reduce((a, b) => a.concat(b), []);
  }

  public scheduledPreAggregations() {
    return this.preAggregations({ scheduled: true });
  }

  public cubeNames() {
    return Object.keys(this.evaluatedCubes);
  }

  public isMeasure(measurePath: string): boolean {
    return this.isInstanceOfType('measures', measurePath);
  }

  public isDimension(path: string): boolean {
    return this.isInstanceOfType('dimensions', path);
  }

  public isSegment(path: string): boolean {
    return this.isInstanceOfType('segments', path);
  }

  public measureByPath(measurePath: string): MeasureDefinition {
    return this.byPath('measures', measurePath);
  }

  public dimensionByPath(dimensionPath: string): DimensionDefinition {
    return this.byPath('dimensions', dimensionPath);
  }

  public segmentByPath(segmentPath: string): SegmentDefinition {
    return this.byPath('segments', segmentPath);
  }

  public cubeExists(cube) {
    return !!this.evaluatedCubes[cube];
  }

  public cubeFromPath(path: string) {
    return this.evaluatedCubes[this.cubeNameFromPath(path)];
  }

  public cubeNameFromPath(path: string) {
    const cubeAndName = path.split('.');
    if (!this.evaluatedCubes[cubeAndName[0]]) {
      throw new UserError(`Cube '${cubeAndName[0]}' not found for path '${path}'`);
    }
    return cubeAndName[0];
  }

  public isInstanceOfType(type: 'measures' | 'dimensions' | 'segments', path: string | string[]): boolean {
    const cubeAndName = Array.isArray(path) ? path : path.split('.');
    return this.evaluatedCubes[cubeAndName[0]] &&
      this.evaluatedCubes[cubeAndName[0]][type] &&
      this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]];
  }

  public byPathAnyType(path: string[]) {
    if (this.isInstanceOfType('measures', path)) {
      return this.byPath('measures', path);
    }

    if (this.isInstanceOfType('dimensions', path)) {
      return this.byPath('dimensions', path);
    }

    if (this.isInstanceOfType('segments', path)) {
      return this.byPath('segments', path);
    }

    throw new UserError(`Can't resolve member '${path.join('.')}'`);
  }

  public byPath(type: 'measures' | 'dimensions' | 'segments', path: string | string[]) {
    if (!type) {
      throw new Error(`Type can't be undefined for '${path}'`);
    }

    if (!path) {
      throw new Error('Path can\'t be undefined');
    }

    const cubeAndName = Array.isArray(path) ? path : path.split('.');
    if (!this.evaluatedCubes[cubeAndName[0]]) {
      throw new UserError(`Cube '${cubeAndName[0]}' not found for path '${path}'`);
    }

    if (!this.evaluatedCubes[cubeAndName[0]][type]) {
      throw new UserError(`${type} not defined for path '${path}'`);
    }

    if (!this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]]) {
      throw new UserError(`'${cubeAndName[1]}' not found for path '${path}'`);
    }

    return this.evaluatedCubes[cubeAndName[0]][type][cubeAndName[1]];
  }

  public parsePath(type, path) {
    // Should throw UserError in case of parse error
    this.byPath(type, path);
    return path.split('.');
  }

  protected parsePathAnyType(path) {
    // Should throw UserError in case of parse error
    this.byPathAnyType(path);
    return path.split('.');
  }

  public collectUsedCubeReferences(cube: any, sqlFn: any) {
    const cubeEvaluator = this;

    const cubeReferencesUsed: string[] = [];
    const pathReferencesUsed: string[][] = [];

    const evaluatedSql = cubeEvaluator.resolveSymbolsCall(sqlFn, (name) => {
      const referencedCube = cubeEvaluator.symbols[name] && name || cube;
      const resolvedSymbol =
        cubeEvaluator.resolveSymbol(
          cube,
          name
        );
      // eslint-disable-next-line no-underscore-dangle
      if (resolvedSymbol._objectWithResolvedProperties) {
        return resolvedSymbol;
      }
      const path = [referencedCube, name];
      pathReferencesUsed.push(path);
      return cubeEvaluator.pathFromArray(path);
    }, {
      // eslint-disable-next-line no-shadow
      sqlResolveFn: (_symbol: unknown, cubeName: string, memberName: string) => {
        const path = [cubeName, memberName];
        pathReferencesUsed.push(path);
        return cubeEvaluator.pathFromArray(path);
      },
      contextSymbols: BaseQuery.emptyParametrizedContextSymbols(this, () => '$empty_param$'),
      cubeReferencesUsed,
    });
    return { cubeReferencesUsed, pathReferencesUsed, evaluatedSql };
  }

  protected evaluatePreAggregationReferences(cube, aggregation) {
    const timeDimensions = aggregation.timeDimensionReference ? [{
      dimension: this.evaluateReferences(cube, aggregation.timeDimensionReference),
      granularity: aggregation.granularity
    }] : [];
    return {
      allowNonStrictDateRangeMatch: aggregation.allowNonStrictDateRangeMatch,
      dimensions:
        (aggregation.dimensionReferences && this.evaluateReferences(cube, aggregation.dimensionReferences) || [])
          .concat(
            aggregation.segmentReferences && this.evaluateReferences(cube, aggregation.segmentReferences) || []
          ),
      measures:
        aggregation.measureReferences && this.evaluateReferences(cube, aggregation.measureReferences) || [],
      timeDimensions,
      rollups:
        aggregation.rollupReferences && this.evaluateReferences(cube, aggregation.rollupReferences, { originalSorting: true }) || [],
    };
  }
}
