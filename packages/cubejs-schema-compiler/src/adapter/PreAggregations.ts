import R from 'ramda';

import { CubeSymbols, PreAggregationDefinition } from '../compiler/CubeSymbols';
import { UserError } from '../compiler/UserError';
import { BaseQuery } from './BaseQuery';
import {
  PreAggregationDefinitions,
  PreAggregationReferences,
  PreAggregationTimeDimensionReference
} from '../compiler/CubeEvaluator';
import { BaseTimeDimension } from './BaseTimeDimension';
import { BaseMeasure } from './BaseMeasure';
import { BaseFilter } from './BaseFilter';
import { BaseGroupFilter } from './BaseGroupFilter';
import { BaseDimension } from './BaseDimension';
import { BaseSegment } from './BaseSegment';

export type RollupJoin = any;

export type PartitionTimeDimension = {
  dimension: string;
  dateRange: [string, string];
  boundaryDateRange: [string, string];
};

export type PreAggregationDefinitionExtended = PreAggregationDefinition & PreAggregationReferences & {
  unionWithSourceData: boolean;
  rollupLambdaId: string;
  lastRollupLambda: boolean;
  rollupLambdaTimeDimensionsReference: PreAggregationTimeDimensionReference[];
  readOnly: boolean;
  partitionGranularity: string;
  streamOffset: 'earliest' | 'latest';
  uniqueKeyColumns: string;
  sqlAlias?: string;
  partitionTimeDimensions?: PartitionTimeDimension[];
};

export type PreAggregationForQuery = {
  preAggregationName: string;
  cube: string;
  canUsePreAggregation: boolean;
  preAggregationId: string;
  preAggregation: PreAggregationDefinitionExtended;
  references: PreAggregationReferences;
  preAggregationsToJoin?: PreAggregationForQuery[];
  referencedPreAggregations?: PreAggregationForQuery[];
  rollupJoin?: RollupJoin;
  sqlAlias?: string;
};

export type PreAggregationForQueryWithTableName = PreAggregationForQuery & {
  tableName: string;
};

export type PreAggregationForCube = {
  preAggregationName: string;
  cube: string;
  preAggregation: PreAggregationDefinitionExtended;
  references: PreAggregationReferences;
};

export type EvaluateReferencesContext = {
  inPreAggEvaluation?: boolean;
};

export type BaseMember = BaseDimension | BaseMeasure | BaseFilter | BaseGroupFilter | BaseSegment;

export type CanUsePreAggregationFn = (references: PreAggregationReferences) => boolean;

/**
 * TODO: Write a real type.
 * @see return value of PreAggregations.preAggregationDescriptionFor()
 */
export type FullPreAggregationDescription = any;

/**
 * TODO: Write a real type.
 * @see return value of static PreAggregations.transformQueryToCanUseForm()
 */
export type TransformedQuery = any;

export class PreAggregations {
  private readonly query: BaseQuery;

  private readonly historyQueries: any;

  private readonly cubeLatticeCache: any;

  private readonly cubeLattices: {};

  private hasCumulativeMeasuresValue: boolean = false;

  public preAggregationForQuery: PreAggregationForQuery | undefined = undefined;

  public constructor(query: BaseQuery, historyQueries, cubeLatticeCache) {
    this.query = query;
    this.historyQueries = historyQueries;
    this.cubeLatticeCache = cubeLatticeCache;
    this.cubeLattices = {};
  }

  /**
   * It returns full pre-aggregation object (with keyQueries, previewSql, loadSql, and so on.
   */
  public preAggregationsDescription(): FullPreAggregationDescription[] {
    const preAggregations = [this.preAggregationsDescriptionLocal()].concat(
      this.query.subQueryDimensions.map(d => this.query.subQueryDescription(d).subQuery)
        .map(q => q.preAggregations.preAggregationsDescription())
    );

    return R.pipe(
      R.unnest as (list: any[][]) => any[],
      R.uniqBy(desc => desc.tableName)
    )(
      preAggregations
    );
  }

  private preAggregationsDescriptionLocal(): FullPreAggregationDescription[] {
    const isInPreAggregationQuery = this.query.options.preAggregationQuery;
    if (!isInPreAggregationQuery) {
      const preAggregationForQuery = this.findPreAggregationForQuery();
      if (preAggregationForQuery) {
        return this.preAggregationDescriptionsFor(preAggregationForQuery);
      }
    }
    if (
      !isInPreAggregationQuery ||
      isInPreAggregationQuery && this.query.options.useOriginalSqlPreAggregationsInPreAggregation) {
      return R.pipe(
        R.map((cube: string) => {
          const { preAggregations } = this.collectOriginalSqlPreAggregations(() => this.query.cubeSql(cube));
          return R.unnest(preAggregations.map(p => this.preAggregationDescriptionsFor(p)));
        }),
        R.filter((x): boolean => Boolean(x)),
        R.unnest
      )(this.preAggregationCubes());
    }
    return [];
  }

  private preAggregationCubes(): string[] {
    const { join } = this.query;
    if (!join) {
      // This can happen with Tesseract, or when there's no cubes to join
      throw new Error('Unexpected missing join tree for query');
    }
    return join.joins.map(j => j.originalTo).concat([join.root]);
  }

  private preAggregationDescriptionsFor(foundPreAggregation: PreAggregationForQuery): FullPreAggregationDescription[] {
    let preAggregations: PreAggregationForQuery[] = [foundPreAggregation];
    if (foundPreAggregation.preAggregation.type === 'rollupJoin') {
      preAggregations = foundPreAggregation.preAggregationsToJoin || [];
    }
    if (foundPreAggregation.preAggregation.type === 'rollupLambda') {
      preAggregations = foundPreAggregation.referencedPreAggregations || [];
    }

    return preAggregations.map(preAggregation => {
      if (this.canPartitionsBeUsed(preAggregation)) {
        const { dimension, partitionDimension } = this.partitionDimension(preAggregation);
        return this.preAggregationDescriptionsForRecursive(
          preAggregation.cube, this.addPartitionRangeTo(
            preAggregation,
            dimension,
            partitionDimension.wildcardRange(),
            partitionDimension.boundaryDateRange || partitionDimension.dateRange
          )
        );
      }
      return this.preAggregationDescriptionsForRecursive(preAggregation.cube, preAggregation);
    }).reduce((a, b) => a.concat(b), []);
  }

  private canPartitionsBeUsed(foundPreAggregation: PreAggregationForQuery): boolean {
    return !!foundPreAggregation.preAggregation.partitionGranularity &&
      !!foundPreAggregation.references.timeDimensions?.length;
  }

  private addPartitionRangeTo(foundPreAggregation: PreAggregationForQuery, dimension, range, boundaryDateRange) {
    return {
      ...foundPreAggregation,
      preAggregation: {
        ...foundPreAggregation.preAggregation,
        partitionTimeDimensions: [{
          dimension,
          dateRange: range,
          boundaryDateRange
        }],
      }
    };
  }

  private partitionDimension(foundPreAggregation: PreAggregationForQuery): { dimension: string, partitionDimension: BaseTimeDimension } {
    const { dimension } = foundPreAggregation.references.timeDimensions[0];
    const partitionDimension = this.query.newTimeDimension({
      dimension,
      granularity: this.castGranularity(foundPreAggregation.preAggregation.partitionGranularity),
      dateRange: this.query.timeDimensions[0]?.dateRange,
      boundaryDateRange: this.query.timeDimensions[0]?.boundaryDateRange
    });
    return { dimension, partitionDimension };
  }

  private preAggregationDescriptionsForRecursive(cube: string, foundPreAggregation: PreAggregationForQuery): FullPreAggregationDescription[] {
    const query = this.query.preAggregationQueryForSqlEvaluation(cube, foundPreAggregation.preAggregation);
    const descriptions = query !== this.query ? query.preAggregations.preAggregationsDescription() : [];
    return descriptions.concat(this.preAggregationDescriptionFor(cube, foundPreAggregation));
  }

  private hasCumulativeMeasures(): boolean {
    if (!this.hasCumulativeMeasuresValue) {
      this.hasCumulativeMeasuresValue = PreAggregations.hasCumulativeMeasures(this.query);
    }
    return this.hasCumulativeMeasuresValue;
  }

  // Return array of `aggregations` columns descriptions in form `<func>(<column>)`
  // Aggregations used in CubeStore create table for describe measures in CubeStore side
  public aggregationsColumns(cube: string, preAggregation: PreAggregationDefinition): string[] {
    if (preAggregation.type === 'rollup') {
      return this.query
        .preAggregationQueryForSqlEvaluation(cube, preAggregation)
        .measures
        .filter(m => m.isAdditive())
        .map(m => {
          const fname = {
            sum: 'sum',
            count: 'sum',
            countDistinctApprox: 'merge',
            min: 'min',
            max: 'max'
          }[m.measureDefinition().type];
          return `${fname}(${m.aliasName()})`;
        });
    }
    return [];
  }

  private preAggregationDescriptionFor(cube: string, foundPreAggregation: PreAggregationForQuery): FullPreAggregationDescription {
    const { preAggregationName, preAggregation, references } = foundPreAggregation;

    const tableName = this.preAggregationTableName(cube, preAggregationName, preAggregation);
    const invalidateKeyQueries = this.query.preAggregationInvalidateKeyQueries(cube, preAggregation, preAggregationName);
    const queryForSqlEvaluation = this.query.preAggregationQueryForSqlEvaluation(cube, preAggregation);
    // Atm this is only defined in KsqlQuery but without it partitions are recreated on every refresh
    const partitionInvalidateKeyQueries = queryForSqlEvaluation.partitionInvalidateKeyQueries?.(cube, preAggregation);

    const allBackAliasMembers = this.query.allBackAliasMembers();

    let matchedTimeDimension: BaseTimeDimension | undefined;

    if (preAggregation.partitionGranularity && !this.hasCumulativeMeasures()) {
      matchedTimeDimension = this.query.timeDimensions.find(td => {
        if (!td.dateRange) {
          return false;
        }

        const timeDimensionsReference =
          foundPreAggregation.preAggregation.rollupLambdaTimeDimensionsReference ||
          foundPreAggregation.references.timeDimensions;
        const timeDimensionReference = timeDimensionsReference[0];

        // timeDimensionsReference[*].dimension can contain full join path, so we should trim it
        const timeDimensionReferenceDimension = CubeSymbols.joinHintFromPath(timeDimensionReference.dimension).path;

        if (td.dimension === timeDimensionReferenceDimension) {
          return true;
        }

        // Handling for views
        return td.dimension === allBackAliasMembers[timeDimensionReferenceDimension];
      });
    }

    let filters: BaseFilter[] | undefined;

    if (preAggregation.partitionGranularity) {
      filters = this.query.filters?.filter((td): td is BaseFilter => {
        // TODO support all date operators
        if (td.isDateOperator() && 'camelizeOperator' in td && td.camelizeOperator === 'inDateRange') {
          if (td.dimension === foundPreAggregation.references.timeDimensions[0].dimension) {
            return true;
          }

          // Handling for views
          return td.dimension === allBackAliasMembers[foundPreAggregation.references.timeDimensions[0].dimension];
        }

        return false;
      });
    }

    const uniqueKeyColumnsDefault = () => null;
    const uniqueKeyColumns = ({
      rollup: () => queryForSqlEvaluation.preAggregationUniqueKeyColumns(cube, preAggregation),
      originalSql: () => preAggregation.uniqueKeyColumns || null
    }[preAggregation.type] || uniqueKeyColumnsDefault)();

    const aggregationsColumns = this.aggregationsColumns(cube, preAggregation);

    return {
      preAggregationId: `${cube}.${preAggregationName}`,
      timezone: this.query.options?.timezone,
      timestampFormat: queryForSqlEvaluation.timestampFormat(),
      timestampPrecision: queryForSqlEvaluation.timestampPrecision(),
      tableName,
      invalidateKeyQueries,
      partitionInvalidateKeyQueries,
      type: preAggregation.type,
      external: preAggregation.external,
      previewSql: queryForSqlEvaluation.preAggregationPreviewSql(tableName),
      preAggregationsSchema: queryForSqlEvaluation.preAggregationSchema(),
      loadSql: queryForSqlEvaluation.preAggregationLoadSql(cube, preAggregation, tableName),
      sql: queryForSqlEvaluation.preAggregationSql(cube, preAggregation),
      outputColumnTypes: queryForSqlEvaluation.preAggregationOutputColumnTypes(cube, preAggregation),
      uniqueKeyColumns,
      aggregationsColumns,
      dataSource: queryForSqlEvaluation.dataSource,
      // in fact, we can reference preAggregation.granularity however accessing timeDimensions is more strict and consistent
      granularity: references.timeDimensions[0]?.granularity,
      partitionGranularity: preAggregation.partitionGranularity,
      updateWindowSeconds: preAggregation.refreshKey &&
        'updateWindow' in preAggregation.refreshKey &&
        preAggregation.refreshKey?.updateWindow &&
        queryForSqlEvaluation.parseSecondDuration(preAggregation.refreshKey.updateWindow),
      preAggregationStartEndQueries:
        (preAggregation.partitionGranularity || references.timeDimensions[0]?.granularity) &&
        this.refreshRangeQuery(cube).preAggregationStartEndQueries(cube, preAggregation),
      matchedTimeDimensionDateRange:
        preAggregation.partitionGranularity && (
          matchedTimeDimension?.boundaryDateRangeFormatted() ||
          filters?.[0]?.formattedDateRange() // TODO intersect all date ranges
        ),
      indexesSql: Object.keys(preAggregation.indexes || {})
        .map(
          index => {
            // @todo Dont use sqlAlias directly, we needed to move it in preAggregationTableName
            const indexName = this.preAggregationTableName(cube, `${foundPreAggregation.sqlAlias || preAggregationName}_${index}`, preAggregation, true);
            return {
              indexName,
              sql: queryForSqlEvaluation.indexSql(
                cube,
                preAggregation,
                preAggregation.indexes?.[index],
                indexName,
                tableName
              )
            };
          }
        ),
      createTableIndexes: Object.keys(preAggregation.indexes || {})
        .map(
          index => {
            // @todo Dont use sqlAlias directly, we needed to move it in preAggregationTableName
            const indexName = this.preAggregationTableName(cube, `${foundPreAggregation.sqlAlias || preAggregationName}_${index}`, preAggregation, true);
            return {
              indexName,
              type: preAggregation.indexes?.[index].type,
              columns: queryForSqlEvaluation.evaluateIndexColumns(cube, preAggregation.indexes?.[index])
            };
          }
        ),
      readOnly: preAggregation.readOnly || queryForSqlEvaluation.preAggregationReadOnly(cube, preAggregation),
      streamOffset: preAggregation.streamOffset,
      unionWithSourceData: preAggregation.unionWithSourceData,
      rollupLambdaId: preAggregation.rollupLambdaId,
      lastRollupLambda: preAggregation.lastRollupLambda,
    };
  }

  private preAggregationTableName(cube: string, preAggregationName: string, preAggregation: PreAggregationDefinitionExtended | PreAggregationForQuery, skipSchema: boolean = false): string {
    const name = preAggregation.sqlAlias || preAggregationName;
    return this.query.preAggregationTableName(
      cube,
      name,
      skipSchema
    );
  }

  public findPreAggregationToUseForCube(cube: string): PreAggregationForCube | null {
    const preAggregates = this.query.cubeEvaluator.preAggregationsForCube(cube);
    const originalSqlPreAggregations = R.pipe(
      R.toPairs,
      R.filter(([, a]) => a.type === 'originalSql')
    )(preAggregates);
    if (originalSqlPreAggregations.length) {
      const [preAggregationName, preAggregation] = originalSqlPreAggregations[0];
      return {
        preAggregationName,
        preAggregation,
        cube,
        references: this.evaluateAllReferences(cube, preAggregation, preAggregationName)
      };
    }
    return null;
  }

  public static transformQueryToCanUseForm(query: BaseQuery): TransformedQuery {
    const allBackAliasMembers = query.allBackAliasMembers();
    const resolveFullMemberPath = query.resolveFullMemberPathFn();
    const flattenDimensionMembers = this.flattenDimensionMembers(query);
    const sortedDimensions = this.squashDimensions(flattenDimensionMembers).map(resolveFullMemberPath);
    const measures: (BaseMeasure | BaseFilter | BaseGroupFilter)[] = [...query.measures, ...query.measureFilters];
    const measurePaths = (R.uniq(
      this.flattenMembers(measures)
        .filter((m): m is BaseMeasure => m instanceof BaseMeasure)
        .map(m => m.expressionPath())
    )).map(resolveFullMemberPath);
    const collectLeafMeasures = query.collectLeafMeasures.bind(query);
    const dimensionsList = query.dimensions.map(dim => dim.expressionPath());
    const segmentsList = query.segments.map(s => s.expressionPath());
    const ownedDimensions = PreAggregations.ownedMembers(query, flattenDimensionMembers).map(resolveFullMemberPath);
    const ownedTimeDimensions = query.timeDimensions.map(td => {
      const owned = PreAggregations.ownedMembers(query, [td]);
      let { dimension } = td;
      // TODO If there's more than one owned time dimension for the given input time dimension then it's some
      // TODO kind of calculation which isn't supported yet
      if (owned.length === 1) {
        [dimension] = owned;
      }
      return query.newTimeDimension({
        dimension,
        dateRange: td.dateRange,
        granularity: td.granularity,
      });
    });

    let sortedAllCubeNames;
    let sortedUsedCubePrimaryKeys;

    if (query.ungrouped) {
      const { allCubeNames } = query;
      sortedAllCubeNames = allCubeNames.concat([]);
      sortedAllCubeNames.sort();
      sortedUsedCubePrimaryKeys = query.allCubeNames.flatMap(c => query.primaryKeyNames(c));
      sortedUsedCubePrimaryKeys.sort();
    }

    const measureToLeafMeasures = {};

    const leafMeasurePaths =
      R.pipe(
        R.map((m: { measure: string }) => {
          const leafMeasures = query.collectFrom([m], collectLeafMeasures, 'collectLeafMeasures');
          measureToLeafMeasures[m.measure] = leafMeasures.map((measure) => {
            const baseMeasure = query.newMeasure(measure);

            return {
              measure,
              additive: baseMeasure.isAdditive(),
              type: baseMeasure.definition().type
            };
          });

          return leafMeasures;
        }),
        R.unnest,
        R.uniq,
        R.map(resolveFullMemberPath),
      )(measures);

    function allValuesEq1(map) {
      if (!map) return false;
      // eslint-disable-next-line no-restricted-syntax
      for (const v of map?.values()) {
        if (v !== 1) return false;
      }
      return true;
    }

    const sortedTimeDimensions = PreAggregations.sortTimeDimensionsWithRollupGranularity(query.timeDimensions);
    const timeDimensions = PreAggregations.timeDimensionsAsIs(query.timeDimensions);
    const ownedTimeDimensionsWithRollupGranularity = PreAggregations.sortTimeDimensionsWithRollupGranularity(ownedTimeDimensions)
      .map(([d, g]) => [resolveFullMemberPath(d), g]);
    const ownedTimeDimensionsAsIs = PreAggregations.timeDimensionsAsIs(ownedTimeDimensions)
      .map(([d, g]) => [resolveFullMemberPath(d), g]);

    const hasNoTimeDimensionsWithoutGranularity = !query.timeDimensions.filter(d => !d.granularity).length;

    const allFiltersWithinSelectedDimensions =
      R.all((d: string) => dimensionsList.indexOf(d) !== -1)(
        R.flatten(
          query.filters.map(f => f.getMembers())
        ).map(f => query.cubeEvaluator.pathFromArray(f.path()))
      );

    const isAdditive = R.all(m => m.isAdditive(), query.measures);
    const hasMultiStage = R.any(m => m.isMultiStage(), query.measures);
    const leafMeasures = leafMeasurePaths.map(path => query.newMeasure(path));
    const leafMeasureAdditive = R.all(m => m.isAdditive(), leafMeasures);
    const cumulativeMeasures = leafMeasures
      .filter(m => m.isCumulative());
    const hasCumulativeMeasures = cumulativeMeasures.length > 0;
    const windowGranularity = cumulativeMeasures
      .map(m => m.windowGranularity())
      .reduce((a, b) => query.minGranularity(a, b), null);
    const granularityHierarchies = query.granularityHierarchies();
    const hasMultipliedMeasures = query.fullKeyQueryAggregateMeasures({ hasMultipliedForPreAggregation: true }).multipliedMeasures.length > 0;

    let filterDimensionsSingleValueEqual = this.collectFilterDimensionsWithSingleValueEqual(
      query.filters,
      dimensionsList.concat(segmentsList).reduce((map, d) => map.set(d, 1), new Map()),
    );

    filterDimensionsSingleValueEqual =
      allValuesEq1(filterDimensionsSingleValueEqual) ? new Set(filterDimensionsSingleValueEqual?.keys()) : null;

    return {
      sortedDimensions,
      sortedTimeDimensions,
      timeDimensions,
      measures: measurePaths,
      leafMeasureAdditive,
      leafMeasures: leafMeasurePaths,
      measureToLeafMeasures,
      hasNoTimeDimensionsWithoutGranularity,
      allFiltersWithinSelectedDimensions,
      isAdditive,
      granularityHierarchies,
      hasMultipliedMeasures,
      hasCumulativeMeasures,
      windowGranularity,
      filterDimensionsSingleValueEqual,
      ownedDimensions,
      ownedTimeDimensionsWithRollupGranularity,
      ownedTimeDimensionsAsIs,
      allBackAliasMembers,
      ungrouped: query.ungrouped,
      sortedUsedCubePrimaryKeys,
      sortedAllCubeNames,
      hasMultiStage
    };
  }

  public static ownedMembers(query: BaseQuery, members): string[] {
    return R.pipe(R.uniq, R.sortBy(R.identity))(
      query
        .collectFrom(members, query.collectMemberNamesFor.bind(query), 'collectMemberNamesFor')
        .filter(d => query.cubeEvaluator.byPathAnyType(d).ownedByCube)
    );
  }

  public static sortTimeDimensionsWithRollupGranularity(timeDimensions: BaseTimeDimension[] | undefined): [expressionPath: string, rollupGranularity: string | null][] {
    return timeDimensions && R.sortBy(
      ([exprPath]) => exprPath,
      timeDimensions.map(d => [d.expressionPath(), d.rollupGranularity()] as [string, string | null])
    ) || [];
  }

  public static timeDimensionsAsIs(timeDimensions: BaseTimeDimension[] | undefined): [expressionPath: string, resolvedGranularity: string | null][] {
    return timeDimensions && R.sortBy(
      ([exprPath]) => exprPath,
      timeDimensions.map(d => [d.expressionPath(), d.resolvedGranularity()] as [string, string | null]),
    ) || [];
  }

  public static collectFilterDimensionsWithSingleValueEqual(filters, map) {
    // eslint-disable-next-line no-restricted-syntax
    for (const f of filters) {
      if (f.operator === 'equals') {
        map.set(f.expressionPath(), Math.min(map.get(f.expressionPath()) || 2, f.values.length));
      } else if (f.operator === 'and') {
        const res = this.collectFilterDimensionsWithSingleValueEqual(f.values, map);
        if (res == null) return null;
      } else {
        return null;
      }
    }

    return map;
  }

  // FIXME: It seems to be not used at all
  public static transformedQueryToReferences(query) {
    return {
      measures: query.measures,
      dimensions: query.sortedDimensions,
      timeDimensions: query.sortedTimeDimensions.map(([dimension, granularity]) => ({ dimension, granularity }))
    };
  }

  private canUsePreAggregationFn(query: BaseQuery, refs: PreAggregationReferences | null = null) {
    return PreAggregations.canUsePreAggregationForTransformedQueryFn(
      PreAggregations.transformQueryToCanUseForm(query),
      refs,
    );
  }

  /**
   * Returns function to determine whether pre-aggregation can be used or not
   * for specified query, or its value for `refs` if specified.
   */
  public static canUsePreAggregationForTransformedQueryFn(transformedQuery: TransformedQuery, refs: PreAggregationReferences | null = null): CanUsePreAggregationFn {
    /**
     * Returns an array of 2-elements arrays with the dimension and granularity
     * sorted by the concatenated dimension + granularity key.
     */
    const sortTimeDimensions = (timeDimensions: ({ dimension: string; granularity?: string }[] | undefined)): [string, string][] => (
      timeDimensions &&
      R.sortBy(
        d => d.join('.'),
        timeDimensions.map(
          d => [
            d.dimension,
            d.granularity || 'day', // TODO granularity shouldn't be null?
          ]
        ),
      ) || []
    );

    const filterDimensionsSingleValueEqual: Set<string> =
      transformedQuery.filterDimensionsSingleValueEqual && (transformedQuery.filterDimensionsSingleValueEqual instanceof Set
        ? transformedQuery.filterDimensionsSingleValueEqual
        : new Set(
          Object.keys(
            transformedQuery.filterDimensionsSingleValueEqual || {},
          )
        ));

    const backAlias = (references: ([string, string | undefined] | string)[]) => references.map(r => (
      Array.isArray(r) ?
        [transformedQuery.allBackAliasMembers[r[0]] || r[0], r[1]] :
        transformedQuery.allBackAliasMembers[r] || r
    ));

    /**
     * Determine whether pre-aggregation can be used or not.
     */
    const canUsePreAggregationNotAdditive: CanUsePreAggregationFn = (references: PreAggregationReferences): boolean => {
      const refTimeDimensions = backAlias(sortTimeDimensions(references.timeDimensions));
      const qryTimeDimensions = references.allowNonStrictDateRangeMatch
        ? transformedQuery.timeDimensions
        : transformedQuery.sortedTimeDimensions;
      const backAliasMeasures = backAlias(references.measures);
      const backAliasDimensions = backAlias(references.dimensions);
      return ((
        transformedQuery.hasNoTimeDimensionsWithoutGranularity
      ) && (
        !transformedQuery.hasCumulativeMeasures
      ) && (
        R.equals(qryTimeDimensions, refTimeDimensions)
      ) && (
        transformedQuery.isAdditive ||
        R.equals(transformedQuery.timeDimensions, refTimeDimensions)
      ) && (
        filterDimensionsSingleValueEqual &&
        references.dimensions.length === filterDimensionsSingleValueEqual.size &&
        R.all(d => filterDimensionsSingleValueEqual.has(d), backAliasDimensions) ||
        transformedQuery.allFiltersWithinSelectedDimensions &&
        R.equals(backAliasDimensions, transformedQuery.sortedDimensions)
      ) && (
        R.all(m => backAliasMeasures.indexOf(m) !== -1, transformedQuery.measures) ||
        // TODO do we need backAlias here?
        R.all(m => backAliasMeasures.indexOf(m) !== -1, transformedQuery.leafMeasures)
      ));
    };

    /**
     * Expand granularity into array of granularity hierarchy.
     */
    const expandGranularity = (dimension: string, granularity: string): Array<string> => (
      transformedQuery.granularityHierarchies[`${dimension}.${granularity}`] ||
      [granularity]
    );

    /**
     * Determine whether time dimensions match to the window granularity or not.
     */
    const windowGranularityMatches = (references: PreAggregationReferences): boolean => {
      if (!transformedQuery.windowGranularity) {
        return true;
      }
      // Beware that sortedTimeDimensions contain full join paths
      const sortedTimeDimensions = sortTimeDimensions(references.timeDimensions);

      return sortedTimeDimensions
        .map(td => expandGranularity(td[0], transformedQuery.windowGranularity))
        .some(
          expandedGranularities => expandedGranularities.some(
            windowGranularity => sortedTimeDimensions.every(
              td => td[1] === windowGranularity
            )
          )
        );
    };

    /**
     * Returns an array of 2-element arrays with dimension and granularity.
     */
    const expandTimeDimension = (timeDimension: string[]): string[][] => {
      const [dimension, resolvedGranularity] = timeDimension;
      if (!resolvedGranularity) {
        return [[dimension, '*']]; // Any granularity should fit
      }
      const trimmedDim = dimension.split('.').slice(-2).join('.');
      return expandGranularity(trimmedDim, resolvedGranularity)
        .map((newGranularity) => [dimension, newGranularity]);
    };

    const canUsePreAggregationLeafMeasureAdditive: CanUsePreAggregationFn = (references): boolean => {
      /**
       * Array of 2-element arrays with dimension and granularity.
       * @type {Array<Array<string>>}
       */
      const queryTimeDimensionsList = references.allowNonStrictDateRangeMatch
        ? transformedQuery.timeDimensions.map(expandTimeDimension)
        : transformedQuery.sortedTimeDimensions.map(expandTimeDimension);

      const ownedQueryTimeDimensionsList = references.allowNonStrictDateRangeMatch
        ? transformedQuery.ownedTimeDimensionsAsIs.map(expandTimeDimension)
        : transformedQuery.ownedTimeDimensionsWithRollupGranularity.map(expandTimeDimension);

      // Even if there are no multiplied measures in the query (because no multiplier dimensions are requested)
      // but the same measures are multiplied in the pre-aggregation, we can't use pre-aggregation
      // for such queries.
      if (references.multipliedMeasures) {
        const backAliasMultipliedMeasures = backAlias(references.multipliedMeasures);

        if (transformedQuery.leafMeasures.some(m => references.multipliedMeasures?.includes(m)) ||
          transformedQuery.measures.some(m => backAliasMultipliedMeasures.includes(m))
        ) {
          return false;
        }
      }

      // In 'rollupJoin' / 'rollupLambda' pre-aggregations fullName members will be empty, because there are
      // no connections in the joinTree between cubes from different datasources
      const dimsToMatch = references.fullNameDimensions.length > 0 ? references.fullNameDimensions : references.dimensions;

      const dimensionsMatch = (dimensions, doBackAlias) => R.all(
        d => (
          doBackAlias ?
            backAlias(dimsToMatch) :
            (dimsToMatch)
        ).indexOf(d) !== -1,
        dimensions
      );

      // In 'rollupJoin' / 'rollupLambda' pre-aggregations fullName members will be empty, because there are
      // no connections in the joinTree between cubes from different datasources
      const timeDimsToMatch = references.fullNameTimeDimensions.length > 0 ? references.fullNameTimeDimensions : references.timeDimensions;

      const timeDimensionsMatch = (timeDimensionsList, doBackAlias) => R.allPass(
        timeDimensionsList.map(
          tds => R.anyPass(tds.map((td: [string, string]) => {
            if (td[1] === '*') {
              return R.any((tdtc: [string, string]) => tdtc[0] === td[0]); // need to match the dimension at least
            } else {
              return R.includes(td);
            }
          }))
        )
      )(
        doBackAlias ?
          backAlias(sortTimeDimensions(timeDimsToMatch)) :
          (sortTimeDimensions(timeDimsToMatch))
      );

      if (transformedQuery.ungrouped) {
        const allReferenceCubes = R.pipe(R.map((name: string) => name?.split('.')[0]), R.uniq, R.sortBy(R.identity))([
          ...references.measures.map(m => CubeSymbols.joinHintFromPath(m).path),
          ...references.dimensions.map(d => CubeSymbols.joinHintFromPath(d).path),
          ...references.timeDimensions.map(td => CubeSymbols.joinHintFromPath(td.dimension).path),
        ]);
        if (
          !R.equals(transformedQuery.sortedAllCubeNames, allReferenceCubes) ||
          !(
            dimensionsMatch(transformedQuery.sortedUsedCubePrimaryKeys, true) || dimensionsMatch(transformedQuery.sortedUsedCubePrimaryKeys, false)
          )
        ) {
          return false;
        }
      }

      const backAliasMeasures = backAlias(references.measures);
      return ((
        windowGranularityMatches(references)
      ) && (
        R.all(
          (m: string) => references.measures.indexOf(m) !== -1,
          transformedQuery.leafMeasures,
        ) || R.all(
          m => backAliasMeasures.indexOf(m) !== -1,
          transformedQuery.measures,
        )
      ) && (
        dimensionsMatch(transformedQuery.sortedDimensions, true) && timeDimensionsMatch(queryTimeDimensionsList, true) ||
        dimensionsMatch(transformedQuery.ownedDimensions, false) && timeDimensionsMatch(ownedQueryTimeDimensionsList, false)
      ));
    };

    const canUseFn: CanUsePreAggregationFn =
      (
        transformedQuery.leafMeasureAdditive && !transformedQuery.hasMultipliedMeasures && !transformedQuery.hasMultiStage || transformedQuery.ungrouped
      ) ? ((r: PreAggregationReferences): boolean => canUsePreAggregationLeafMeasureAdditive(r) ||
          canUsePreAggregationNotAdditive(r))
        : canUsePreAggregationNotAdditive;

    if (refs) {
      // @ts-ignore TS think it is boolean here
      return canUseFn(refs);
    } else {
      return canUseFn;
    }
  }

  private static squashDimensions(flattenDimensionMembers: BaseMember[]): string[] {
    return R.pipe(R.uniq, R.sortBy(R.identity))(
      flattenDimensionMembers
        .filter((member: BaseMember): member is BaseMeasure => typeof (member as any).expressionPath === 'function')
        .map(d => d.expressionPath())
    );
  }

  private static flattenMembers(members: BaseMember[]): BaseMember[] {
    return R.flatten(
      members.map(m => m.getMembers()),
    );
  }

  private static flattenDimensionMembers(query: BaseQuery): BaseMember[] {
    return this.flattenMembers([
      ...query.dimensions,
      ...query.filters,
      ...query.segments,
    ]);
  }

  public getCubeLattice(_cube, _preAggregationName, _preAggregation): unknown {
    throw new UserError('Auto rollups supported only in Enterprise version');
  }

  /**
   * Returns pre-agg which determined as applicable for the query (the first one
   * from the list of potentially applicable pre-aggs). The order of the
   * potentially applicable pre-aggs is the same as the order in which these
   * pre-aggs appear in the schema file.
   */
  public findPreAggregationForQuery(): PreAggregationForQuery | undefined {
    if (!this.preAggregationForQuery) {
      if (this.query.useNativeSqlPlanner && this.query.canUseNativeSqlPlannerPreAggregation) {
        this.preAggregationForQuery = this.query.findPreAggregationForQueryRust();
      } else {
        this.preAggregationForQuery =
          this
            .rollupMatchResults()
            // Refresh worker can access specific pre-aggregations even in case those hidden by others
            .find(p => p.canUsePreAggregation && (!this.query.options.preAggregationId || p.preAggregationId === this.query.options.preAggregationId));
      }
    }
    return this.preAggregationForQuery;
  }

  private findAutoRollupPreAggregationsForCube(cube: string, preAggregations: PreAggregationDefinitions): PreAggregationForQuery[] {
    if (
      this.query.measures.some((m) => {
        const path = m.path();
        return path !== null && path[0] === cube;
      }) ||
      !this.query.measures.length && !this.query.timeDimensions.length &&
      this.query.dimensions.every((d) => {
        const path = d.path();
        return path !== null && path[0] === cube;
      })
    ) {
      return R.pipe(
        R.toPairs,
        // eslint-disable-next-line no-unused-vars
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        R.filter(([k, a]) => a.type === 'autoRollup'),
        R.map(([preAggregationName, preAggregation]) => {
          const cubeLattice: any = this.getCubeLattice(cube, preAggregationName, preAggregation);
          const optimalPreAggregation = cubeLattice.findOptimalPreAggregationFromLattice(this.query);
          return optimalPreAggregation && {
            preAggregationName: preAggregationName + this.autoRollupNameSuffix(optimalPreAggregation),
            preAggregation: Object.assign(
              optimalPreAggregation,
              preAggregation
            ),
            cube,
            canUsePreAggregation: true,
            references: optimalPreAggregation
          };
        })
      )(preAggregations);
    }
    return [];
  }

  /**
   * Returns an array of potentially applicable for the query pre-aggs in the
   * same order they appear in the schema file.
   */
  private rollupMatchResults(): PreAggregationForQuery[] {
    const { query } = this;

    const canUsePreAggregation = this.canUsePreAggregationFn(query);

    return R.pipe(
      R.map((cube: string) => {
        const preAggregations =
          this.query.cubeEvaluator.preAggregationsForCube(cube);

        let rollupPreAggregations =
          this.findRollupPreAggregationsForCube(
            cube,
            canUsePreAggregation,
            preAggregations,
          );

        rollupPreAggregations = rollupPreAggregations.concat(
          this.findAutoRollupPreAggregationsForCube(
            cube,
            preAggregations,
          ),
        );

        return rollupPreAggregations;
      }),
      R.unnest
    )(query.collectCubeNames());
  }

  private findRollupPreAggregationsForCube(cube: string, canUsePreAggregation: CanUsePreAggregationFn, preAggregations: PreAggregationDefinitions): PreAggregationForQuery[] {
    return R.pipe(
      R.toPairs,
      R.filter(([_k, a]) => a.type === 'rollup' || a.type === 'rollupJoin' || a.type === 'rollupLambda'),
      R.map(([preAggregationName, preAggregation]) => this.evaluatedPreAggregationObj(cube, preAggregationName, preAggregation, canUsePreAggregation))
    )(preAggregations);
  }

  public getRollupPreAggregationByName(cube, preAggregationName): PreAggregationForQueryWithTableName | {} {
    const canUsePreAggregation = () => true;
    const preAggregation = R.pipe(
      R.toPairs,
      R.filter(([_, a]) => a.type === 'rollup' || a.type === 'rollupJoin' || a.type === 'rollupLambda'),
      R.find(([k, _]) => k === preAggregationName)
    )(this.query.cubeEvaluator.preAggregationsForCube(cube));
    if (preAggregation) {
      const tableName = this.preAggregationTableName(cube, preAggregation[0], preAggregation[1]);
      const preAggObj = preAggregation ? this.evaluatedPreAggregationObj(cube, preAggregation[0], preAggregation[1], canUsePreAggregation) : {};
      return {
        tableName,
        ...preAggObj
      };
    } else {
      return {};
    }
  }

  // TODO check multiplication factor didn't change
  private buildRollupJoin(preAggObj: PreAggregationForQuery, preAggObjsToJoin: PreAggregationForQuery[]): RollupJoin {
    return this.query.cacheValue(
      ['buildRollupJoin', JSON.stringify(preAggObj), JSON.stringify(preAggObjsToJoin)],
      () => {
        const targetJoins = this.resolveJoinMembers(
          // TODO join hints?
          this.query.joinGraph.buildJoin(this.cubesFromPreAggregation(preAggObj))
        );
        const existingJoins = R.unnest(preAggObjsToJoin.map(
          // TODO join hints?
          p => this.resolveJoinMembers(this.query.joinGraph.buildJoin(this.cubesFromPreAggregation(p)))
        ));
        const nonExistingJoins = targetJoins.filter(target => !existingJoins.find(
          existing => existing.originalFrom === target.originalFrom &&
            existing.originalTo === target.originalTo &&
            R.equals(existing.fromMembers, target.fromMembers) &&
            R.equals(existing.toMembers, target.toMembers)
        ));
        if (!nonExistingJoins.length) {
          throw new UserError(`Nothing to join in rollup join. Target joins ${JSON.stringify(targetJoins)} are included in existing rollup joins ${JSON.stringify(existingJoins)}`);
        }
        return nonExistingJoins.map(join => {
          const fromPreAggObj = this.preAggObjForJoin(preAggObjsToJoin, join.fromMembers, join);
          const toPreAggObj = this.preAggObjForJoin(preAggObjsToJoin, join.toMembers, join);
          return {
            ...join,
            fromPreAggObj,
            toPreAggObj
          };
        });
      }
    );
  }

  private preAggObjForJoin(preAggObjsToJoin: PreAggregationForQuery[], joinMembers, join): PreAggregationForQuery {
    const fromPreAggObj = preAggObjsToJoin
      .filter(p => joinMembers.every(m => !!p.references.dimensions.find(d => m === d)));
    if (!fromPreAggObj.length) {
      throw new UserError(`No rollups found that can be used for rollup join: ${JSON.stringify(join)}`);
    }
    if (fromPreAggObj.length > 1) {
      throw new UserError(
        `Multiple rollups found that can be used for rollup join ${JSON.stringify(join)}: ${fromPreAggObj.map(p => this.preAggregationId(p)).join(', ')}`,
      );
    }
    return fromPreAggObj[0];
  }

  private resolveJoinMembers(join) {
    return join.joins.map(j => {
      const memberPaths = this.query.collectMemberNamesFor(() => this.query.evaluateSql(j.originalFrom, j.join.sql)).map(m => m.split('.'));
      const invalidMembers = memberPaths.filter(m => m[0] !== j.originalFrom && m[0] !== j.originalTo);
      if (invalidMembers.length) {
        throw new UserError(`Members ${invalidMembers.join(', ')} in join from '${j.originalFrom}' to '${j.originalTo}' doesn't reference join cubes`);
      }
      const fromMembers = memberPaths.filter(m => m[0] === j.originalFrom).map(m => m.join('.'));
      if (!fromMembers.length) {
        throw new UserError(`From members are not found in [${memberPaths.map(m => m.join('.')).join(', ')}] for join ${JSON.stringify(j)}. Please make sure join fields are referencing dimensions instead of columns.`);
      }
      const toMembers = memberPaths.filter(m => m[0] === j.originalTo).map(m => m.join('.'));
      if (!toMembers.length) {
        throw new UserError(`To members are not found in [${memberPaths.map(m => m.join('.')).join(', ')}] for join ${JSON.stringify(j)}. Please make sure join fields are referencing dimensions instead of columns.`);
      }
      return {
        ...j,
        fromMembers,
        toMembers,
      };
    });
  }

  private cubesFromPreAggregation(preAggObj: PreAggregationForQuery): string[] {
    return R.uniq(
      preAggObj.references.measures.map(m => this.query.cubeEvaluator.parsePath('measures', m)).concat(
        preAggObj.references.dimensions.map(m => this.query.cubeEvaluator.parsePathAnyType(m))
      ).map(p => p[0])
    );
  }

  private evaluatedPreAggregationObj(
    cube: string,
    preAggregationName: string,
    preAggregation: PreAggregationDefinitionExtended,
    canUsePreAggregation: CanUsePreAggregationFn
  ): PreAggregationForQuery {
    const references = this.evaluateAllReferences(cube, preAggregation, preAggregationName);
    const preAggObj: PreAggregationForQuery = {
      preAggregationName,
      preAggregation,
      cube,
      canUsePreAggregation: canUsePreAggregation(references),
      references,
      preAggregationId: `${cube}.${preAggregationName}`
    };

    if (preAggregation.type === 'rollupJoin') {
      // TODO evaluation optimizations. Should be cached or moved to compile time.
      const preAggregationsToJoin = preAggObj.references.rollups.map(
        name => {
          const [joinCube, joinPreAggregationName] = this.query.cubeEvaluator.parsePath('preAggregations', name);
          return this.evaluatedPreAggregationObj(
            joinCube,
            joinPreAggregationName,
            this.query.cubeEvaluator.byPath('preAggregations', name) as PreAggregationDefinitionExtended,
            canUsePreAggregation
          );
        }
      );
      return {
        ...preAggObj,
        preAggregationsToJoin,
        rollupJoin: this.buildRollupJoin(preAggObj, preAggregationsToJoin)
      };
    } else if (preAggregation.type === 'rollupLambda') {
      // TODO evaluation optimizations. Should be cached or moved to compile time.
      const referencedPreAggregations = preAggObj.references.rollups.map(
        name => {
          const [referencedCube, referencedPreAggregation] = this.query.cubeEvaluator.parsePath('preAggregations', name);
          return this.evaluatedPreAggregationObj(
            referencedCube,
            referencedPreAggregation,
            this.query.cubeEvaluator.byPath('preAggregations', name) as PreAggregationDefinitionExtended,
            canUsePreAggregation
          );
        }
      );
      if (referencedPreAggregations.length === 0) {
        throw new UserError(`rollupLambda '${cube}.${preAggregationName}' should reference at least one rollup`);
      }
      referencedPreAggregations.forEach((referencedPreAggregation, i) => {
        if (i === referencedPreAggregations.length - 1 && preAggObj.preAggregation.unionWithSourceData && preAggObj.cube !== referencedPreAggregations[i].cube) {
          throw new UserError(`unionWithSourceData can be enabled only for pre-aggregation within '${preAggObj.cube}' cube but '${referencedPreAggregations[i].preAggregationName}' pre-aggregation is defined within '${referencedPreAggregations[i].cube}' cube`);
        }
        referencedPreAggregations[i] = {
          ...referencedPreAggregations[i],
          preAggregation: {
            ...referencedPreAggregations[i].preAggregation,
            unionWithSourceData: i === referencedPreAggregations.length - 1 ? preAggObj.preAggregation.unionWithSourceData : false,
            rollupLambdaId: `${cube}.${preAggregationName}`,
            lastRollupLambda: i === referencedPreAggregations.length - 1,
            rollupLambdaTimeDimensionsReference: preAggObj.references.timeDimensions,
          }
        };
        if (i > 0) {
          const partitionGranularity = PreAggregations.checkPartitionGranularityDefined(cube, preAggregationName, referencedPreAggregations[i]);
          const prevReferencedPreAggregation = referencedPreAggregations[i - 1];
          const partitionGranularityPrev = PreAggregations.checkPartitionGranularityDefined(cube, preAggregationName, prevReferencedPreAggregation);
          const minGranularity = this.query.minGranularity(partitionGranularityPrev, partitionGranularity);
          if (minGranularity !== partitionGranularity) {
            throw new UserError(`'${prevReferencedPreAggregation.cube}.${prevReferencedPreAggregation.preAggregationName}' and '${referencedPreAggregation.cube}.${referencedPreAggregation.preAggregationName}' referenced by '${cube}.${preAggregationName}' rollupLambda have incompatible partition granularities. '${partitionGranularityPrev}' can't be padded by '${partitionGranularity}'`);
          }
        }
        PreAggregations.memberNameMismatchValidation(preAggObj, referencedPreAggregation, 'measures');
        PreAggregations.memberNameMismatchValidation(preAggObj, referencedPreAggregation, 'dimensions');
        PreAggregations.memberNameMismatchValidation(preAggObj, referencedPreAggregation, 'timeDimensions');
      });
      return {
        ...preAggObj,
        referencedPreAggregations,
      };
    } else {
      return preAggObj;
    }
  }

  public static checkPartitionGranularityDefined(cube: string, preAggregationName: string, preAggregation: PreAggregationForQuery): string {
    if (!preAggregation.preAggregation.partitionGranularity) {
      throw new UserError(`'${preAggregation.cube}.${preAggregation.preAggregationName}' referenced by '${cube}.${preAggregationName}' rollupLambda doesn't have partition granularity. Partition granularity is required if multiple rollups are provided.`);
    }
    return preAggregation.preAggregation.partitionGranularity;
  }

  public static memberNameMismatchValidation(preAggA: PreAggregationForQuery, preAggB: PreAggregationForQuery, memberType: 'measures' | 'dimensions' | 'timeDimensions') {
    const preAggAMemberNames = PreAggregations.memberShortNames(preAggA.references[memberType]);
    const preAggBMemberNames = PreAggregations.memberShortNames(preAggB.references[memberType]);
    if (!R.equals(
      preAggAMemberNames,
      preAggBMemberNames
    )) {
      throw new UserError(`Names for ${memberType} doesn't match between '${preAggA.cube}.${preAggA.preAggregationName}' and '${preAggB.cube}.${preAggB.preAggregationName}': ${JSON.stringify(preAggAMemberNames)} does not equal to ${JSON.stringify(preAggBMemberNames)}`);
    }
  }

  private static memberShortNames(memberArray: (string | PreAggregationTimeDimensionReference)[]): string[] {
    return memberArray.map(member => {
      if (typeof member !== 'string') {
        return `${member.dimension.split('.')[1]}.${member.granularity}`;
      } else {
        return member.split('.')[1];
      }
    });
  }

  public rollupMatchResultDescriptions() {
    return this.rollupMatchResults().map(p => ({
      name: this.query.cubeEvaluator.pathFromArray([p.cube, p.preAggregationName]),
      tableName: this.preAggregationTableName(p.cube, p.preAggregationName, p.preAggregation),
      references: p.references,
      canUsePreAggregation: p.canUsePreAggregation
    }));
  }

  public canUseTransformedQuery(): TransformedQuery {
    return PreAggregations.transformQueryToCanUseForm(this.query);
  }

  public static hasCumulativeMeasures(query: BaseQuery): boolean {
    const measures = [...query.measures, ...query.measureFilters];
    const collectLeafMeasures = query.collectLeafMeasures.bind(query);
    return R.pipe(
      R.map(m => query.collectFrom([m], collectLeafMeasures, 'collectLeafMeasures')),
      R.unnest,
      R.uniq,
      R.map(p => query.newMeasure(p)),
      R.any(m => m.isCumulative())
    )(measures);
  }

  public castGranularity(granularity) {
    return granularity;
  }

  public collectOriginalSqlPreAggregations(fn) {
    const preAggregations = [];
    const result = this.query.evaluateSymbolSqlWithContext(fn, { collectOriginalSqlPreAggregations: preAggregations });
    return { preAggregations, result };
  }

  private refreshRangeQuery(cube): BaseQuery {
    return this.query.newSubQueryForCube(
      cube,
      {
        rowLimit: null,
        offset: null,
        preAggregationQuery: true,
      }
    );
  }

  public originalSqlPreAggregationQuery(cube, aggregation): BaseQuery {
    return this.query.newSubQueryForCube(
      cube,
      {
        rowLimit: null,
        offset: null,
        timeDimensions: aggregation.partitionTimeDimensions,
        preAggregationQuery: true,
      }
    );
  }

  public rollupPreAggregationQuery(cube: string, aggregation: PreAggregationDefinitionExtended, context: EvaluateReferencesContext = {}): BaseQuery {
    // `this.evaluateAllReferences` will retain not only members, but their join path as well, and pass join hints
    // to subquery. Otherwise, members in subquery would regenerate new join tree from clean state,
    // and it can be different from expected by join path in pre-aggregation declaration
    const references = this.evaluateAllReferences(cube, aggregation, null, context);
    const cubeQuery = this.query.newSubQueryForCube(cube, {});
    return this.query.newSubQueryForCube(cube, {
      rowLimit: null,
      offset: null,
      measures: references.measures,
      dimensions: references.dimensions,
      timeDimensions: this.mergePartitionTimeDimensions(
        references,
        aggregation.partitionTimeDimensions
      ),
      preAggregationQuery: true,
      useOriginalSqlPreAggregationsInPreAggregation:
        aggregation.useOriginalSqlPreAggregations,
      ungrouped:
        cubeQuery.preAggregationAllowUngroupingWithPrimaryKey(
          cube,
          aggregation
        ) &&
        !!references.dimensions.find((d) => {
          // `d` can contain full join path, so we should trim it
          const trimmedDimension = CubeSymbols.joinHintFromPath(d).path;
          return this.query.cubeEvaluator.dimensionByPath(trimmedDimension).primaryKey;
        }),
    });
  }

  public autoRollupPreAggregationQuery(cube: string, aggregation: PreAggregationDefinitionExtended): BaseQuery {
    return this.query.newSubQueryForCube(
      cube,
      {
        rowLimit: null,
        offset: null,
        measures: aggregation.measures,
        dimensions: aggregation.dimensions,
        timeDimensions:
          this.mergePartitionTimeDimensions(aggregation, aggregation.partitionTimeDimensions),
        preAggregationQuery: true,
        useOriginalSqlPreAggregationsInPreAggregation: aggregation.useOriginalSqlPreAggregations,
      }
    );
  }

  private mergePartitionTimeDimensions(aggregation: PreAggregationReferences, partitionTimeDimensions?: PartitionTimeDimension[]) {
    if (!partitionTimeDimensions) {
      return aggregation.timeDimensions;
    }
    return aggregation.timeDimensions.map(d => {
      const toMerge = partitionTimeDimensions.find(
        // Both qd and d comes from PreaggregationReferences
        qd => qd.dimension === d.dimension
      );
      return toMerge ? { ...d, dateRange: toMerge.dateRange, boundaryDateRange: toMerge.boundaryDateRange } : d;
    });
  }

  private autoRollupNameSuffix(aggregation): string {
    // eslint-disable-next-line prefer-template
    return '_' + aggregation.dimensions.concat(
      aggregation.timeDimensions.map(d => `${d.dimension}${d.granularity.substring(0, 1)}`)
    ).map(s => {
      const path = s.split('.');
      return `${path[0][0]}${path[1]}`;
    }).map(s => s.replace(/_/g, '')).join('_')
      .replace(/[.]/g, '')
      .toLowerCase();
  }

  private evaluateAllReferences(cube: string, aggregation: PreAggregationDefinition, preAggregationName: string | null = null, context: EvaluateReferencesContext = {}): PreAggregationReferences {
    const evaluateReferences = () => {
      const references = this.query.cubeEvaluator.evaluatePreAggregationReferences(cube, aggregation);
      if (!context.inPreAggEvaluation) {
        const preAggQuery = this.query.preAggregationQueryForSqlEvaluation(cube, aggregation, { inPreAggEvaluation: true });
        const aggregateMeasures = preAggQuery?.fullKeyQueryAggregateMeasures({ hasMultipliedForPreAggregation: true });
        references.multipliedMeasures = aggregateMeasures?.multipliedMeasures?.map(m => m.measure);
        if (preAggQuery) {
          // We need to build a join tree for all references, so they would always include full join path
          // even for preaggregation references without join path. It is necessary to be able to match
          // query and preaggregation based on full join tree. But we can not update
          // references.{dimensions,measures,timeDimensions} directly, because it will break
          // evaluation of references in the query on later stages.
          // So we store full named members separately and use them in canUsePreAggregation functions.
          references.joinTree = preAggQuery.join;
          const root = references.joinTree?.root || '';
          references.fullNameMeasures = references.measures.map(m => (m.startsWith(root) ? m : `${root}.${m}`));
          references.fullNameDimensions = references.dimensions.map(d => (d.startsWith(root) ? d : `${root}.${d}`));
          references.fullNameTimeDimensions = references.timeDimensions.map(d => ({
            dimension: (d.dimension.startsWith(root) ? d.dimension : `${root}.${d.dimension}`),
            granularity: d.granularity,
          }));
        }
      }
      if (aggregation.type === 'rollupLambda') {
        if (references.rollups.length > 0) {
          const [firstLambdaCube] = this.query.cubeEvaluator.parsePath('preAggregations', references.rollups[0]);
          const firstLambdaPreAggregation = this.query.cubeEvaluator.byPath('preAggregations', references.rollups[0]) as PreAggregationDefinitionExtended;
          const firstLambdaReferences = this.query.cubeEvaluator.evaluatePreAggregationReferences(firstLambdaCube, firstLambdaPreAggregation);

          if (references.measures.length === 0 &&
            references.dimensions.length === 0 &&
            references.timeDimensions.length === 0) {
            return { ...firstLambdaReferences, rollups: references.rollups };
          } else {
            return references;
          }
        }
      }
      return references;
    };
    if (!preAggregationName) {
      return evaluateReferences();
    }
    return this.query.cacheValue(['evaluateAllReferences', cube, preAggregationName], evaluateReferences);
  }

  public originalSqlPreAggregationTable(preAggregationDescription: PreAggregationForCube): string {
    // eslint-disable-next-line prefer-const
    let { preAggregationName, preAggregation } = preAggregationDescription;

    // @todo Dont use sqlAlias directly, we needed to move it in preAggregationTableName
    if (preAggregation?.sqlAlias) {
      preAggregationName = preAggregation.sqlAlias;
    }

    return this.query.preAggregationTableName(
      preAggregationDescription.cube,
      preAggregationName
    );
  }

  private rollupLambdaUnion(preAggregationForQuery: PreAggregationForQuery, rollupGranularity: string): string {
    if (!preAggregationForQuery.referencedPreAggregations) {
      return this.preAggregationTableName(
        preAggregationForQuery.cube,
        preAggregationForQuery.preAggregationName,
        preAggregationForQuery.preAggregation
      );
    }

    const targetDimensionsReferences = this.dimensionsRenderedReference(preAggregationForQuery);
    const targetTimeDimensionsReferences = this.timeDimensionsRenderedReference(rollupGranularity, preAggregationForQuery);
    const targetMeasuresReferences = this.measureAliasesRenderedReference(preAggregationForQuery);

    const columnsFor = (targetReferences, references, preAggregation) => Object.keys(targetReferences).map(
      member => {
        const [, memberProp] = member.split('.');

        let refKey = references[member];

        if (refKey) {
          return `${refKey} ${targetReferences[member]}`;
        }

        refKey = references[this.query.cubeEvaluator.pathFromArray([preAggregation.cube, memberProp])];

        if (refKey) {
          return `${refKey} ${targetReferences[member]}`;
        }

        throw new Error(`Preaggregation "${preAggregation.preAggregationName}" referenced property "${member}" not found in cube "${preAggregation.cube}"`);
      }
    );

    const tables = preAggregationForQuery.referencedPreAggregations.map(preAggregation => {
      const dimensionsReferences = this.dimensionsRenderedReference(preAggregation);
      const timeDimensionsReferences = this.timeDimensionsRenderedReference(rollupGranularity, preAggregation);
      const measuresReferences = this.measureAliasesRenderedReference(preAggregation);

      return {
        tableName: this.preAggregationTableName(
          preAggregation.cube,
          preAggregation.preAggregationName,
          preAggregation.preAggregation
        ),
        columns: columnsFor(targetDimensionsReferences, dimensionsReferences, preAggregation)
          .concat(columnsFor(targetTimeDimensionsReferences, timeDimensionsReferences, preAggregation))
          .concat(columnsFor(targetMeasuresReferences, measuresReferences, preAggregation))
      };
    });
    if (tables.length === 1) {
      return tables[0].tableName;
    }
    const union = tables.map(table => `SELECT ${table.columns.join(', ')} FROM ${table.tableName}`).join(' UNION ALL ');
    return `(${union})`;
  }

  public rollupPreAggregation(preAggregationForQuery: PreAggregationForQuery, measures: BaseMeasure[], isFullSimpleQuery: boolean, filters): string {
    let toJoin;
    // TODO granularity shouldn't be null?
    const rollupGranularity = preAggregationForQuery.references.timeDimensions[0]?.granularity || 'day';

    const sqlAndAlias = (preAgg) => ({
      preAggregation: preAgg,
      alias: this.query.cubeAlias(this.query.cubeEvaluator.pathFromArray([preAgg.cube, preAgg.preAggregationName])),
      sql: this.rollupLambdaUnion(preAgg, rollupGranularity)
    });

    if (preAggregationForQuery.preAggregation.type === 'rollupJoin') {
      const join = preAggregationForQuery.rollupJoin;

      toJoin = [
        sqlAndAlias(join[0].fromPreAggObj),
        ...join.map(
          j => ({
            ...sqlAndAlias(j.toPreAggObj),
            on: this.query.evaluateSql(j.originalFrom, j.join.sql, {
              sqlResolveFn: (symbol, cube, n) => {
                const path = this.query.cubeEvaluator.pathFromArray([cube, n]);
                const member =
                  this.query.cubeEvaluator.isMeasure(path) ?
                    this.query.newMeasure(path) :
                    this.query.newDimension(path);
                return member.aliasName();
              }
            })
          })
        )
      ];
    } else if (preAggregationForQuery.preAggregation.type === 'rollupLambda') {
      toJoin = [sqlAndAlias(preAggregationForQuery)];
    } else {
      toJoin = [sqlAndAlias(preAggregationForQuery)];
    }

    const from = this.query.joinSql(toJoin);

    const replacedFilters =
      filters || [...this.query.segments, ...this.query.filters, ...(
        this.query.timeDimensions.map(dimension => dimension.dateRange && ({
          filterToWhere: () => this.query.timeRangeFilter(
            this.query.dimensionSql(dimension),
            dimension.localDateTimeFromParam(),
            dimension.localDateTimeToParam(),
          ),
        }))
      )].filter(f => !!f);

    const renderedReference = {
      ...(this.measuresRenderedReference(preAggregationForQuery)),
      ...(this.dimensionsRenderedReference(preAggregationForQuery)),
      ...(this.timeDimensionsRenderedReference(rollupGranularity, preAggregationForQuery)),
    };

    return this.query.evaluateSymbolSqlWithContext(
      // eslint-disable-next-line prefer-template
      () => `SELECT ${this.query.selectAllDimensionsAndMeasures(measures)} FROM ${from} ${this.query.baseWhere(replacedFilters)}` +
        this.query.groupByClause() +
        (
          isFullSimpleQuery ?
            this.query.baseHaving(this.query.measureFilters) +
            this.query.orderBy() +
            this.query.groupByDimensionLimit() : ''
        ),
      {
        renderedReference,
        rollupQuery: true,
        rollupGranularity,
      }
    );
  }

  private measuresRenderedReference(preAggregationForQuery: PreAggregationForQuery): Record<string, string> {
    const measures = this.rollupMeasures(preAggregationForQuery);

    return Object.fromEntries(measures
      .flatMap(path => {
        const measure = this.query.newMeasure(path);
        const measurePath = measure.path();
        const column = this.query.ungrouped ? measure.aliasName() : (this.query.aggregateOnGroupedColumn(
          measure.measureDefinition(),
          measure.aliasName(),
          !this.query.safeEvaluateSymbolContext().overTimeSeriesAggregate,
          path,
        ) || `sum(${measure.aliasName()})`);
        if (measurePath === null) {
          return [[path, column]];
        }
        const memberPath = this.query.cubeEvaluator.pathFromArray(measurePath);
        // Return both full join path and measure path
        return [
          [path, column],
          [memberPath, column],
        ];
      }));
  }

  private measureAliasesRenderedReference(preAggregationForQuery: PreAggregationForQuery): Record<string, string> {
    const measures = this.rollupMeasures(preAggregationForQuery);

    return Object.fromEntries(measures
      .flatMap(path => {
        const measure = this.query.newMeasure(path);
        const measurePath = measure.path();
        const alias = measure.aliasName();
        if (measurePath === null) {
          return [[path, alias]];
        }
        const memberPath = this.query.cubeEvaluator.pathFromArray(measurePath);
        // Return both full join path and measure path
        return [
          [path, alias],
          [memberPath, alias],
        ];
      }));
  }

  private dimensionsRenderedReference(preAggregationForQuery: PreAggregationForQuery): Record<string, string> {
    const dimensions = this.rollupDimensions(preAggregationForQuery);

    return Object.fromEntries(dimensions
      .flatMap(path => {
        const dimension = this.query.newDimension(path);
        const dimensionPath = dimension.path();
        const column = this.query.escapeColumnName(dimension.unescapedAliasName());
        if (dimensionPath === null) {
          return [[path, column]];
        }
        const memberPath = this.query.cubeEvaluator.pathFromArray(dimensionPath);
        // Return both full join path and dimension path
        return [
          [path, column],
          [memberPath, column],
        ];
      }));
  }

  private timeDimensionsRenderedReference(rollupGranularity: string, preAggregationForQuery: PreAggregationForQuery): Record<string, string> {
    const timeDimensions = this.rollupTimeDimensions(preAggregationForQuery);

    return Object.fromEntries(timeDimensions
      .flatMap(td => {
        const timeDimension = this.query.newTimeDimension(td);
        const column = this.query.escapeColumnName(timeDimension.unescapedAliasName(rollupGranularity));
        const memberPath = this.query.cubeEvaluator.pathFromArray(timeDimension.path());
        // Return both full join path and dimension path
        return [
          [td.dimension, column],
          [memberPath, column],
        ];
      }));
  }

  private rollupMembers<T extends 'measures' | 'dimensions' | 'timeDimensions'>(preAggregationForQuery: PreAggregationForQuery, type: T): PreAggregationReferences[T] {
    return preAggregationForQuery.preAggregation.type === 'autoRollup' ?
      // TODO proper types
      (preAggregationForQuery.preAggregation as any)[type] :
      this.evaluateAllReferences(preAggregationForQuery.cube, preAggregationForQuery.preAggregation, preAggregationForQuery.preAggregationName)[type];
  }

  public rollupMeasures(preAggregationForQuery: PreAggregationForQuery): string[] {
    return this.rollupMembers(preAggregationForQuery, 'measures');
  }

  public rollupDimensions(preAggregationForQuery: PreAggregationForQuery): string[] {
    return this.rollupMembers(preAggregationForQuery, 'dimensions');
  }

  public rollupTimeDimensions(preAggregationForQuery: PreAggregationForQuery): PreAggregationTimeDimensionReference[] {
    return this.rollupMembers(preAggregationForQuery, 'timeDimensions');
  }

  public preAggregationId(preAggregation: PreAggregationForQuery): string {
    return `${preAggregation.cube}.${preAggregation.preAggregationName}`;
  }
}
