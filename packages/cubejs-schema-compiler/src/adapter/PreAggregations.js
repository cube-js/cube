import R from 'ramda';
import { FROM_PARTITION_RANGE, getEnv, TO_PARTITION_RANGE } from '@cubejs-backend/shared';

import { CubeSymbols } from "../compiler/CubeSymbols";
import { UserError } from '../compiler/UserError';

export class PreAggregations {
  /**
   * @param {import('../adapter/BaseQuery').BaseQuery} query
   * @param historyQueries
   * @param cubeLatticeCache
   */
  constructor(query, historyQueries, cubeLatticeCache) {
    this.query = query;
    this.historyQueries = historyQueries;
    this.cubeLatticeCache = cubeLatticeCache;
    this.cubeLattices = {};
  }

  /**
   * @return {unknown[]}
   */
  preAggregationsDescription() {
    const preAggregations = [this.preAggregationsDescriptionLocal()].concat(
      this.query.subQueryDimensions.map(d => this.query.subQueryDescription(d).subQuery)
        .map(q => q.preAggregations.preAggregationsDescription())
    );

    return R.pipe(
      R.unnest,
      R.uniqBy(desc => desc.tableName)
    )(
      preAggregations
    );
  }

  preAggregationsDescriptionLocal() {
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
        R.map(cube => {
          const { preAggregations } = this.collectOriginalSqlPreAggregations(() => this.query.cubeSql(cube));
          return R.unnest(preAggregations.map(p => this.preAggregationDescriptionsFor(p)));
        }),
        R.filter(R.identity),
        R.unnest
      )(this.preAggregationCubes());
    }
    return [];
  }

  preAggregationCubes() {
    const { join } = this.query;
    return join.joins.map(j => j.originalTo).concat([join.root]);
  }

  preAggregationDescriptionsFor(foundPreAggregation) {
    let preAggregations = [foundPreAggregation];
    if (foundPreAggregation.preAggregation.type === 'rollupJoin') {
      preAggregations = foundPreAggregation.preAggregationsToJoin;
    }
    if (foundPreAggregation.preAggregation.type === 'rollupLambda') {
      preAggregations = foundPreAggregation.referencedPreAggregations;
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

  canPartitionsBeUsed(foundPreAggregation) {
    return foundPreAggregation.preAggregation.partitionGranularity &&
      foundPreAggregation.references.timeDimensions &&
      foundPreAggregation.references.timeDimensions.length;
  }

  addPartitionRangeTo(foundPreAggregation, dimension, range, boundaryDateRange) {
    return Object.assign({}, foundPreAggregation, {
      preAggregation: Object.assign({}, foundPreAggregation.preAggregation, {
        partitionTimeDimensions: [{
          dimension,
          dateRange: range,
          boundaryDateRange
        }],
      })
    });
  }

  partitionDimension(foundPreAggregation) {
    const { dimension } = foundPreAggregation.references.timeDimensions[0];
    const partitionDimension = this.query.newTimeDimension({
      dimension,
      granularity: this.castGranularity(foundPreAggregation.preAggregation.partitionGranularity),
      dateRange: this.query.timeDimensions[0] && this.query.timeDimensions[0].dateRange,
      boundaryDateRange: this.query.timeDimensions[0] && this.query.timeDimensions[0].boundaryDateRange
    });
    return { dimension, partitionDimension };
  }

  preAggregationDescriptionsForRecursive(cube, foundPreAggregation) {
    const query = this.query.preAggregationQueryForSqlEvaluation(cube, foundPreAggregation.preAggregation);
    const descriptions = query !== this.query ? query.preAggregations.preAggregationsDescription() : [];
    return descriptions.concat(this.preAggregationDescriptionFor(cube, foundPreAggregation));
  }

  get hasCumulativeMeasures() {
    if (!this.hasCumulativeMeasuresValue) {
      this.hasCumulativeMeasuresValue = PreAggregations.hasCumulativeMeasures(this.query);
    }
    return this.hasCumulativeMeasuresValue;
  }

  // Return array of `aggregations` columns descriptions in form `<func>(<column>)`
  // Aggregations used in CubeStore create table for describe measures in CubeStore side
  aggregationsColumns(cube, preAggregation) {
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

  preAggregationDescriptionFor(cube, foundPreAggregation) {
    const { preAggregationName, preAggregation, references } = foundPreAggregation;

    const tableName = this.preAggregationTableName(cube, preAggregationName, preAggregation);
    const invalidateKeyQueries = this.query.preAggregationInvalidateKeyQueries(cube, preAggregation, preAggregationName);
    const queryForSqlEvaluation = this.query.preAggregationQueryForSqlEvaluation(cube, preAggregation);
    const partitionInvalidateKeyQueries = queryForSqlEvaluation.partitionInvalidateKeyQueries?.(cube, preAggregation);

    const allBackAliasMembers = this.query.allBackAliasMembers();

    const matchedTimeDimension = preAggregation.partitionGranularity && !this.hasCumulativeMeasures &&
      this.query.timeDimensions.find(td => {
        if (!td.dateRange) {
          return false;
        }

        const timeDimensionsReference =
          foundPreAggregation.preAggregation.rollupLambdaTimeDimensionsReference ||
          foundPreAggregation.references.timeDimensions;
        const timeDimensionReference = timeDimensionsReference[0];

        // timeDimensionsReference[*].dimension can contain full join path, so we should trim it
        // TODO check full join path match here
        const timeDimensionReferenceDimension = CubeSymbols.joinHintFromPath(timeDimensionReference.dimension).path;

        if (td.dimension === timeDimensionReferenceDimension) {
          return true;
        }

        // Handling for views
        return td.dimension === allBackAliasMembers[timeDimensionReferenceDimension];
      });

    const filters = preAggregation.partitionGranularity && this.query.filters.filter(td => {
      // TODO support all date operators
      if (td.isDateOperator() && td.camelizeOperator === 'inDateRange') {
        if (td.dimension === foundPreAggregation.references.timeDimensions[0].dimension) {
          return true;
        }

        // Handling for views
        return td.dimension === allBackAliasMembers[foundPreAggregation.references.timeDimensions[0].dimension];
      }

      return false;
    });

    const uniqueKeyColumnsDefault = () => null;
    const uniqueKeyColumns = ({
      rollup: () => queryForSqlEvaluation.preAggregationUniqueKeyColumns(cube, preAggregation),
      originalSql: () => preAggregation.uniqueKeyColumns || null
    }[preAggregation.type] || uniqueKeyColumnsDefault)();

    const aggregationsColumns = this.aggregationsColumns(cube, preAggregation);

    return {
      preAggregationId: `${cube}.${preAggregationName}`,
      timezone: this.query.options && this.query.options.timezone,
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
      // in fact we can reference preAggregation.granularity however accessing timeDimensions is more strict and consistent
      granularity: references.timeDimensions[0]?.granularity,
      partitionGranularity: preAggregation.partitionGranularity,
      updateWindowSeconds: preAggregation.refreshKey && preAggregation.refreshKey.updateWindow &&
        queryForSqlEvaluation.parseSecondDuration(preAggregation.refreshKey.updateWindow),
      preAggregationStartEndQueries:
        (preAggregation.partitionGranularity || references.timeDimensions[0]?.granularity) &&
        this.refreshRangeQuery(cube).preAggregationStartEndQueries(cube, preAggregation),
      matchedTimeDimensionDateRange:
        preAggregation.partitionGranularity && (
          matchedTimeDimension && matchedTimeDimension.boundaryDateRangeFormatted() ||
          filters && filters[0] && filters[0].formattedDateRange() // TODO intersect all date ranges
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
                preAggregation.indexes[index],
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
              type: preAggregation.indexes[index].type,
              columns: queryForSqlEvaluation.evaluateIndexColumns(cube, preAggregation.indexes[index])
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

  preAggregationTableName(cube, preAggregationName, preAggregation, skipSchema) {
    const name = preAggregation.sqlAlias || preAggregationName;
    return this.query.preAggregationTableName(
      cube,
      name,
      skipSchema
    );
  }

  findPreAggregationToUseForCube(cube) {
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

  static transformQueryToCanUseForm(query) {
    const flattenDimensionMembers = this.flattenDimensionMembers(query);
    const sortedDimensions = this.squashDimensions(query, flattenDimensionMembers);
    const allBackAliasMembers = query.allBackAliasMembers();
    const measures = query.measures.concat(query.measureFilters);
    const measurePaths = R.uniq(this.flattenMembers(measures).map(m => m.expressionPath()));
    const collectLeafMeasures = query.collectLeafMeasures.bind(query);
    const dimensionsList = query.dimensions.map(dim => dim.expressionPath());
    const segmentsList = query.segments.map(s => s.expressionPath());
    const ownedDimensions = PreAggregations.ownedMembers(query, flattenDimensionMembers);
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
        R.map(m => {
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
        R.uniq
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
    const ownedTimeDimensionsWithRollupGranularity = PreAggregations.sortTimeDimensionsWithRollupGranularity(ownedTimeDimensions);
    const ownedTimeDimensionsAsIs = PreAggregations.timeDimensionsAsIs(ownedTimeDimensions);

    const hasNoTimeDimensionsWithoutGranularity = !query.timeDimensions.filter(d => !d.granularity).length;

    const allFiltersWithinSelectedDimensions =
      R.all(d => dimensionsList.indexOf(d) !== -1)(
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

  /**
   *
   * @param query
   * @param members
   * @param {Map<string, Array<string>>} cubeToJoinPrefix
   * @returns {Array<string>}
   */
  static ownedMembers(query, members) {
    return R.pipe(R.uniq, R.sortBy(R.identity))(
      query
        .collectFrom(members, query.collectMemberNamesFor.bind(query), 'collectMemberNamesFor')
        .filter(d => query.cubeEvaluator.byPathAnyType(d).ownedByCube)
    );
  }

  static sortTimeDimensionsWithRollupGranularity(timeDimensions) {
    return timeDimensions && R.sortBy(
      R.prop(0),
      timeDimensions.map(d => [d.expressionPath(), d.rollupGranularity()])
    ) || [];
  }

  static timeDimensionsAsIs(timeDimensions) {
    return timeDimensions && R.sortBy(
      R.prop(0),
      timeDimensions.map(d => [d.expressionPath(), d.resolvedGranularity()]),
    ) || [];
  }

  static collectFilterDimensionsWithSingleValueEqual(filters, map) {
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

  static transformedQueryToReferences(query) {
    return {
      measures: query.measures,
      dimensions: query.sortedDimensions,
      timeDimensions: query.sortedTimeDimensions.map(([dimension, granularity]) => ({ dimension, granularity }))
    };
  }

  canUsePreAggregationFn(query, refs) {
    return PreAggregations.canUsePreAggregationForTransformedQueryFn(
      PreAggregations.transformQueryToCanUseForm(query),
      refs,
    );
  }

  /**
   * Returns function to determine whether pre-aggregation can be used or not
   * for specified query, or its value for `refs` if specified.
   * @param {Object} transformedQuery transformed query
   * @param {PreAggregationReferences?} refs pre-aggs reference
   * @returns {function(preagg: Object): boolean}
   */
  static canUsePreAggregationForTransformedQueryFn(transformedQuery, refs) {
    // TODO this needs to check not only members list, but their join paths as well:
    //  query can have same members as pre-agg, but different calculated join path
    //  `refs` will come from preagg references, and would contain full join paths

    // TODO remove this in favor of matching with join path
    /**
     * @param {PreAggregationReferences} references
     * @returns {PreAggregationReferences}
     */
    function trimmedReferences(references) {
      const timeDimensionsTrimmed = references
        .timeDimensions
        .map(td => ({
          ...td,
          dimension: CubeSymbols.joinHintFromPath(td.dimension).path,
        }));
      const measuresTrimmed = references
        .measures
        .map(m => CubeSymbols.joinHintFromPath(m).path);
      const dimensionsTrimmed = references
        .dimensions
        .map(d => CubeSymbols.joinHintFromPath(d).path);

      return {
        ...references,
        dimensions: dimensionsTrimmed,
        measures: measuresTrimmed,
        timeDimensions: timeDimensionsTrimmed,
      };
    }

    /**
     * Returns an array of 2-elements arrays with the dimension and granularity
     * sorted by the concatenated dimension + granularity key.
     * @param {Array<{dimension: string, granularity: string}>} timeDimensions
     * @returns {Array<Array<string>>}
     */
    const sortTimeDimensions = (timeDimensions) => (
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

    /**
     * @type {Set<string>}
     */
    const filterDimensionsSingleValueEqual =
      transformedQuery.filterDimensionsSingleValueEqual && (transformedQuery.filterDimensionsSingleValueEqual instanceof Set
        ? transformedQuery.filterDimensionsSingleValueEqual
        : new Set(
          Object.keys(
            transformedQuery.filterDimensionsSingleValueEqual || {},
          )
        ));

    const backAlias = (references) => references.map(r => (
      Array.isArray(r) ?
        [transformedQuery.allBackAliasMembers[r[0]] || r[0], r[1]] :
        transformedQuery.allBackAliasMembers[r] || r
    ));

    /**
     * Determine whether pre-aggregation can be used or not.
     * @param {PreAggregationReferences} references
     * @returns {boolean}
     */
    const canUsePreAggregationNotAdditive = (references) => {
      // TODO remove this in favor of matching with join path
      const referencesTrimmed = trimmedReferences(references);

      const refTimeDimensions = backAlias(sortTimeDimensions(referencesTrimmed.timeDimensions));
      const qryTimeDimensions = references.allowNonStrictDateRangeMatch
        ? transformedQuery.timeDimensions
        : transformedQuery.sortedTimeDimensions;
      const backAliasMeasures = backAlias(referencesTrimmed.measures);
      const backAliasDimensions = backAlias(referencesTrimmed.dimensions);
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
        referencesTrimmed.dimensions.length === filterDimensionsSingleValueEqual.size &&
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
     * @param {string} dimension Dimension in the form of `cube.timeDimension`
     * @param {string} granularity Granularity
     * @returns {Array<string>}
     */
    const expandGranularity = (dimension, granularity) => (
      transformedQuery.granularityHierarchies[`${dimension}.${granularity}`] ||
      [granularity]
    );

    /**
     * Determine whether time dimensions match to the window granularity or not.
     * @param {PreAggregationReferences} references
     * @returns {boolean}
     */
    const windowGranularityMatches = (references) => {
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
     * @param {*} timeDimension
     * @returns {Array<Array<string>>}
     */
    const expandTimeDimension = (timeDimension) => {
      const [dimension, resolvedGranularity] = timeDimension;
      if (!resolvedGranularity) {
        return [[dimension, '*']]; // Any granularity should fit
      }
      return expandGranularity(dimension, resolvedGranularity)
        .map((newGranularity) => [dimension, newGranularity]);
    };

    /**
     * Determine whether pre-aggregation can be used or not.
     * TODO: revisit cumulative leaf measure matches.
     * @param {PreAggregationReferences} references
     * @returns {boolean}
     */
    const canUsePreAggregationLeafMeasureAdditive = (references) => {
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

      // TODO remove this in favor of matching with join path
      const referencesTrimmed = trimmedReferences(references);

      const dimensionsMatch = (dimensions, doBackAlias) => R.all(
        d => (
          doBackAlias ?
            backAlias(referencesTrimmed.dimensions) :
            (referencesTrimmed.dimensions)
        ).indexOf(d) !== -1,
        dimensions
      );

      const timeDimensionsMatch = (timeDimensionsList, doBackAlias) => R.allPass(
        timeDimensionsList.map(
          tds => R.anyPass(tds.map(td => {
            if (td[1] === '*') {
              return R.any(tdtc => tdtc[0] === td[0]); // need to match the dimension at least
            } else {
              return R.contains(td);
            }
          }))
        )
      )(
        doBackAlias ?
          backAlias(sortTimeDimensions(referencesTrimmed.timeDimensions)) :
          (sortTimeDimensions(referencesTrimmed.timeDimensions))
      );

      if (transformedQuery.ungrouped) {
        const allReferenceCubes = R.pipe(R.map(m => (m.dimension || m).split('.')[0]), R.uniq, R.sortBy(R.identity))(
          referencesTrimmed.measures.concat(referencesTrimmed.dimensions).concat(referencesTrimmed.timeDimensions)
        );
        if (
          !R.equals(transformedQuery.sortedAllCubeNames, allReferenceCubes) ||
          !(
            dimensionsMatch(transformedQuery.sortedUsedCubePrimaryKeys, true) || dimensionsMatch(transformedQuery.sortedUsedCubePrimaryKeys, false)
          )
        ) {
          return false;
        }
      }

      const backAliasMeasures = backAlias(referencesTrimmed.measures);
      return ((
        windowGranularityMatches(references)
      ) && (
        R.all(
          m => referencesTrimmed.measures.indexOf(m) !== -1,
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

    /**
     * Determine whether pre-aggregation can be used or not.
     * @returns {boolean}
     */
    const canUseFn =
      (
        transformedQuery.leafMeasureAdditive && !transformedQuery.hasMultipliedMeasures && !transformedQuery.hasMultiStage || transformedQuery.ungrouped
      ) ? (r) => canUsePreAggregationLeafMeasureAdditive(r) ||
          canUsePreAggregationNotAdditive(r)
        : canUsePreAggregationNotAdditive;

    if (refs) {
      return canUseFn(refs);
    } else {
      return canUseFn;
    }
  }

  static squashDimensions(query, flattenDimensionMembers) {
    return R.pipe(R.uniq, R.sortBy(R.identity))(
      flattenDimensionMembers.map(d => d.expressionPath())
    );
  }

  static flattenMembers(members) {
    return R.flatten(
      members.map(m => m.getMembers()),
    );
  }

  static flattenDimensionMembers(query) {
    return this.flattenMembers(
      query.dimensions
        .concat(query.filters)
        .concat(query.segments)
    );
  }

  // eslint-disable-next-line no-unused-vars
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  getCubeLattice(cube, preAggregationName, preAggregation) {
    throw new UserError('Auto rollups supported only in Enterprise version');
  }

  /**
   * Returns pre-agg which determined as applicable for the query (the first one
   * from the list of potentially applicable pre-aggs). The order of the
   * potentially applicable pre-aggs is the same as the order in which these
   * pre-aggs appear in the schema file.
   * @returns {Object}
   */
  findPreAggregationForQuery() {
    if (!this.preAggregationForQuery) {
      this.preAggregationForQuery =
        this
          .rollupMatchResults()
          // Refresh worker can access specific pre-aggregations even in case those hidden by others
          .find(p => p.canUsePreAggregation && (!this.query.options.preAggregationId || p.preAggregationId === this.query.options.preAggregationId));
    }
    return this.preAggregationForQuery;
  }

  findAutoRollupPreAggregationsForCube(cube, preAggregations) {
    if (
      R.any(m => m.path() && m.path()[0] === cube, this.query.measures) ||
      !this.query.measures.length && !this.query.timeDimensions.length &&
      R.all(d => d.path() && d.path()[0] === cube, this.query.dimensions)
    ) {
      return R.pipe(
        R.toPairs,
        // eslint-disable-next-line no-unused-vars
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        R.filter(([k, a]) => a.type === 'autoRollup'),
        R.map(([preAggregationName, preAggregation]) => {
          const cubeLattice = this.getCubeLattice(cube, preAggregationName, preAggregation);
          const optimalPreAggregation = cubeLattice.findOptimalPreAggregationFromLattice(this.query);
          return optimalPreAggregation && {
            preAggregationName: preAggregationName + this.autoRollupNameSuffix(cube, optimalPreAggregation),
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
   * Returns an array of potentially applicable for the query preaggs in the
   * same order they appear in the schema file.
   * @returns {Array<Object>}
   */
  rollupMatchResults() {
    const { query } = this;

    const canUsePreAggregation = this.canUsePreAggregationFn(query);

    return R.pipe(
      R.map(cube => {
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

  findRollupPreAggregationsForCube(cube, canUsePreAggregation, preAggregations) {
    return R.pipe(
      R.toPairs,
      // eslint-disable-next-line no-unused-vars
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      R.filter(([k, a]) => a.type === 'rollup' || a.type === 'rollupJoin' || a.type === 'rollupLambda'),
      R.map(([preAggregationName, preAggregation]) => this.evaluatedPreAggregationObj(cube, preAggregationName, preAggregation, canUsePreAggregation))
    )(preAggregations);
  }

  // TODO check multiplication factor didn't change
  buildRollupJoin(preAggObj, preAggObjsToJoin) {
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

  preAggObjForJoin(preAggObjsToJoin, joinMembers, join) {
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

  resolveJoinMembers(join) {
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

  cubesFromPreAggregation(preAggObj) {
    return R.uniq(
      preAggObj.references.measures.map(m => this.query.cubeEvaluator.parsePath('measures', m)).concat(
        preAggObj.references.dimensions.map(m => this.query.cubeEvaluator.parsePathAnyType(m))
      ).map(p => p[0])
    );
  }

  evaluatedPreAggregationObj(cube, preAggregationName, preAggregation, canUsePreAggregation) {
    const references = this.evaluateAllReferences(cube, preAggregation, preAggregationName);
    const preAggObj = {
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
            this.query.cubeEvaluator.byPath('preAggregations', name),
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
            this.query.cubeEvaluator.byPath('preAggregations', name),
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

  static checkPartitionGranularityDefined(cube, preAggregationName, preAggregation) {
    if (!preAggregation.preAggregation.partitionGranularity) {
      throw new UserError(`'${preAggregation.cube}.${preAggregation.preAggregationName}' referenced by '${cube}.${preAggregationName}' rollupLambda doesn't have partition granularity. Partition granularity is required if multiple rollups are provided.`);
    }
    return preAggregation.preAggregation.partitionGranularity;
  }

  static memberNameMismatchValidation(preAggA, preAggB, memberType) {
    const preAggAMemberNames = PreAggregations.memberShortNames(preAggA.references[memberType], memberType === 'timeDimensions');
    const preAggBMemberNames = PreAggregations.memberShortNames(preAggB.references[memberType], memberType === 'timeDimensions');
    if (!R.equals(
      preAggAMemberNames,
      preAggBMemberNames
    )) {
      throw new UserError(`Names for ${memberType} doesn't match between '${preAggA.cube}.${preAggA.preAggregationName}' and '${preAggB.cube}.${preAggB.preAggregationName}': ${JSON.stringify(preAggAMemberNames)} does not equal to ${JSON.stringify(preAggBMemberNames)}`);
    }
  }

  static memberShortNames(memberArray, isTimeDimension) {
    return memberArray.map(member => (isTimeDimension ? `${member.dimension.split('.')[1]}.${member.granularity}` : member.split('.')[1]));
  }

  rollupMatchResultDescriptions() {
    return this.rollupMatchResults().map(p => ({
      name: this.query.cubeEvaluator.pathFromArray([p.cube, p.preAggregationName]),
      tableName: this.preAggregationTableName(p.cube, p.preAggregationName, p.preAggregation),
      references: p.references,
      canUsePreAggregation: p.canUsePreAggregation
    }));
  }

  canUseTransformedQuery() {
    return PreAggregations.transformQueryToCanUseForm(this.query);
  }

  static hasCumulativeMeasures(query) {
    const measures = (query.measures.concat(query.measureFilters));
    const collectLeafMeasures = query.collectLeafMeasures.bind(query);
    return R.pipe(
      R.map(m => query.collectFrom([m], collectLeafMeasures, 'collectLeafMeasures')),
      R.unnest,
      R.uniq,
      R.map(p => query.newMeasure(p)),
      R.any(m => m.isCumulative())
    )(measures);
  }

  castGranularity(granularity) {
    return granularity;
  }

  collectOriginalSqlPreAggregations(fn) {
    const preAggregations = [];
    const result = this.query.evaluateSymbolSqlWithContext(fn, { collectOriginalSqlPreAggregations: preAggregations });
    return { preAggregations, result };
  }

  refreshRangeQuery(cube) {
    return this.query.newSubQueryForCube(
      cube,
      {
        rowLimit: null,
        offset: null,
        preAggregationQuery: true,
      }
    );
  }

  originalSqlPreAggregationQuery(cube, aggregation) {
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

  rollupPreAggregationQuery(cube, aggregation) {
    // `this.evaluateAllReferences` will retain not only members, but their join path as well, and pass join hints
    // to subquery. Otherwise, members in subquery would regenerate new join tree from clean state,
    // and it can be different from expected by join path in pre-aggregation declaration
    const references = this.evaluateAllReferences(cube, aggregation);
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
          // TODO check full join path match here
          const trimmedDimension = CubeSymbols.joinHintFromPath(d).path;
          return this.query.cubeEvaluator.dimensionByPath(trimmedDimension).primaryKey;
        }),
    });
  }

  autoRollupPreAggregationQuery(cube, aggregation) {
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

  mergePartitionTimeDimensions(aggregation, partitionTimeDimensions) {
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

  autoRollupNameSuffix(cube, aggregation) {
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

  /**
   *
   * @param {string} cube
   * @param aggregation
   * @param {string} [preAggregationName]
   * @returns {PreAggregationReferences}
   */
  evaluateAllReferences(cube, aggregation, preAggregationName) {
    // TODO build a join tree for all references, so they would always include full join path
    //  Even for preaggregation references without join path
    //  It is necessary to be able to match query and preaggregation based on full join tree

    const evaluateReferences = () => {
      const references = this.query.cubeEvaluator.evaluatePreAggregationReferences(cube, aggregation);
      if (aggregation.type === 'rollupLambda') {
        if (references.rollups.length > 0) {
          const [firstLambdaCube] = this.query.cubeEvaluator.parsePath('preAggregations', references.rollups[0]);
          const firstLambdaPreAggregation = this.query.cubeEvaluator.byPath('preAggregations', references.rollups[0]);
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

  originalSqlPreAggregationTable(preAggregationDescription) {
    // eslint-disable-next-line prefer-const
    let { preAggregationName, preAggregation } = preAggregationDescription;

    // @todo Dont use sqlAlias directly, we needed to move it in preAggregationTableName
    if (preAggregation && preAggregation.sqlAlias) {
      preAggregationName = preAggregation.sqlAlias;
    }

    return this.query.preAggregationTableName(
      preAggregationDescription.cube,
      preAggregationName
    );
  }

  rollupLambdaUnion(preAggregationForQuery, rollupGranularity) {
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

  rollupPreAggregation(preAggregationForQuery, measures, isFullSimpleQuery, filters) {
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
      filters || this.query.segments
        .concat(this.query.filters).concat(
          this.query.timeDimensions.map(dimension => dimension.dateRange && ({
            filterToWhere: () => this.query.timeRangeFilter(
              this.query.dimensionSql(dimension),
              dimension.localDateTimeFromParam(),
              dimension.localDateTimeToParam(),
            ),
          }))
        ).filter(f => !!f);

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

  measuresRenderedReference(preAggregationForQuery) {
    return R.pipe(
      R.map(path => {
        const measure = this.query.newMeasure(path);
        return [
          path,
          this.query.ungrouped ? measure.aliasName() : (this.query.aggregateOnGroupedColumn(
            measure.measureDefinition(),
            measure.aliasName(),
            !this.query.safeEvaluateSymbolContext().overTimeSeriesAggregate,
            path,
          ) || `sum(${measure.aliasName()})`),
        ];
      }),
      R.fromPairs,
    )(this.rollupMeasures(preAggregationForQuery));
  }

  measureAliasesRenderedReference(preAggregationForQuery) {
    return R.pipe(
      R.map(path => {
        const measure = this.query.newMeasure(path);
        return [
          path,
          measure.aliasName(),
        ];
      }),
      R.fromPairs,
    )(this.rollupMeasures(preAggregationForQuery));
  }

  dimensionsRenderedReference(preAggregationForQuery) {
    return R.pipe(
      R.map(path => {
        const dimension = this.query.newDimension(path);
        return [
          path,
          this.query.escapeColumnName(dimension.unescapedAliasName()),
        ];
      }),
      R.fromPairs,
    )(this.rollupDimensions(preAggregationForQuery));
  }

  timeDimensionsRenderedReference(rollupGranularity, preAggregationForQuery) {
    return R.pipe(
      R.map((td) => {
        const timeDimension = this.query.newTimeDimension(td);
        return [
          td.dimension,
          this.query.escapeColumnName(timeDimension.unescapedAliasName(rollupGranularity)),
        ];
      }),
      R.fromPairs,
    )(this.rollupTimeDimensions(preAggregationForQuery));
  }

  rollupMembers(preAggregationForQuery, type) {
    return preAggregationForQuery.preAggregation.type === 'autoRollup' ?
      preAggregationForQuery.preAggregation[type] :
      this.evaluateAllReferences(preAggregationForQuery.cube, preAggregationForQuery.preAggregation, preAggregationForQuery.preAggregationName)[type];
  }

  rollupMeasures(preAggregationForQuery) {
    return this.rollupMembers(preAggregationForQuery, 'measures');
  }

  rollupDimensions(preAggregationForQuery) {
    return this.rollupMembers(preAggregationForQuery, 'dimensions');
  }

  rollupTimeDimensions(preAggregationForQuery) {
    return this.rollupMembers(preAggregationForQuery, 'timeDimensions');
  }

  preAggregationId(preAggregation) {
    return `${preAggregation.cube}.${preAggregation.preAggregationName}`;
  }
}
