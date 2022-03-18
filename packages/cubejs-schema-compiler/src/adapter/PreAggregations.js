import R from 'ramda';

import { UserError } from '../compiler/UserError';

export class PreAggregations {
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

  preAggregationDescriptionFor(cube, foundPreAggregation) {
    const { preAggregationName, preAggregation } = foundPreAggregation;

    const tableName = this.preAggregationTableName(cube, preAggregationName, preAggregation);
    const invalidateKeyQueries = this.query.preAggregationInvalidateKeyQueries(cube, preAggregation);

    const matchedTimeDimension =
      preAggregation.partitionGranularity &&
      !this.hasCumulativeMeasures &&
      this.query.timeDimensions.find(
        td => td.dimension === foundPreAggregation.references.timeDimensions[0].dimension && td.dateRange
      );
    const filters = preAggregation.partitionGranularity && this.query.filters.filter(
      td => td.dimension === foundPreAggregation.references.timeDimensions[0].dimension &&
        td.isDateOperator() &&
        td.camelizeOperator === 'inDateRange' // TODO support all date operators
    );
    const queryForSqlEvaluation = this.query.preAggregationQueryForSqlEvaluation(cube, preAggregation);
    const uniqueKeyColumns = preAggregation.type === 'rollup' ?
      this.query.preAggregationQueryForSqlEvaluation(cube, preAggregation).dimensionColumns() : null;
    return {
      preAggregationId: `${cube}.${preAggregationName}`,
      timezone: this.query.options && this.query.options.timezone,
      timestampFormat: queryForSqlEvaluation.timestampFormat(),
      tableName,
      invalidateKeyQueries,
      type: preAggregation.type,
      external: preAggregation.external,
      previewSql: this.query.preAggregationPreviewSql(tableName),
      preAggregationsSchema: this.query.preAggregationSchema(),
      loadSql: this.query.preAggregationLoadSql(cube, preAggregation, tableName),
      sql: this.query.preAggregationSql(cube, preAggregation),
      uniqueKeyColumns,
      dataSource: queryForSqlEvaluation.dataSource,
      partitionGranularity: preAggregation.partitionGranularity,
      preAggregationStartEndQueries:
        (preAggregation.partitionGranularity || preAggregation.granularity) &&
        this.refreshRangeQuery().preAggregationStartEndQueries(cube, preAggregation),
      matchedTimeDimensionDateRange:
        preAggregation.partitionGranularity && (
          matchedTimeDimension && matchedTimeDimension.boundaryDateRangeFormatted() ||
          filters && filters[0] && filters[0].formattedDateRange() // TODO intersect all date ranges
        ),
      indexesSql: Object.keys(preAggregation.indexes || {}).map(
        index => {
          // @todo Dont use sqlAlias directly, we needed to move it in preAggregationTableName
          const indexName = this.preAggregationTableName(cube, `${foundPreAggregation.sqlAlias || preAggregationName}_${index}`, preAggregation, true);
          return {
            indexName,
            sql: this.query.indexSql(
              cube,
              preAggregation,
              preAggregation.indexes[index],
              indexName,
              tableName
            )
          };
        }
      )
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
        references: this.evaluateAllReferences(cube, preAggregation)
      };
    }
    return null;
  }

  static transformQueryToCanUseForm(query) {
    const sortedDimensions = this.squashDimensions(query);
    const measures = query.measures.concat(query.measureFilters);
    const measurePaths = R.uniq(measures.map(m => m.measure));
    const collectLeafMeasures = query.collectLeafMeasures.bind(query);
    const dimensionsList = query.dimensions.map(dim => dim.dimension);
    const segmentsList = query.segments.map(s => s.segment);
    
    const measureToLeafMeasures = {};

    const leafMeasurePaths =
      R.pipe(
        R.map(m => {
          const leafMeasures = query.collectFrom([m], collectLeafMeasures, 'collectLeafMeasures');
          measureToLeafMeasures[m.measure] = leafMeasures.map((measure) => ({
            measure,
            additive: query.newMeasure(measure).isAdditive()
          }));
          
          return leafMeasures;
        }),
        R.unnest,
        R.uniq
      )(measures);

    function sortTimeDimensions(timeDimensions) {
      return timeDimensions && R.sortBy(
        R.prop(0),
        timeDimensions.map(d => [d.dimension, d.rollupGranularity()])
      ) || [];
    }

    function allValuesEq1(map) {
      if (!map) return false;
      // eslint-disable-next-line no-restricted-syntax
      for (const v of map?.values()) {
        if (v !== 1) return false;
      }
      return true;
    }

    const sortedTimeDimensions = sortTimeDimensions(query.timeDimensions);
    const timeDimensions = query.timeDimensions && R.sortBy(
      R.prop(0),
      query.timeDimensions.map(d => [d.dimension, d.granularity])
    ) || [];

    const hasNoTimeDimensionsWithoutGranularity = !query.timeDimensions.filter(d => !d.granularity).length;

    const allFiltersWithinSelectedDimensions =
      R.all(d => dimensionsList.indexOf(d) !== -1)(
        query.filters.map(f => f.dimension)
      );

    const isAdditive = R.all(m => m.isAdditive(), query.measures);
    const leafMeasures = leafMeasurePaths.map(path => query.newMeasure(path));
    const leafMeasureAdditive = R.all(m => m.isAdditive(), leafMeasures);
    const cumulativeMeasures = leafMeasures
      .filter(m => m.isCumulative());
    const hasCumulativeMeasures = cumulativeMeasures.length > 0;
    const windowGranularity = cumulativeMeasures
      .map(m => m.windowGranularity())
      .reduce((a, b) => query.minGranularity(a, b), null);
    const granularityHierarchies = query.granularityHierarchies();
    const hasMultipliedMeasures = query.fullKeyQueryAggregateMeasures().multipliedMeasures.length > 0;

    let filterDimensionsSingleValueEqual = this.collectFilterDimensionsWithSingleValueEqual(
      query.filters,
      dimensionsList.concat(segmentsList).reduce((map, d) => map.set(d, 1), new Map())
    );

    filterDimensionsSingleValueEqual = new Set(
      allValuesEq1(filterDimensionsSingleValueEqual) ? filterDimensionsSingleValueEqual?.keys() : null
    );

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
      filterDimensionsSingleValueEqual
    };
  }

  static collectFilterDimensionsWithSingleValueEqual(filters, map) {
    // eslint-disable-next-line no-restricted-syntax
    for (const f of filters) {
      if (f.operator === 'equals') {
        map.set(f.dimension, Math.min(map.get(f.dimension) || 2, f.values.length));
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
      PreAggregations.transformQueryToCanUseForm(query), refs
    );
  }

  canUsePreAggregationAndCheckIfRefValid(query) {
    const transformedQuery = PreAggregations.transformQueryToCanUseForm(query);
    return (refs) => PreAggregations.canUsePreAggregationForTransformedQueryFn(
      transformedQuery, refs
    );
  }

  checkAutoRollupPreAggregationValid(refs) {
    try {
      this.autoRollupPreAggregationQuery(null, refs); // TODO null
      return true;
    } catch (e) {
      if (e instanceof UserError || e.toString().indexOf('ReferenceError') !== -1) {
        return false;
      } else {
        throw e;
      }
    }
  }

  static canUsePreAggregationForTransformedQueryFn(transformedQuery, refs) {
    function sortTimeDimensions(timeDimensions) {
      return timeDimensions && R.sortBy(
        d => d.join('.'),
        timeDimensions.map(d => [d.dimension, d.granularity || 'day']) // TODO granularity shouldn't be null?
      ) || [];
    }
    const expandGranularity = (granularity) => transformedQuery.granularityHierarchies[granularity] || [granularity];
    // TimeDimension :: [Dimension, Granularity]
    // TimeDimension -> [TimeDimension]
    function expandTimeDimension(timeDimension) {
      const [dimension, granularity] = timeDimension;
      const makeTimeDimension = newGranularity => [dimension, newGranularity];
      return expandGranularity(granularity).map(makeTimeDimension);
    }
    // [[TimeDimension]]
    const queryTimeDimensionsList = transformedQuery.sortedTimeDimensions.map(expandTimeDimension);

    const windowGranularityMatches = (references) => {
      if (!transformedQuery.windowGranularity) {
        return true;
      }

      const sortedTimeDimensions = references.sortedTimeDimensions || sortTimeDimensions(references.timeDimensions);
      return expandGranularity(transformedQuery.windowGranularity).map(windowGranularity => R.all(
        td => td[1] === windowGranularity,
        sortedTimeDimensions,
      )).filter(x => !!x).length > 0;
    };

    const canUsePreAggregationNotAdditive = (references) => {
      const sortedTimeDimensions = references.sortedTimeDimensions || sortTimeDimensions(references.timeDimensions);
      return transformedQuery.hasNoTimeDimensionsWithoutGranularity &&
      (
        references.dimensions.length === transformedQuery.filterDimensionsSingleValueEqual.size &&
        R.all(d => transformedQuery.filterDimensionsSingleValueEqual.has(d), references.dimensions)
      ) &&
      (
        R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.measures) ||
        R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures)
      ) &&
      R.equals(
        transformedQuery.sortedTimeDimensions,
        sortedTimeDimensions
      ) &&
      (transformedQuery.isAdditive || R.equals(
        transformedQuery.timeDimensions,
        sortedTimeDimensions
      )) &&
      !transformedQuery.hasCumulativeMeasures;
    };

    // TODO revisit cumulative leaf measure matches
    const canUsePreAggregationLeafMeasureAdditive = (references) => R.all(
      d => (references.sortedDimensions || references.dimensions).indexOf(d) !== -1,
      transformedQuery.sortedDimensions
    ) &&
      R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures) &&
      R.allPass(
        queryTimeDimensionsList.map(tds => R.anyPass(tds.map(td => R.contains(td))))
      )(references.sortedTimeDimensions || sortTimeDimensions(references.timeDimensions)) &&
      windowGranularityMatches(references);

    let canUseFn;
    if (transformedQuery.leafMeasureAdditive && !transformedQuery.hasMultipliedMeasures) {
      canUseFn = (r) => canUsePreAggregationLeafMeasureAdditive(r) || canUsePreAggregationNotAdditive(r);
    } else {
      canUseFn = canUsePreAggregationNotAdditive;
    }
    if (refs) {
      return canUseFn(refs);
    } else {
      return canUseFn;
    }
  }

  static squashDimensions(query) {
    return R.pipe(R.uniq, R.sortBy(R.identity))(
      query.dimensions.concat(query.filters).map(d => d.dimension).concat(query.segments.map(s => s.segment))
    );
  }

  // eslint-disable-next-line no-unused-vars
  getCubeLattice(cube, preAggregationName, preAggregation) {
    throw new UserError('Auto rollups supported only in Enterprise version');
  }

  findPreAggregationForQuery() {
    if (!this.preAggregationForQuery) {
      this.preAggregationForQuery = this.rollupMatchResults().find(p => p.canUsePreAggregation);
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

  rollupMatchResults() {
    const { query } = this;

    const canUsePreAggregation = this.canUsePreAggregationFn(query);

    return R.pipe(
      R.map(cube => {
        const preAggregations = this.query.cubeEvaluator.preAggregationsForCube(cube);
        let rollupPreAggregations =
          this.findRollupPreAggregationsForCube(cube, canUsePreAggregation, preAggregations);
        rollupPreAggregations = rollupPreAggregations.concat(
          this.findAutoRollupPreAggregationsForCube(cube, preAggregations)
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
      R.filter(([k, a]) => a.type === 'rollup' || a.type === 'rollupJoin'),
      R.map(([preAggregationName, preAggregation]) => {
        const preAggObj = this.evaluatedPreAggregationObj(
          cube, preAggregationName, preAggregation, canUsePreAggregation
        );
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
        } else {
          return preAggObj;
        }
      })
    )(preAggregations);
  }

  // TODO check multiplication factor didn't change
  buildRollupJoin(preAggObj, preAggObjsToJoin) {
    return this.query.cacheValue(
      ['buildRollupJoin', JSON.stringify(preAggObj), JSON.stringify(preAggObjsToJoin)],
      () => {
        const targetJoins = this.resolveJoinMembers(
          this.query.joinGraph.buildJoin(this.cubesFromPreAggregation(preAggObj))
        );
        const existingJoins = R.unnest(preAggObjsToJoin.map(
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
        `Multiple rollups found that can be used for rollup join ${JSON.stringify(join)}: ${fromPreAggObj.map(p => `${p.cube}.${p.preAggregationName}`).join(', ')}`,
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
        preAggObj.references.dimensions.map(m => this.query.cubeEvaluator.parsePath('dimensions', m))
      ).map(p => p[0])
    );
  }

  evaluatedPreAggregationObj(cube, preAggregationName, preAggregation, canUsePreAggregation) {
    const references = this.evaluateAllReferences(cube, preAggregation);
    return {
      preAggregationName,
      preAggregation,
      cube,
      canUsePreAggregation: canUsePreAggregation(references),
      references
    };
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

  refreshRangeQuery() {
    return this.query.newSubQuery({
      rowLimit: null,
      preAggregationQuery: true,
    });
  }

  originalSqlPreAggregationQuery(cube, aggregation) {
    return this.query.newSubQuery({
      rowLimit: null,
      timeDimensions: aggregation.partitionTimeDimensions,
      preAggregationQuery: true,
    });
  }

  rollupPreAggregationQuery(cube, aggregation) {
    const references = this.evaluateAllReferences(cube, aggregation);
    return this.query.newSubQuery({
      rowLimit: null,
      measures: references.measures,
      dimensions: references.dimensions,
      timeDimensions: this.mergePartitionTimeDimensions(references, aggregation.partitionTimeDimensions),
      preAggregationQuery: true,
      useOriginalSqlPreAggregationsInPreAggregation: aggregation.useOriginalSqlPreAggregations,
    });
  }

  autoRollupPreAggregationQuery(cube, aggregation) {
    return this.query.newSubQuery({
      rowLimit: null,
      measures: aggregation.measures,
      dimensions: aggregation.dimensions,
      timeDimensions:
        this.mergePartitionTimeDimensions(aggregation, aggregation.partitionTimeDimensions),
      preAggregationQuery: true,
      useOriginalSqlPreAggregationsInPreAggregation: aggregation.useOriginalSqlPreAggregations,
    });
  }

  mergePartitionTimeDimensions(aggregation, partitionTimeDimensions) {
    if (!partitionTimeDimensions) {
      return aggregation.timeDimensions;
    }
    return aggregation.timeDimensions.map(d => {
      const toMerge = partitionTimeDimensions.find(
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

  evaluateAllReferences(cube, aggregation) {
    return this.query.cubeEvaluator.evaluatePreAggregationReferences(cube, aggregation);
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

  rollupPreAggregation(preAggregationForQuery, measures, isFullSimpleQuery, filters) {
    let toJoin;

    const sqlAndAlias = (preAgg) => ({
      preAggregation: preAgg,
      alias: this.query.cubeAlias(this.query.cubeEvaluator.pathFromArray([preAgg.cube, preAgg.preAggregationName]))
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
    } else {
      toJoin = [sqlAndAlias(preAggregationForQuery)];
    }

    const from = this.query.joinSql(
      toJoin.map(j => ({
        ...j,
        sql:
          this.query.preAggregationTableName(
            j.preAggregation.cube,
            // @todo Dont use sqlAlias directly, we needed to move it in preAggregationTableName
            j.preAggregation.preAggregation.sqlAlias || j.preAggregation.preAggregationName
          )
      }))
    );

    const segmentFilters = this.query.segments.map(
      s => this.query.newFilter({ dimension: s.segment, operator: 'equals', values: [true] })
    );
    const replacedFilters =
      filters || segmentFilters
        .concat(this.query.filters).concat(
          this.query.timeDimensions.map(dimension => dimension.dateRange && ({
            filterToWhere: () => this.query.timeRangeFilter(
              this.query.dimensionSql(dimension),
              dimension.localDateTimeFromParam(),
              dimension.localDateTimeToParam(),
            ),
          }))
        ).filter(f => !!f);

    const renderedReference = R.pipe(
      R.map(path => {
        const measure = this.query.newMeasure(path);
        return [
          path,
          this.query.aggregateOnGroupedColumn(
            measure.measureDefinition(),
            measure.aliasName(),
            !this.query.safeEvaluateSymbolContext().overTimeSeriesAggregate,
            path
          ) || `sum(${measure.aliasName()})`
        ];
      }),
      R.fromPairs
    )(this.rollupMeasures(preAggregationForQuery));

    // TODO granularity shouldn't be null?
    const rollupGranularity = this.castGranularity(preAggregationForQuery.preAggregation.granularity) || 'day';

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

  rollupMeasures(preAggregationForQuery) {
    return preAggregationForQuery.preAggregation.type === 'autoRollup' ?
      preAggregationForQuery.preAggregation.measures :
      this.evaluateAllReferences(preAggregationForQuery.cube, preAggregationForQuery.preAggregation).measures;
  }
}
