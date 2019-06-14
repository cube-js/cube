const R = require('ramda');
const UserError = require('../compiler/UserError');

class PreAggregations {
  constructor(query, historyQueries, cubeLatticeCache) {
    this.query = query;
    this.historyQueries = historyQueries;
    this.cubeLatticeCache = cubeLatticeCache;
    this.cubeLattices = {};
  }

  preAggregationsDescription() {
    return R.pipe(R.unnest, R.uniqBy(desc => desc.tableName))(
      [this.preAggregationsDescriptionLocal()].concat(
        this.query.subQueryDimensions.map(d => this.query.subQueryDescription(d).subQuery)
          .map(q => q.preAggregations.preAggregationsDescription())
      )
    );
  }

  preAggregationsDescriptionLocal() {
    const preAggregationForQuery = this.findPreAggregationForQuery();
    if (preAggregationForQuery) {
      if (preAggregationForQuery.preAggregation.useOriginalSqlPreAggregations) {
        const { preAggregations, result } =
          this.collectOriginalSqlPreAggregations(() =>
            this.preAggregationDescriptionsFor(preAggregationForQuery.cube, preAggregationForQuery)
          );
        return R.unnest(preAggregations.map(p => this.preAggregationDescriptionsFor(p.cube, p))).concat(result);
      }
      return this.preAggregationDescriptionsFor(preAggregationForQuery.cube, preAggregationForQuery);
    }
    return R.pipe(
      R.map(cube => {
        const foundPreAggregation = this.findPreAggregationToUseForCube(cube);
        if (foundPreAggregation) {
          return this.preAggregationDescriptionsFor(cube, foundPreAggregation);
        }
        return null;
      }),
      R.filter(R.identity),
      R.unnest
    )(this.preAggregationCubes());
  }

  preAggregationCubes() {
    const join = this.query.join;
    return join.joins.map(j => j.originalTo).concat([join.root]);
  }

  preAggregationDescriptionsFor(cube, foundPreAggregation) {
    if (foundPreAggregation.preAggregation.partitionGranularity && this.query.timeDimensions.length) {
      const { dimension, partitionDimension } = this.partitionDimension(foundPreAggregation);
      return partitionDimension.timeSeries().map(range =>
        this.preAggregationDescriptionFor(cube, this.addPartitionRangeTo(foundPreAggregation, dimension, range))
      );
    }
    return [this.preAggregationDescriptionFor(cube, foundPreAggregation)];
  }

  addPartitionRangeTo(foundPreAggregation, dimension, range) {
    return Object.assign({}, foundPreAggregation, {
      preAggregation: Object.assign({}, foundPreAggregation.preAggregation, {
        partitionTimeDimensions: [{
          dimension,
          dateRange: range
        }],
      })
    });
  }

  partitionDimension(foundPreAggregation) {
    const dimension = this.query.timeDimensions[0].dimension;
    const partitionDimension = this.query.newTimeDimension({
      dimension,
      granularity: this.castGranularity(foundPreAggregation.preAggregation.partitionGranularity),
      dateRange: this.query.timeDimensions[0].dateRange
    });
    return { dimension, partitionDimension };
  }

  preAggregationDescriptionFor(cube, foundPreAggregation) {
    const { preAggregationName, preAggregation } = foundPreAggregation;
    const tableName = this.preAggregationTableName(cube, preAggregationName, preAggregation);
    return {
      preAggregationsSchema: this.query.preAggregationSchema(),
      tableName,
      loadSql: this.query.preAggregationLoadSql(cube, preAggregation, tableName),
      invalidateKeyQueries: this.query.preAggregationInvalidateKeyQueries(cube, preAggregation),
      external: preAggregation.external
    };
  }

  preAggregationTableName(cube, preAggregationName, preAggregation) {
    return this.query.preAggregationTableName(
      cube, preAggregationName + (
      preAggregation.partitionTimeDimensions ?
        preAggregation.partitionTimeDimensions[0].dateRange[0].replace('T00:00:00.000', '').replace(/-/g, '') :
        ''
    ));
  }

  findPreAggregationToUseForCube(cube) {
    const preAggregates = this.query.cubeEvaluator.preAggregationsForCube(cube);
    const originalSqlPreAggregations = R.pipe(
      R.toPairs,
      R.filter(([k, a]) => a.type === 'originalSql')
    )(preAggregates);
    if (originalSqlPreAggregations.length) {
      const [preAggregationName, preAggregation] = originalSqlPreAggregations[0];
      return {
        preAggregationName,
        preAggregation,
        cube
      };
    }
    return null;
  }

  static transformQueryToCanUseForm(query) {
    const sortedDimensions = this.squashDimensions(query);
    const measures = (query.measures.concat(query.measureFilters));
    const measurePaths = R.uniq(measures.map(m => m.measure));
    const leafMeasurePaths =
      R.pipe(
        R.map(m => query.collectLeafMeasures(() => query.traverseSymbol(m))),
        R.unnest,
        R.uniq
      )(measures);

    function sortTimeDimensions(timeDimensions) {
      return timeDimensions && R.sortBy(
        R.prop(0),
        timeDimensions.map(d => [d.dimension, d.granularity || 'date'])
      ) || [];
    }

    const sortedTimeDimensions = sortTimeDimensions(query.timeDimensions);
    const hasNoTimeDimensionsWithoutGranularity = !query.timeDimensions.filter(d => !d.granularity).length;

    const allFiltersWithinSelectedDimensions =
      R.all(d => query.dimensions.map(dim => dim.dimension).indexOf(d) !== -1)(
        query.filters.map(f => f.dimension)
      );

    const isAdditive = R.all(m => m.isAdditive(), query.measures);
    const leafMeasureAdditive = R.all(path => query.newMeasure(path).isAdditive(), leafMeasurePaths);

    return {
      sortedDimensions,
      sortedTimeDimensions,
      measures: measurePaths,
      leafMeasureAdditive,
      leafMeasures: leafMeasurePaths,
      hasNoTimeDimensionsWithoutGranularity,
      allFiltersWithinSelectedDimensions,
      isAdditive
    };
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
    return (refs) => {
      return PreAggregations.canUsePreAggregationForTransformedQueryFn(
        transformedQuery, refs
      );
    };
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
        timeDimensions.map(d => [d.dimension, d.granularity || 'date'])
      ) || [];
    }

    const canUsePreAggregationNotAdditive = (references) =>
      transformedQuery.hasNoTimeDimensionsWithoutGranularity &&
      transformedQuery.allFiltersWithinSelectedDimensions &&
      R.equals(references.sortedDimensions || references.dimensions, transformedQuery.sortedDimensions) &&
      (
        R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.measures) ||
        R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures)
      ) &&
      R.equals(
        transformedQuery.sortedTimeDimensions,
        references.sortedTimeDimensions || sortTimeDimensions(references.timeDimensions)
      );

    const canUsePreAggregationLeafMeasureAdditive = (references) =>
      R.all(
        d => (references.sortedDimensions || references.dimensions).indexOf(d) !== -1,
        transformedQuery.sortedDimensions
      ) &&
      R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures) &&
      R.allPass(
        transformedQuery.sortedTimeDimensions.map(td => R.contains(td))
      )(references.sortedTimeDimensions || sortTimeDimensions(references.timeDimensions));

    const canUsePreAggregationAdditive = (references) =>
      R.all(
        d => (references.sortedDimensions || references.dimensions).indexOf(d) !== -1,
        transformedQuery.sortedDimensions
      ) &&
      (
        R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.measures) ||
        R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures)
      ) &&
      R.allPass(
        transformedQuery.sortedTimeDimensions.map(td => R.contains(td))
      )(references.sortedTimeDimensions || sortTimeDimensions(references.timeDimensions));


    let canUseFn;
    if (transformedQuery.isAdditive) {
      canUseFn = canUsePreAggregationAdditive;
    } else if (transformedQuery.leafMeasureAdditive) {
      canUseFn = canUsePreAggregationLeafMeasureAdditive;
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

  getCubeLattice(cube, preAggregationName, preAggregation) {
    throw new UserError('Auto rollups supported only in Enterprise version');
  }

  findPreAggregationForQuery() {
    if (!this.preAggregationForQuery) {
      const query = this.query;

      if (PreAggregations.hasCumulativeMeasures(query)) {
        return null;
      }

      const canUsePreAggregation = this.canUsePreAggregationFn(query);

      this.preAggregationForQuery = R.pipe(
        R.map(cube => {
          const preAggregations = this.query.cubeEvaluator.preAggregationsForCube(cube);
          let rollupPreAggregations = R.pipe(
            R.toPairs,
            R.filter(([k, a]) => a.type === 'rollup'),
            R.filter(([k, aggregation]) => canUsePreAggregation(this.evaluateAllReferences(cube, aggregation))),
            R.map(([preAggregationName, preAggregation]) => ({ preAggregationName, preAggregation, cube }))
          )(preAggregations);
          if (
            R.any(m => m.path() && m.path()[0] === cube, this.query.measures) ||
            !this.query.measures.length && !this.query.timeDimensions.length &&
            R.all(d => d.path() && d.path()[0] === cube, this.query.dimensions)
          ) {
            const autoRollupPreAggregations = R.pipe(
              R.toPairs,
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
                  cube
                };
              })
            )(preAggregations);
            rollupPreAggregations = rollupPreAggregations.concat(autoRollupPreAggregations);
          }
          return rollupPreAggregations;
        }),
        R.unnest
      )(query.collectCubeNames())[0];
    }
    return this.preAggregationForQuery;
  }

  static hasCumulativeMeasures(query) {
    const measures = (query.measures.concat(query.measureFilters));
    return R.pipe(
      R.map(m => query.collectLeafMeasures(() => query.traverseSymbol(m))),
      R.unnest,
      R.uniq,
      R.map(p => query.newMeasure(p)),
      R.any(m => m.isCumulative())
    )(measures);
  }

  castGranularity(granularity) {
    // TODO replace date granularity with day
    if (granularity === 'day') {
      return 'date';
    }
    return granularity;
  }

  collectOriginalSqlPreAggregations(fn) {
    const preAggregations = [];
    const result = this.query.evaluateSymbolSqlWithContext(fn, { collectOriginalSqlPreAggregations: preAggregations });
    return { preAggregations, result };
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
      collectOriginalSqlPreAggregations: this.query.safeEvaluateSymbolContext().collectOriginalSqlPreAggregations
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
      collectOriginalSqlPreAggregations: this.query.safeEvaluateSymbolContext().collectOriginalSqlPreAggregations
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
      return toMerge ? Object.assign({}, d, { dateRange: toMerge.dateRange }) : d;
    });
  }

  autoRollupNameSuffix(cube, aggregation) {
    return '_' + aggregation.dimensions.concat(
      aggregation.timeDimensions.map(d => `${d.dimension}${d.granularity.substring(0, 1)}`)
    ).map(s => {
      const path = s.split('.');
      return `${path[0][0]}${path[1]}`;
    }).map(s => s.replace(/_/g, '')).join("_").replace(/[.]/g, '').toLowerCase();
  }

  evaluateAllReferences(cube, aggregation) {
    const timeDimensions = aggregation.timeDimensionReference ? [{
      dimension: this.evaluateReferences(cube, aggregation.timeDimensionReference),
      granularity: this.castGranularity(aggregation.granularity)
    }] : [];
    return {
      dimensions:
        (aggregation.dimensionReferences && this.evaluateReferences(cube, aggregation.dimensionReferences) || []).concat(
          aggregation.segmentReferences && this.evaluateReferences(cube, aggregation.segmentReferences) || []
        ),
      measures:
        aggregation.measureReferences && this.evaluateReferences(cube, aggregation.measureReferences) || [],
      timeDimensions
    };
  }

  evaluateReferences(cube, referencesFn) {
    return this.query.cubeEvaluator.evaluateReferences(cube, referencesFn);
  }

  rollupPreAggregation(preAggregationForQuery) {
    const table = preAggregationForQuery.preAggregation.partitionGranularity && this.query.timeDimensions.length ?
      this.partitionUnion(preAggregationForQuery) :
      this.query.preAggregationTableName(
        preAggregationForQuery.cube,
        preAggregationForQuery.preAggregationName
      );
    let segmentFilters = this.query.segments.map(s =>
      this.query.newFilter({ dimension: s.segment, operator: 'equals', values: [true] })
    );
    const filters =
      segmentFilters
        .concat(this.query.filters).concat(this.query.timeDimensions.map(dimension => dimension.dateRange && ({
          filterToWhere: () => this.query.timeRangeFilter(
            this.query.dimensionSql(dimension),
            this.query.timeStampInClientTz(dimension.dateFromParam()),
            this.query.timeStampInClientTz(dimension.dateToParam())
          )
        }))).filter(f => !!f);

    const renderedReference = R.pipe(
      R.map(path => {
        const measure = this.query.newMeasure(path);
        return [
          path,
          this.query.aggregateOnGroupedColumn(measure.measureDefinition(), measure.aliasName()) ||
          `sum(${measure.aliasName()})`
        ];
      }),
      R.fromPairs
    )(preAggregationForQuery.preAggregation.type === 'autoRollup' ?
      preAggregationForQuery.preAggregation.measures :
      this.evaluateAllReferences(preAggregationForQuery.cube, preAggregationForQuery.preAggregation).measures
    );

    return this.query.evaluateSymbolSqlWithContext(
      () => `SELECT ${this.query.baseSelect()} FROM ${table} ${this.query.baseWhere(filters)}` +
        this.query.groupByClause() +
        this.query.baseHaving(this.query.measureFilters) +
        this.query.orderBy() +
        this.query.groupByDimensionLimit(),
      {
        renderedReference,
        rollupQuery: true
      }
    );
  }

  partitionUnion(preAggregationForQuery) {
    const { dimension, partitionDimension } = this.partitionDimension(preAggregationForQuery);

    const union = partitionDimension.timeSeries().map(range => {
      const preAggregation = this.addPartitionRangeTo(preAggregationForQuery, dimension, range);
      return this.preAggregationTableName(
        preAggregationForQuery.cube,
        preAggregationForQuery.preAggregationName,
        preAggregation.preAggregation
      );
    }).map(table => `SELECT * FROM ${table}`).join(" UNION ALL ");
    return `(${union}) as partition_union`;
  }
}

module.exports = PreAggregations;
