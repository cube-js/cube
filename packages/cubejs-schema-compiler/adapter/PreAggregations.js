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
    const isInPreAggregationQuery = this.query.options.preAggregationQuery;
    if (!isInPreAggregationQuery) {
      const preAggregationForQuery = this.findPreAggregationForQuery();
      if (preAggregationForQuery) {
        return this.preAggregationDescriptionsFor(preAggregationForQuery.cube, preAggregationForQuery);
      }
    }
    if (
      !isInPreAggregationQuery ||
      isInPreAggregationQuery && this.query.options.useOriginalSqlPreAggregationsInPreAggregation) {
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
    return [];
  }

  preAggregationCubes() {
    const { join } = this.query;
    return join.joins.map(j => j.originalTo).concat([join.root]);
  }

  preAggregationDescriptionsFor(cube, foundPreAggregation) {
    if (this.canPartitionsBeUsed(foundPreAggregation)) {
      const { dimension, partitionDimension } = this.partitionDimension(foundPreAggregation);
      return R.unnest(partitionDimension.timeSeries().map(
        range => this.preAggregationDescriptionsForRecursive(
          cube, this.addPartitionRangeTo(foundPreAggregation, dimension, range)
        )
      ));
    }
    return this.preAggregationDescriptionsForRecursive(cube, foundPreAggregation);
  }

  canPartitionsBeUsed(foundPreAggregation) {
    return foundPreAggregation.preAggregation.partitionGranularity &&
      this.query.timeDimensions.length &&
      foundPreAggregation.references.timeDimensions &&
      foundPreAggregation.references.timeDimensions.length &&
      this.query.timeDimensions.find(td => td.dimension === foundPreAggregation.references.timeDimensions[0].dimension);
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
    const { dimension } = this.query.timeDimensions[0];
    const partitionDimension = this.query.newTimeDimension({
      dimension,
      granularity: this.castGranularity(foundPreAggregation.preAggregation.partitionGranularity),
      dateRange: this.query.timeDimensions[0].dateRange
    });
    return { dimension, partitionDimension };
  }

  preAggregationDescriptionsForRecursive(cube, foundPreAggregation) {
    const query = this.query.preAggregationQueryForSqlEvaluation(cube, foundPreAggregation.preAggregation);
    const descriptions = query !== this.query ? query.preAggregations.preAggregationsDescription() : [];
    return descriptions.concat(this.preAggregationDescriptionFor(cube, foundPreAggregation));
  }

  preAggregationDescriptionFor(cube, foundPreAggregation) {
    const { preAggregationName, preAggregation } = foundPreAggregation;
    const tableName = this.preAggregationTableName(cube, preAggregationName, preAggregation);
    const refreshKeyQueries = this.query.preAggregationInvalidateKeyQueries(cube, preAggregation);
    return {
      preAggregationsSchema: this.query.preAggregationSchema(),
      tableName,
      loadSql: this.query.preAggregationLoadSql(cube, preAggregation, tableName),
      sql: this.query.preAggregationSql(cube, preAggregation),
      invalidateKeyQueries: refreshKeyQueries.queries,
      refreshKeyRenewalThresholds: refreshKeyQueries.refreshKeyRenewalThresholds,
      external: preAggregation.external,
      indexesSql: Object.keys(preAggregation.indexes || {}).map(
        index => {
          const indexName = this.preAggregationTableName(cube, `${preAggregationName}_${index}`, preAggregation, true);
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
    let partitionSuffix = '';
    if (preAggregation.partitionTimeDimensions) {
      const partitionTimeDimension = preAggregation.partitionTimeDimensions[0];
      partitionSuffix = partitionTimeDimension.dateRange[0].substring(
        0,
        preAggregation.partitionGranularity === 'hour' ? 13 : 10
      ).replace(/[-T:]/g, '');
    }
    return this.query.preAggregationTableName(
      cube,
      preAggregationName + partitionSuffix,
      skipSchema
    );
  }

  findPreAggregationToUseForCube(cube) {
    const preAggregates = this.query.cubeEvaluator.preAggregationsForCube(cube);
    const originalSqlPreAggregations = R.pipe(
      R.toPairs,
      // eslint-disable-next-line no-unused-vars
      R.filter(([k, a]) => a.type === 'originalSql')
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
    const measures = (query.measures.concat(query.measureFilters));
    const measurePaths = R.uniq(measures.map(m => m.measure));
    const collectLeafMeasures = query.collectLeafMeasures.bind(query);
    const leafMeasurePaths =
      R.pipe(
        R.map(m => query.collectFrom([m], collectLeafMeasures, 'collectLeafMeasures')),
        R.unnest,
        R.uniq
      )(measures);

    function sortTimeDimensions(timeDimensions) {
      return timeDimensions && R.sortBy(
        R.prop(0),
        timeDimensions.map(d => [d.dimension, d.rollupGranularity()])
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
    const granularityHierarchies = query.granularityHierarchies();
    const hasMultipliedMeasures = query.fullKeyQueryAggregateMeasures().multipliedMeasures.length > 0;

    return {
      sortedDimensions,
      sortedTimeDimensions,
      measures: measurePaths,
      leafMeasureAdditive,
      leafMeasures: leafMeasurePaths,
      hasNoTimeDimensionsWithoutGranularity,
      allFiltersWithinSelectedDimensions,
      isAdditive,
      granularityHierarchies,
      hasMultipliedMeasures
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
    // TimeDimension :: [Dimension, Granularity]
    // TimeDimension -> [TimeDimension]
    function expandTimeDimension(timeDimension) {
      const [dimension, granularity] = timeDimension;
      const makeTimeDimension = newGranularity => [dimension, newGranularity];
      return (transformedQuery.granularityHierarchies[granularity] || [granularity]).map(makeTimeDimension);
    }
    // [[TimeDimension]]
    const queryTimeDimensionsList = transformedQuery.sortedTimeDimensions.map(expandTimeDimension);

    const canUsePreAggregationNotAdditive = (references) => transformedQuery.hasNoTimeDimensionsWithoutGranularity &&
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

    const canUsePreAggregationLeafMeasureAdditive = (references) => R.all(
      d => (references.sortedDimensions || references.dimensions).indexOf(d) !== -1,
      transformedQuery.sortedDimensions
    ) &&
      R.all(m => references.measures.indexOf(m) !== -1, transformedQuery.leafMeasures) &&
      R.allPass(
        queryTimeDimensionsList.map(tds => R.anyPass(tds.map(td => R.contains(td))))
      )(references.sortedTimeDimensions || sortTimeDimensions(references.timeDimensions));

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

    if (PreAggregations.hasCumulativeMeasures(query)) {
      return [];
    }

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
      R.filter(([k, a]) => a.type === 'rollup'),
      R.map(([preAggregationName, preAggregation]) => {
        const references = this.evaluateAllReferences(cube, preAggregation);
        return {
          preAggregationName,
          preAggregation,
          cube,
          canUsePreAggregation: canUsePreAggregation(references),
          references
        };
      })
    )(preAggregations);
  }

  rollupMatchResultDescriptions() {
    return this.rollupMatchResults().map(p => ({
      ...this.preAggregationDescriptionFor(p.cube, p),
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
      return toMerge ? Object.assign({}, d, { dateRange: toMerge.dateRange }) : d;
    });
  }

  autoRollupNameSuffix(cube, aggregation) {
    // eslint-disable-next-line prefer-template
    return '_' + aggregation.dimensions.concat(
      aggregation.timeDimensions.map(d => `${d.dimension}${d.granularity.substring(0, 1)}`)
    ).map(s => {
      const path = s.split('.');
      return `${path[0][0]}${path[1]}`;
    }).map(s => s.replace(/_/g, '')).join("_")
      .replace(/[.]/g, '')
      .toLowerCase();
  }

  evaluateAllReferences(cube, aggregation) {
    return this.query.cubeEvaluator.evaluatePreAggregationReferences(cube, aggregation);
  }

  originalSqlPreAggregationTable(preAggregation) {
    return this.canPartitionsBeUsed(preAggregation) ?
      this.partitionUnion(preAggregation, true) :
      this.query.preAggregationTableName(
        preAggregation.cube,
        preAggregation.preAggregationName
      );
  }

  rollupPreAggregation(preAggregationForQuery) {
    const table = this.canPartitionsBeUsed(preAggregationForQuery) ?
      this.partitionUnion(preAggregationForQuery) :
      this.query.preAggregationTableName(
        preAggregationForQuery.cube,
        preAggregationForQuery.preAggregationName
      );
    const segmentFilters = this.query.segments.map(
      s => this.query.newFilter({ dimension: s.segment, operator: 'equals', values: [true] })
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
      this.evaluateAllReferences(preAggregationForQuery.cube, preAggregationForQuery.preAggregation).measures);

    // TODO granularity shouldn't be null?
    const rollupGranularity = this.castGranularity(preAggregationForQuery.preAggregation.granularity) || 'day';

    return this.query.evaluateSymbolSqlWithContext(
      // eslint-disable-next-line prefer-template
      () => `SELECT ${this.query.baseSelect()} FROM ${table} ${this.query.baseWhere(filters)}` +
        this.query.groupByClause() +
        this.query.baseHaving(this.query.measureFilters) +
        this.query.orderBy() +
        this.query.groupByDimensionLimit(),
      {
        renderedReference,
        rollupQuery: true,
        rollupGranularity,
      }
    );
  }

  partitionUnion(preAggregationForQuery, withoutAlias) {
    const { dimension, partitionDimension } = this.partitionDimension(preAggregationForQuery);

    const tables = partitionDimension.timeSeries().map(range => {
      const preAggregation = this.addPartitionRangeTo(preAggregationForQuery, dimension, range);
      return this.preAggregationTableName(
        preAggregationForQuery.cube,
        preAggregationForQuery.preAggregationName,
        preAggregation.preAggregation
      );
    });
    if (tables.length === 1) {
      return tables[0];
    }
    const union = tables.map(table => `SELECT * FROM ${table}`).join(" UNION ALL ");
    return `(${union})${withoutAlias ? '' : ' as partition_union'}`;
  }
}

module.exports = PreAggregations;
