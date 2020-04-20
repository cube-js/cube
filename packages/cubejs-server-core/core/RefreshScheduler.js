const uuid = require('uuid/v4');

class RefreshScheduler {
  constructor(serverCore) {
    this.serverCore = serverCore;
  }

  async refreshQueriesForPreAggregation(context, compilerApi, preAggregation, queryingOptions) {
    const dbType = compilerApi.getDbType();
    const compilers = await compilerApi.getCompilers();
    const query = compilerApi.createQuery(compilers, dbType, queryingOptions);
    if (preAggregation.preAggregation.partitionGranularity) {
      const dataSource = query.cubeDataSource(preAggregation.cube);

      const orchestratorApi = this.serverCore.getOrchestratorApi({ ...context, dataSource });
      const [startDate, endDate] =
        await Promise.all(
          query
            .preAggregationStartEndQueries(preAggregation.cube, preAggregation.preAggregation)
            .map(sql => orchestratorApi.executeQuery({
              query: sql[0],
              values: sql[1],
              continueWait: true,
              cacheKeyQueries: []
            }))
        );

      const extractDate = ({ data }) => {
        // TODO some backends return dates as objects here. Use ApiGateway data transformation ?
        data = JSON.parse(JSON.stringify(data));
        return data[0] && data[0][Object.keys(data[0])[0]];
      };

      const baseQuery = {
        ...queryingOptions,
        ...preAggregation.references,
        timeDimensions: [{
          ...preAggregation.references.timeDimensions[0],
          dateRange: [extractDate(startDate), extractDate(endDate)]
        }]
      };
      const partitionQuery = compilerApi.createQuery(compilers, dbType, baseQuery);
      const { partitionDimension } = partitionQuery.preAggregations.partitionDimension(preAggregation);
      return partitionDimension.timeSeries().map(range => ({
        ...baseQuery,
        timeDimensions: [{
          ...preAggregation.references.timeDimensions[0],
          dateRange: range
        }]
      }));
    } else if (preAggregation.preAggregation.type === 'rollup') {
      return [{ ...queryingOptions, ...preAggregation.references }];
    } else if (preAggregation.preAggregation.type === 'originalSql') {
      const cubeFromPath = query.cubeEvaluator.cubeFromPath(preAggregation.cube);
      const measuresCount = Object.keys(cubeFromPath.measures || {}).length;
      const dimensionsCount = Object.keys(cubeFromPath.dimensions || {}).length;
      if (measuresCount === 0 && dimensionsCount === 0) {
        throw new Error(
          `Can't refresh pre-aggregation without measures and dimensions: ${preAggregation.preAggregationName}`
        );
      }
      return [{
        ...queryingOptions,
        ...(
          measuresCount &&
            { measures: [`${preAggregation.cube}.${Object.keys(cubeFromPath.measures)[0]}`] }
        ),
        ...(
          dimensionsCount &&
            { dimensions: [`${preAggregation.cube}.${Object.keys(cubeFromPath.dimensions)[0]}`] }
        )
      }];
    } else {
      throw new Error(
        `Scheduled refresh is unsupported for ${preAggregation.preAggregation.type} of ${preAggregation.preAggregationName}`
      );
    }
  }

  async runScheduledRefresh(context, queryingOptions) {
    queryingOptions = { timezone: 'UTC', ...queryingOptions };
    const { throwErrors, ...restOptions } = queryingOptions;
    context = { requestId: `scheduler-${uuid()}`, ...context };
    this.serverCore.logger('Refresh Scheduler Run', {
      authInfo: context.authInfo,
      requestId: context.requestId
    });
    try {
      const compilerApi = this.serverCore.getCompilerApi(context);
      await Promise.all([
        this.refreshCubesRefreshKey(context, compilerApi, restOptions),
        this.refreshPreAggregations(context, compilerApi, restOptions)
      ]);
      return {
        finished: true
      };
    } catch (e) {
      if (e.error !== 'Continue wait') {
        this.serverCore.logger('Refresh Scheduler Error', {
          error: e.error || e.stack || e.toString(),
          authInfo: context.authInfo,
          requestId: context.requestId
        });
      }
      if (throwErrors) {
        throw e;
      }
    }
    return { finished: false };
  }

  async refreshCubesRefreshKey(context, compilerApi, queryingOptions) {
    const dbType = compilerApi.getDbType();
    const compilers = await compilerApi.getCompilers();
    const queryForEvaluation = compilerApi.createQuery(compilers, dbType, {});
    await Promise.all(queryForEvaluation.cubeEvaluator.cubeNamesWithRefreshKeys().map(async cube => {
      const cubeFromPath = queryForEvaluation.cubeEvaluator.cubeFromPath(cube);
      const measuresCount = Object.keys(cubeFromPath.measures || {}).length;
      const dimensionsCount = Object.keys(cubeFromPath.dimensions || {}).length;
      if (measuresCount === 0 && dimensionsCount === 0) {
        return;
      }
      const query = {
        ...queryingOptions,
        ...(
          measuresCount &&
          { measures: [`${cube}.${Object.keys(cubeFromPath.measures)[0]}`] }
        ),
        ...(
          dimensionsCount &&
          { dimensions: [`${cube}.${Object.keys(cubeFromPath.dimensions)[0]}`] }
        )
      };
      const sqlQuery = await compilerApi.getSql(query);
      const orchestratorApi = this.serverCore.getOrchestratorApi({ ...context, dataSource: sqlQuery.dataSource });
      await orchestratorApi.executeQuery({
        ...sqlQuery,
        preAggregations: [],
        query: 'SELECT 1', // TODO get rid off it
        continueWait: true,
        renewQuery: true,
        requestId: context.requestId
      });
    }));
  }

  async refreshPreAggregations(context, compilerApi, queryingOptions) {
    const scheduledPreAggregations = await compilerApi.scheduledPreAggregations();
    await Promise.all(scheduledPreAggregations.map(async preAggregation => {
      const queries = await this.refreshQueriesForPreAggregation(
        context, compilerApi, preAggregation, queryingOptions
      );
      await Promise.all(queries.map(async (query, i) => {
        const sqlQuery = await compilerApi.getSql(query);
        const orchestratorApi = this.serverCore.getOrchestratorApi({ ...context, dataSource: sqlQuery.dataSource });
        await orchestratorApi.executeQuery({
          ...sqlQuery,
          preAggregations: sqlQuery.preAggregations.map(
            (p) => ({ ...p, priority: i - queries.length })
          ),
          continueWait: true,
          renewQuery: true,
          requestId: context.requestId
        });
      }));
    }));
  }
}

module.exports = RefreshScheduler;
