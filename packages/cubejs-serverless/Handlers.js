const express = require('serverless-express/express');
const handler = require('serverless-express/handler');
const cors = require('cors');
const ServerCore = require('@cubejs-backend/server-core');

const processHandlers = {
  queryProcess: async ({ queryKey, dataSource }, orchestrator) => {
    const queue = orchestrator.queryCache.getQueue(dataSource);
    await queue.processQuery(queryKey);
  },
  queryCancel: async ({ query, dataSource }, orchestrator) => {
    const queue = orchestrator.queryCache.getQueue(dataSource);
    await queue.processCancel(query);
  },
  externalQueryProcess: async (queryKey, orchestrator) => {
    const queue = orchestrator.queryCache.getExternalQueue();
    await queue.processQuery(queryKey);
  },
  externalQueryCancel: async (query, orchestrator) => {
    const queue = orchestrator.queryCache.getExternalQueue();
    await queue.processCancel(query);
  },
  preAggregationProcess: async ({ queryKey, dataSource }, orchestrator) => {
    const queue = orchestrator.preAggregations.getQueue(dataSource);
    await queue.processQuery(queryKey);
  },
  preAggregationCancel: async ({ query, dataSource }, orchestrator) => {
    const queue = orchestrator.preAggregations.getQueue(dataSource);
    await queue.processCancel(query);
  }
};

class Handlers {
  constructor(options) {
    options = {
      ...options,
      orchestratorOptions: (context) => ({
        ...(options && options.orchestratorOptions),
        queryCacheOptions: {
          ...(options && options.orchestratorOptions && options.orchestratorOptions.queryCacheOptions),
          queueOptions: (dataSource) => ({
            sendProcessMessageFn: async (queryKey) => this.sendNotificationMessage({ queryKey, dataSource }, 'queryProcess', context),
            sendCancelMessageFn: async (query) => this.sendNotificationMessage({ query, dataSource }, 'queryCancel', context),
            ...(
              options &&
              options.orchestratorOptions &&
              options.orchestratorOptions.queryCacheOptions &&
              options.orchestratorOptions.queryCacheOptions.queueOptions
            )
          }),
          externalQueueOptions: {
            sendProcessMessageFn: async (queryKey) => this.sendNotificationMessage(queryKey, 'externalQueryProcess', context),
            sendCancelMessageFn: async (query) => this.sendNotificationMessage(query, 'externalQueryCancel', context),
            ...(
              options &&
              options.orchestratorOptions &&
              options.orchestratorOptions.queryCacheOptions &&
              options.orchestratorOptions.queryCacheOptions.externalQueueOptions
            )
          }
        },
        preAggregationsOptions: {
          ...(options && options.orchestratorOptions && options.orchestratorOptions.preAggregationsOptions),
          queueOptions: (dataSource) => ({
            sendProcessMessageFn: async (queryKey) => this.sendNotificationMessage({ queryKey, dataSource }, 'preAggregationProcess', context),
            sendCancelMessageFn: async (query) => this.sendNotificationMessage({ query, dataSource }, 'preAggregationCancel', context),
            ...(
              options &&
              options.orchestratorOptions &&
              options.orchestratorOptions.preAggregationsOptions &&
              options.orchestratorOptions.preAggregationsOptions.queueOptions
            )
          })
        }
      })
    };
    this.serverCore = new ServerCore(options);
  }

  // eslint-disable-next-line no-unused-vars
  async sendNotificationMessage(message, type, context) {
    throw new Error('sendNotificationMessage is not implemented');
  }

  getApiHandler() {
    if (!this.apiHandler) {
      const app = express();
      app.use(cors({
        allowedHeaders: 'authorization,content-type,x-request-id',
      }));

      this.serverCore.initApp(app);
      this.apiHandler = handler(app);
    }
    return this.apiHandler;
  }

  api(event, context) {
    return this.getApiHandler()(event, context);
  }

  // eslint-disable-next-line no-unused-vars
  async process(event) {
    throw new Error('process is not implemented');
  }

  async processMessage(message) {
    const processFn = processHandlers[message.type];
    if (!processFn) {
      throw new Error(`Unrecognized message type: ${message.type}`);
    }
    const orchestratorApi = this.serverCore.getOrchestratorApi(message.context);
    await processFn(message.message, orchestratorApi.orchestrator);
  }
}

module.exports = Handlers;
