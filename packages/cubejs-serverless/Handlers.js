const express = require('serverless-express/express');
const handler = require('serverless-express/handler');
const cors = require('cors');
const ServerCore = require('@cubejs-backend/server-core');

const processHandlers = {
  queryProcess: async (queryKey, orchestrator) => {
    const queue = orchestrator.queryCache.getQueue();
    await queue.processQuery(queryKey);
  },
  queryCancel: async (query, orchestrator) => {
    const queue = orchestrator.queryCache.getQueue();
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
  preAggregationProcess: async (queryKey, orchestrator) => {
    const queue = orchestrator.preAggregations.getQueue();
    await queue.processQuery(queryKey);
  },
  preAggregationCancel: async (query, orchestrator) => {
    const queue = orchestrator.preAggregations.getQueue();
    await queue.processCancel(query);
  }
};

class Handlers {
  constructor(options) {
    options = {
      orchestratorOptions: (context) => ({
        queryCacheOptions: {
          queueOptions: {
            sendProcessMessageFn: async (queryKey) => this.sendNotificationMessage(queryKey, 'queryProcess', context),
            sendCancelMessageFn: async (query) => this.sendNotificationMessage(query, 'queryCancel', context)
          },
          externalQueueOptions: {
            sendProcessMessageFn: async (queryKey) => this.sendNotificationMessage(queryKey, 'externalQueryProcess', context),
            sendCancelMessageFn: async (query) => this.sendNotificationMessage(query, 'externalQueryCancel', context)
          }
        },
        preAggregationsOptions: {
          queueOptions: {
            sendProcessMessageFn: async (queryKey) => this.sendNotificationMessage(queryKey, 'preAggregationProcess', context),
            sendCancelMessageFn: async (query) => this.sendNotificationMessage(query, 'preAggregationCancel', context)
          }
        }
      }),
      ...options
    };
    this.serverCore = new ServerCore(options);
  }

  // eslint-disable-next-line no-unused-vars
  async sendNotificationMessage(message, type, context) {
    throw new Error(`sendNotificationMessage is not implemented`);
  }

  getApiHandler() {
    if (!this.apiHandler) {
      const app = express();
      app.use(cors());
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
    throw new Error(`process is not implemented`);
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
