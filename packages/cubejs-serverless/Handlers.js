const aws = require('aws-sdk');
const express = require('serverless-express/express');
const handler = require('serverless-express/handler');
const cors = require('cors');
const ServerCore = require('@cubejs-backend/server-core');

const sns = new aws.SNS();

const topicArn = (topic) => `arn:aws:sns:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT_ID}:${topic}`;
const sendSnsMessage = async (message, type, context) => {
  const params = {
    Message: JSON.stringify({ message, type, context }),
    TopicArn: topicArn(`${process.env.CUBEJS_APP || 'cubejs'}-process`)
  };
  await sns.publish(params).promise();
};

const processHandlers = {
  queryProcess: async (queryKey, orchestrator) => {
    const queue = orchestrator.queryCache.getQueue();
    await queue.processQuery(queryKey);
  },
  queryCancel: async (query, orchestrator) => {
    const queue = orchestrator.queryCache.getQueue();
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
      ...options,
      orchestratorOptions: (context) => ({
        queryCacheOptions: {
          queueOptions: {
            sendProcessMessageFn: async (queryKey) => sendSnsMessage(queryKey, 'queryProcess', context),
            sendCancelMessageFn: async (query) => sendSnsMessage(query, 'queryCancel', context)
          }
        },
        preAggregationsOptions: {
          queueOptions: {
            sendProcessMessageFn: async (queryKey) => sendSnsMessage(queryKey, 'preAggregationProcess', context),
            sendCancelMessageFn: async (query) => sendSnsMessage(query, 'preAggregationCancel', context)
          }
        }
      })
    };
    this.serverCore = new ServerCore(options);
    this.api = this.api.bind(this);
    this.process = this.process.bind(this);
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

  async process(event) {
    await Promise.all(event.Records.map(async record => {
      const message = JSON.parse(record.Sns.Message);
      const processFn = processHandlers[message.type];
      if (!processFn) {
        throw new Error(`Unrecognized message type: ${message.type}`);
      }
      const orchestratorApi = this.serverCore.getOrchestratorApi(message.context);
      await processFn(message.message, orchestratorApi.orchestrator);
    }));

    return {
      statusCode: 200
    };
  }
}

module.exports = Handlers;
