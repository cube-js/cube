const aws = require('aws-sdk');
const sns = new aws.SNS();

const topicArn = (topic) => `arn:aws:sns:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT_ID}:${topic}`;
const sendSnsMessage = async (message, type) => {
  const params = {
    Message: JSON.stringify({ message, type }),
    TopicArn: topicArn(`${process.env.CUBEJS_APP || 'cubejs'}-process`)
  };
  await sns.publish(params).promise();
};

exports.serverCore = require('@cubejs-backend/server-core').create({
  orchestratorOptions: {
    queryCacheOptions: {
      queueOptions: {
        sendProcessMessageFn: async (queryKey) => sendSnsMessage(queryKey, 'queryProcess'),
        sendCancelMessageFn: async (query) => sendSnsMessage(query, 'queryCancel')
      }
    },
    preAggregationsOptions: {
      queueOptions: {
        sendProcessMessageFn: async (queryKey) => sendSnsMessage(queryKey, 'preAggregationProcess'),
        sendCancelMessageFn: async (query) => sendSnsMessage(query, 'preAggregationCancel')
      }
    }
  }
});

const getHandler = () => {
  if (!exports.apiHandler) {
    const express = require('serverless-express/express');
    const app = express();
    app.use(require('cors')());
    exports.serverCore.initApp(app);
    const handler = require('serverless-express/handler');
    exports.apiHandler = handler(app);
  }
  return exports.apiHandler;
};


exports.api = (event, context) => getHandler()(event, context);

let orchestratorApi;

const getOrchestratorApi = async () => {
  if (!orchestratorApi) {
    orchestratorApi = await exports.serverCore.createOrchestratorApi();
  }
  return orchestratorApi;
};

const handlers = {
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

const processMessage = async (event) => {
  await Promise.all(event.Records.map(async record => {
    const message = JSON.parse(record.Sns.Message);
    let processFn = handlers[message.type];
    if (!processFn) {
      throw new Error(`Unrecognized message type: ${message.type}`);
    }
    const orchestratorApi = await getOrchestratorApi();
    await processFn(message.message, orchestratorApi.orchestrator);
  }));

  return {
    statusCode: 200
  }
};

exports.process = async (event) => {
  return processMessage(event);
};