const aws = require('aws-sdk');
const sns = new aws.SNS();

const topicArn = (topic) => `arn:aws:sns:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT_ID}:${topic}`;
const sendSnsMessage = async (message, topic) => {
  const params = {
    Message: JSON.stringify(message),
    TopicArn: topicArn(topic)
  };
  await sns.publish(params).promise();
};

exports.serverCore = require('@cubejs-backend/server-core').create({
  orchestratorOptions: {
    queryCacheOptions: {
      queueOptions: {
        sendProcessMessageFn: async (queryKey) => sendSnsMessage(queryKey, 'cubejs-query-process'),
        sendCancelMessageFn: async (query) => sendSnsMessage(query, 'cubejs-query-cancel')
      }
    },
    preAggregationsOptions: {
      queueOptions: {
        sendProcessMessageFn: async (queryKey) => sendSnsMessage(queryKey, 'cubejs-pre-aggregation-process'),
        sendCancelMessageFn: async (query) => sendSnsMessage(query, 'cubejs-pre-aggregation-cancel')
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

const processMessage = async (event, processFn) => {
  await Promise.all(event.Records.map(async record => {
    const message = JSON.parse(record.Sns.Message);
    const orchestratorApi = await getOrchestratorApi();
    await processFn(message, orchestratorApi.orchestrator);
  }));

  return {
    statusCode: 200
  }
};

exports.queryProcess = async (event) => {
  return processMessage(event, async (queryKey, orchestrator) => {
    const queue = orchestrator.queryCache.getQueue();
    await queue.processQuery(queryKey);
  });
};

exports.queryCancel = async (event) => {
  return processMessage(event, async (query, orchestrator) => {
    const queue = orchestrator.queryCache.getQueue();
    await queue.processCancel(query);
  });
};

exports.preAggregationProcess = async (event) => {
  return processMessage(event, async (queryKey, orchestrator) => {
    const queue = orchestrator.preAggregations.getQueue();
    await queue.processQuery(queryKey);
  });
};

exports.preAggregationCancel = async (event) => {
  return processMessage(event, async (query, orchestrator) => {
    const queue = orchestrator.preAggregations.getQueue();
    await queue.processCancel(query);
  });
};