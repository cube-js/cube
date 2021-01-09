/* globals jest, beforeAll, afterAll */
const { GenericContainer } = require('testcontainers');
const AWS = require('aws-sdk');
const QueryQueueTest = require('../unit/QueryQueue.test');

let container;

const dynamodbLocalVersion = process.env.TEST_LOCAL_DYNAMO_DB_VERSION || 'latest';
const dynamoPort = 8000;

jest.setTimeout(30000);

beforeAll(async () => {
  container = await new GenericContainer('amazon/dynamodb-local', dynamodbLocalVersion)
    .withExposedPorts(dynamoPort)
    .start();

  process.env.CUBEJS_CACHE_TABLE = 'testtable';
  process.env.AWS_REGION = 'us-west-2';

  const mappedPort = container.getMappedPort(dynamoPort);
  const host = container.getHost();

  const endpoint = `http://${host}:${mappedPort}`;

  // Configure the AWS SDK so that it doesn't get mad
  AWS.config.region = 'us-east-1';
  AWS.config.endpoint = new AWS.Endpoint(endpoint);

  const createTableParams = {
    TableName: process.env.CUBEJS_CACHE_TABLE,
    KeySchema: [
      { AttributeName: 'pk', KeyType: 'HASH' }, // Partition key
      { AttributeName: 'sk', KeyType: 'RANGE' } // Sort key
    ],
    AttributeDefinitions: [
      { AttributeName: 'pk', AttributeType: 'S' },
      { AttributeName: 'sk', AttributeType: 'S' },
      { AttributeName: 'GSI1sk', AttributeType: 'N' },
    ],
    ProvisionedThroughput: {
      ReadCapacityUnits: 10,
      WriteCapacityUnits: 10
    },
    GlobalSecondaryIndexes: [
      {
        IndexName: 'GSI1',
        KeySchema: [
          { AttributeName: 'pk', KeyType: 'HASH' },
          { AttributeName: 'GSI1sk', KeyType: 'RANGE' },
        ],
        Projection: {
          ProjectionType: 'ALL'
        },
        ProvisionedThroughput: {
          ReadCapacityUnits: 10,
          WriteCapacityUnits: 10
        }
      }
    ],
  };

  const dynamodb = new AWS.DynamoDB();
  await dynamodb.createTable(createTableParams).promise();
});

afterAll(async () => {
  if (container) await container.stop();
});

QueryQueueTest('DynamoDB', { cacheAndQueueDriver: 'dynamodb' });
