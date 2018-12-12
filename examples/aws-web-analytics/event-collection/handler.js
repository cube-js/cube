const AWS = require('aws-sdk');
const { promisify } = require('util');

const kinesis = new AWS.Kinesis();

const putRecord = promisify(kinesis.putRecord.bind(kinesis));

const response = (body, status) => {
  return {
    statusCode: status || 200,
    body: body && JSON.stringify(body),
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Credentials': true,
      'Content-Type': 'application/json'
    }
  }
}

module.exports.collect = async (event, context) => {
  const body = JSON.parse(event.body);
  if (!body.anonymousId || !body.url || !body.eventType) {
    return response({
      error: 'anonymousId, url and eventType required'
    }, 400);
  }

  await putRecord({
    Data: JSON.stringify({
      anonymous_id: body.anonymousId,
      url: body.url,
      event_type: body.eventType,
      referrer: body.referrer,
      timestamp: (new Date()).toISOString(),
      source_ip: event.requestContext.identity.sourceIp,
      user_agent: event.requestContext.identity.userAgent
    }) + '\n',
    PartitionKey: body.anonymousId,
    StreamName: 'event-collection'
  });

  return response();
};
