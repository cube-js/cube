const Handlers = require('@cubejs-backend/serverless/Handlers');

const aws = require('aws-sdk');

const sns = new aws.SNS();

class AWSHandlers extends Handlers {
  constructor(options) {
    super(options);
    this.api = this.api.bind(this);
    this.process = this.process.bind(this);
  }

  topicArn(topic) {
    return `arn:aws:sns:${process.env.AWS_REGION}:${process.env.AWS_ACCOUNT_ID}:${topic}`;
  }

  async sendNotificationMessage(message, type, context) {
    const params = {
      Message: JSON.stringify({ message, type, context }),
      TopicArn: this.topicArn(`${process.env.CUBEJS_APP || 'cubejs'}-process`)
    };
    await sns.publish(params).promise();
  }

  async process(event) {
    await Promise.all(event.Records.map(async record => {
      const message = JSON.parse(record.Sns.Message);
      await this.processMessage(message);
    }));

    return {
      statusCode: 200
    };
  }
}

module.exports = AWSHandlers;
