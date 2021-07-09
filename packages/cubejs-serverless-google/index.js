const Handlers = require('@cubejs-backend/serverless/Handlers');

const { PubSub } = require('@google-cloud/pubsub');

class GoogleHandlers extends Handlers {
  constructor(options) {
    super(options);
    this.api = this.api.bind(this);
    this.process = this.process.bind(this);
    this.pubsub = new PubSub();
  }

  topicName() {
    let topic = `${process.env.CUBEJS_APP || 'cubejs'}-process`
    if (process.env.CUBEJS_TOPIC_NAME) {
      topic = process.env.CUBEJS_TOPIC_NAME
    }
    return topic
  }

  async sendNotificationMessage(message, type, context) {
    await this.pubsub.topic(this.topicName()).publish(Buffer.from(JSON.stringify({ message, type, context })));
  }

  async process(event) {
    const message = JSON.parse(Buffer.from(event.data, 'base64').toString());
    await this.processMessage(message);
  }
}

module.exports = GoogleHandlers;
