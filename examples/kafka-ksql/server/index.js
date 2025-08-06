const { Kafka } = require('kafkajs')
const express = require('express')
var cors = require('cors')
const bodyParser = require('body-parser');

const { KAFKA_USERNAME: username, KAFKA_PASSWORD: password } = process.env
const sasl = username && password ? { username, password, mechanism: 'plain' } : null
const ssl = !!sasl

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['pkc-4yyd6.us-east1.gcp.confluent.cloud:9092'],
  ssl,
  sasl
})
const producer = kafka.producer()

async function initProducer() {
  await producer.connect()
}

async function sendMessage (producer, message) {
  await producer.send({
    topic: 'real_time_demo',
    messages: [
      { value: JSON.stringify(message) }
    ],
  });
}

const app = express()
const port = 4000

initProducer(producer);
app.use(cors())
app.use(bodyParser.json());

app.post('/', (req, res) => {
  console.log("Message received");
  console.log(req.body);
  sendMessage(producer, { time: new Date().toISOString(), ...req.body })
  res.send('success')
})

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`)
})


process.on('SIGTERM', () => {
  producer.disconnect();
});
