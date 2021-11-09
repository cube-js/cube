const { Kafka } = require('kafkajs')
const express = require('express')
var cors = require('cors')
const bodyParser = require('body-parser');

const username = '5RGJS3US4PUQKAZ5'
const password = 'y7j6w8Mb8wfA+/2+BYlJWqq5DnUmn3NbkbV7E7wCJuo/8f0Nr/BzK3ty+M1ihSAN'
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
