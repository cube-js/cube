import * as dotenv from 'dotenv'
import { Kafka } from 'kafkajs'

dotenv.config()

const { KAFKA_USERNAME: username, KAFKA_PASSWORD: password } = process.env
const sasl = username && password ? { username, password, mechanism: 'plain' } : null
const ssl = process.env.KAFKA_SSL || !!sasl

const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID,
  brokers: [ process.env.KAFKA_BOOTSTRAP_SERVER ],
  ssl,
  sasl
})

const producer = kafka.producer()

export async function produce(messages) {
  await producer.connect()

  await producer.send({
    topic: process.env.KAFKA_TOPIC,
    messages
  })

  await producer.disconnect()

  console.log('Sent to Kafka.')
}

export function wrap(entries, extractKey) {
  return entries.map(entry => ({
    key: extractKey(entry),
    value: JSON.stringify(entry)
  }))
}