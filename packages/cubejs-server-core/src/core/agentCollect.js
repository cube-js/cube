import { getEnv } from '@cubejs-backend/shared';

const fetch = require('node-fetch');
const crypto = require('crypto');

const trackEvents = [];
let agentInterval = null;
let lastEvent;

export default async (event, endpointUrl, logger) => {
  trackEvents.push({
    ...event,
    id: crypto.randomBytes(16).toString('hex'),
    timestamp: new Date().toJSON()
  });
  lastEvent = new Date();
  const flush = async (toFlush, retries) => {
    if (!toFlush) {
      toFlush = trackEvents.splice(0, getEnv('agentFrameSize'));
    }
    if (!toFlush.length) {
      return false;
    }
    if (retries == null) {
      retries = 3;
    }
    try {
      const sentAt = new Date().toJSON();
      const result = await fetch(endpointUrl, {
        method: 'post',
        body: JSON.stringify(toFlush.map(r => ({ ...r, sentAt }))),
        headers: { 'Content-Type': 'application/json' },
      });
      if (result.status !== 200 && retries > 0) {
        return flush(toFlush, retries - 1);
      }
      // console.log(await result.json());
      return true;
    } catch (e) {
      if (retries > 0) {
        return flush(toFlush, retries - 1);
      }
      logger('Agent Error', { error: (e.stack || e).toString() });
    }
    return true;
  };
  if (!agentInterval) {
    agentInterval = setInterval(async () => {
      if (trackEvents.length) {
        await flush();
      } else if (new Date().getTime() - lastEvent.getTime() > 3000) {
        clearInterval(agentInterval);
        agentInterval = null;
      }
    }, 1000);
  }
};
