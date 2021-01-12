const fetch = require('node-fetch');
const crypto = require('crypto');

let flushPromise = null;
const trackEvents = [];

export default async (event, endpointUrl, logger) => {
  trackEvents.push({
    ...event,
    id: crypto.randomBytes(16).toString('hex'),
    timestamp: new Date().toJSON()
  });
  const flush = async (toFlush, retries) => {
    if (!toFlush) {
      toFlush = trackEvents.splice(0, 50);
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
  const flushCycle = async () => {
    for (let i = 0; i < 1000; i++) {
      if (!await flush()) {
        return;
      }
    }
  };
  if (!flushPromise) {
    flushPromise = flushCycle().then(() => {
      flushPromise = null;
    });
  }

  return flushPromise;
};
