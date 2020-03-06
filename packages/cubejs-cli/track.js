const fetch = require('node-fetch');
const crypto = require('crypto');

let flushPromise = null;
let trackEvents = [];

module.exports = async (event) => {
  trackEvents.push({
    ...event,
    id: crypto.randomBytes(16).toString('hex'),
    clientTimestamp: new Date().toJSON()
  });
  const flush = async (toFlush, retries) => {
    if (!toFlush) {
      toFlush = trackEvents;
      trackEvents = [];
    }
    if (!toFlush.length) {
      return null;
    }
    if (retries == null) {
      retries = 10;
    }
    try {
      const sentAt = new Date().toJSON();
      const result = await fetch('https://track.cube.dev/track', {
        method: 'post',
        body: JSON.stringify(toFlush.map(r => ({ ...r, sentAt }))),
        headers: { 'Content-Type': 'application/json' },
      });
      if (result.status !== 200 && retries > 0) {
        return flush(toFlush, retries - 1);
      }
      // console.log(await result.json());
    } catch (e) {
      if (retries > 0) {
        return flush(toFlush, retries - 1);
      }
      // console.log(e);
    }
    return null;
  };
  const currentPromise = (flushPromise || Promise.resolve()).then(() => flush()).then(() => {
    if (currentPromise === flushPromise) {
      flushPromise = null;
    }
  });
  flushPromise = currentPromise;
  return flushPromise;
};
