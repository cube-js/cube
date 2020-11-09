import { loadCliManifest } from './utils';

const fetch = require('node-fetch');
const crypto = require('crypto');

export type BaseEvent = any;
export type Event = BaseEvent & any;

let flushPromise: Promise<any>|null = null;
let trackEvents: Array<Event> = [];

const flush = async (toFlush?: Array<Event>, retries: number = 10): Promise<any> => {
  if (!toFlush) {
    toFlush = trackEvents;
    trackEvents = [];
  }

  if (!toFlush.length) {
    return;
  }

  try {
    const sentAt = new Date().toJSON();
    const result = await fetch('https://track.cube.dev/track', {
      method: 'post',
      body: JSON.stringify(toFlush.map(r => ({ ...r, sentAt }))),
      headers: { 'Content-Type': 'application/json' },
    });
    if (result.status !== 200 && retries > 0) {
      // eslint-disable-next-line consistent-return
      return flush(toFlush, retries - 1);
    }

    // console.log(await result.json());
  } catch (e) {
    if (retries > 0) {
      // eslint-disable-next-line consistent-return
      return flush(toFlush, retries - 1);
    }
    // console.log(e);
  }
};

export const track = async (event: BaseEvent) => {
  const cliManifest = loadCliManifest();

  trackEvents.push({
    ...event,
    id: crypto.randomBytes(16).toString('hex'),
    clientTimestamp: new Date().toJSON(),
    cliVersion: cliManifest.version,
  });

  const currentPromise = (flushPromise || Promise.resolve()).then(() => flush()).then(() => {
    if (currentPromise === flushPromise) {
      flushPromise = null;
    }
  });

  flushPromise = currentPromise;
  return flushPromise;
};
