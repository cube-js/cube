import crypto from 'crypto';
import fetch from 'node-fetch';
import { machineIdSync } from './machine-id';
import { internalExceptions } from './errors';

export type BaseEvent = {
  event: string,
  [key: string]: any,
};

export type Event = BaseEvent & {
  id: string,
  clientTimestamp: string,
  anonymousId: string,
  platform: string,
  nodeVersion: string,
};

let flushPromise: Promise<any>|null = null;
let trackEvents: Array<Event> = [];

async function flush(toFlush?: Array<Event>, retries: number = 10): Promise<any> {
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
      internalExceptions(
        new Error(`Probably an unexpected request caused a bad response: ${result.status}`)
      );

      // eslint-disable-next-line consistent-return
      return flush(toFlush, retries - 1);
    }

    // console.log(await result.json());
  } catch (e) {
    if (retries > 0) {
      // eslint-disable-next-line consistent-return
      return flush(toFlush, retries - 1);
    }

    internalExceptions(e);
  }
}

let anonymousId: string = 'unknown';

try {
  anonymousId = machineIdSync();
} catch (e) {
  internalExceptions(e);
}

export function getAnonymousId() {
  return anonymousId;
}

export async function track(opts: BaseEvent) {
  // fixes the issue with async tests
  // the promise returned from this function can be executed after the test has finished
  if (process.env.CI) {
    return Promise.resolve();
  }

  trackEvents.push({
    ...opts,
    id: crypto.randomBytes(16).toString('hex'),
    clientTimestamp: new Date().toJSON(),
    platform: process.platform,
    nodeVersion: process.version,
    anonymousId,
  });

  const currentPromise = (flushPromise || Promise.resolve()).then(() => flush()).then(() => {
    if (currentPromise === flushPromise) {
      flushPromise = null;
    }
  });

  flushPromise = currentPromise;
  return flushPromise;
}
