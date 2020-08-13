import { fetch } from 'whatwg-fetch';
import cookie from 'component-cookie';
import uuidv4 from 'uuid/v4';

let flushPromise = null;
let trackEvents = [];
let baseProps = {};

const track = async (event) => {
  if (!cookie('playground_anonymous')) {
    cookie('playground_anonymous', uuidv4());
  }
  trackEvents.push({
    ...baseProps,
    ...event,
    id: uuidv4(),
    clientAnonymousId: cookie('playground_anonymous'),
    clientTimestamp: new Date().toJSON(),
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
        body: JSON.stringify(toFlush.map((r) => ({ ...r, sentAt }))),
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
  const currentPromise = (flushPromise || Promise.resolve())
    .then(() => flush())
    .then(() => {
      if (currentPromise === flushPromise) {
        flushPromise = null;
      }
    });
  flushPromise = currentPromise;
  return flushPromise;
};

export const setAnonymousId = (anonymousId, props) => {
  baseProps = props;
  track({ event: 'identify', anonymousId, ...props });
};

export const event = (name, params) => {
  track({ event: name, ...params });
};

export const playgroundAction = (name, options) => {
  event('Playground Action', { name, ...options });
};

export const page = (path) => {
  track({ event: 'page', ...path });
};
