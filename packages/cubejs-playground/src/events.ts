import { fetch } from 'whatwg-fetch';
import cookie from 'component-cookie';
import uuidv4 from 'uuid/v4';

let flushPromise = null;
let trackEvents: BaseEvent[] = [];
let baseProps = {
  sentFrom: 'frontend'
};
let telemetry: boolean | undefined;

export const setTelemetry = (isAllowed) => telemetry = isAllowed;

const track = async (event) => {
  if (telemetry !== true) {
    return;
  }

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

  const flush = async (toFlush?: BaseEvent[], retries: number = 10) => {
    if (!toFlush) {
      toFlush = trackEvents;
      trackEvents = [];
    }

    if (!toFlush.length) {
      return null;
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
    } catch (e) {
      if (retries > 0) {
        return flush(toFlush, retries - 1);
      }
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
  // @ts-ignore
  flushPromise = currentPromise;
  return flushPromise;
};

export const setAnonymousId = (anonymousId, props) => {
  baseProps = {
    ...baseProps,
    ...props
  };
  track({ event: 'identify', anonymousId, ...props });
};

type BaseEvent = Record<string, any>;

export const event = (name: string, params: BaseEvent = {}) => {
  track({ event: name, ...params });
};

export const playgroundAction = (name: string, options: BaseEvent = {}) => {
  event('Playground Action', { name, ...options });
};

export const page = (path) => {
  track({ event: 'page', ...path });
};
