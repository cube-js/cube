import { fetch } from 'whatwg-fetch';
import cookie from 'js-cookie';
import { v4 as uuidv4 } from 'uuid';

let flushPromise = null;
let trackEvents: BaseEvent[] = [];
let baseProps = {
  sentFrom: 'frontend',
};
let telemetry: boolean | undefined;
let track:
  | null
  | ((event: Record<string, any>, telemetry?: boolean) => Promise<any>) = null;

export const setTelemetry = (isAllowed) => (telemetry = isAllowed);

export const trackImpl = async (event) => {
  if (telemetry !== true) {
    return;
  }

  let clientAnonymousId: string | null = localStorage.getItem(
    'playground_anonymous'
  );

  if (!clientAnonymousId) {
    clientAnonymousId = <string>(
      (cookie.get('playground_anonymous') || uuidv4().toString())
    );
    localStorage.setItem('playground_anonymous', clientAnonymousId);
    cookie.remove('playground_anonymous');
  }

  trackEvents.push({
    ...baseProps,
    ...event,
    id: uuidv4(),
    clientAnonymousId,
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

export const setTracker = (
  tracker: (props: Record<string, any>) => Promise<any>
) => (track = tracker);

export const setAnonymousId = (anonymousId, props) => {
  baseProps = {
    ...baseProps,
    ...props,
  };
  track?.({ event: 'identify', anonymousId, ...props }, telemetry);
};

type BaseEvent = Record<string, any>;

export const event = (name: string, params: BaseEvent = {}) => {
  track?.({ event: name, ...params }, telemetry);
};

export const playgroundAction = (name: string, options: BaseEvent = {}) => {
  event('Playground Action', { name, ...options });
};

export const page = (path) => {
  track?.({ event: 'page', ...path }, telemetry);
};
