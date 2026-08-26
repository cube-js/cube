import http from 'http';
import type { AddressInfo } from 'net';

import * as native from '../js';
import metaFixture from './meta';
import { FakeRowStream } from './response-fake';

const TEST_SQL =
  'SELECT order_date FROM KibanaSampleDataEcommerce ORDER BY order_date DESC LIMIT 100000;';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * How long the orchestrator holds a `/v1/cubesql` attempt before giving up and
 * reporting `Continue wait`, i.e. `continueWaitTimeout` (seconds in config,
 * milliseconds here so the test runs in a few seconds rather than a minute).
 */
const CONTINUE_WAIT_TIMEOUT_MS = 1500;

/**
 * Longer than any deadline in this file: the underlying query never completes
 * while a test is running, so every attempt ends either in a `Continue wait` or
 * in the client giving up first. That is the whole point — this reproduces the
 * shape of a query that outlives its caller, not one that races to finish.
 */
const QUERY_DURATION_MS = 60_000;

type Harness = {
  url: string;
  loadEvents: string[];
  close: () => Promise<void>;
};

/**
 * A `/v1/cubesql` endpoint over a real socket.
 *
 * The handler mirrors `ApiGateway`'s route (`packages/cubejs-api-gateway/src/
 * gateway.ts`): the same two headers, and `execSql` handed the Express/Node
 * response object itself. That matters — the native side learns the client is
 * gone only through `res`'s `close` event, so a synthetic `Writable` cannot
 * stand in for it here.
 *
 * `sqlApiLoad` stands in for the orchestrator: it blocks for at most
 * `continueWaitTimeoutMs` and then reports `Continue wait`, which is what
 * `QueryCache`/`QueryQueue` do when a query is not ready within
 * `continueWaitTimeout`. With `throwContinueWait` the native side turns the
 * first such answer into a `Continue wait` for the client; without it, it loops
 * and calls back in, holding the connection open across windows.
 */
async function startCubeSqlServer({
  throwContinueWait,
  continueWaitTimeoutMs = CONTINUE_WAIT_TIMEOUT_MS,
  queryDurationMs = QUERY_DURATION_MS,
}: {
  throwContinueWait?: boolean;
  continueWaitTimeoutMs?: number;
  queryDurationMs?: number;
}): Promise<Harness> {
  const loadEvents: string[] = [];
  // Per-request, so the `throwContinueWait` polling case measures each attempt
  // against its own start rather than the first one's.
  const attemptStartedAt = new WeakMap<object, number>();

  const instance = await native.registerInterface(<any>{
    checkSqlAuth: async () => ({
      password: null,
      superuser: true,
      securityContext: {},
    }),
    checkAuth: async () => ({}),
    contextToApiScopes: async () => ['data', 'meta'],
    meta: async () => metaFixture,
    sql: async () => ({ error: 'not used by this test' }),
    stream: async ({ query }: any) => ({ stream: new FakeRowStream(query) }),
    sqlGenerators: async () => ({
      cubeNameToDataSource: {},
      memberToDataSource: {},
      dataSourceToSqlGenerator: {},
    }),
    sqlApiLoad: async ({ request, streaming, query }: any) => {
      const key = request ?? {};
      if (!attemptStartedAt.has(key)) {
        attemptStartedAt.set(key, Date.now());
      }
      const elapsed = Date.now() - (attemptStartedAt.get(key) as number);

      if (elapsed < queryDurationMs) {
        // The orchestrator waits for the query, up to `continueWaitTimeout`.
        await sleep(Math.min(continueWaitTimeoutMs, queryDurationMs - elapsed));

        if (Date.now() - (attemptStartedAt.get(key) as number) < queryDurationMs) {
          return { error: 'Continue wait' };
        }
      }

      if (streaming) {
        return { stream: new FakeRowStream(query) };
      }

      return {
        results: [
          {
            annotation: {
              measures: {},
              dimensions: {},
              segments: {},
              timeDimensions: {},
            },
            data: {
              members: ['KibanaSampleDataEcommerce.order_date'],
              columns: [['2024-01-01T00:00:00.000']],
            },
          },
        ],
      };
    },
    logLoadEvent: ({ event }: { event: string; properties: any }) => {
      loadEvents.push(event);
    },
    canSwitchUserForSession: () => true,
  });

  const server = http.createServer((req, res) => {
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Transfer-Encoding', 'chunked');

    native
      .execSql(
        instance,
        TEST_SQL,
        res,
        null,
        'stale-if-slow',
        undefined,
        throwContinueWait,
        `e2e-${Date.now()}`
      )
      .catch(() => {
        // `execSql` settles successfully even on a disconnect; a rejection here
        // would be a harness failure, and the assertions below would catch it
        // as a missing event rather than an unhandled rejection.
      });
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address() as AddressInfo;

  return {
    url: `http://127.0.0.1:${port}/cubejs-api/v1/cubesql`,
    loadEvents,
    close: async () => {
      await new Promise<void>((resolve) => {
        server.close(() => resolve());
      });
      await native.shutdownInterface(instance, 'fast');
    },
  };
}

type ClientResult = {
  aborted: boolean;
  body: string;
};

/**
 * Issues the request and destroys the socket after `deadlineMs`, the way a
 * caller with a fixed per-attempt timeout does. Resolves either when the
 * response ends on its own or when the deadline fires, whichever comes first.
 */
function requestWithDeadline(
  url: string,
  deadlineMs: number
): Promise<ClientResult> {
  return new Promise((resolve, reject) => {
    let body = '';
    let settled = false;
    let timer: NodeJS.Timeout;

    const finish = (result: ClientResult) => {
      if (!settled) {
        settled = true;
        clearTimeout(timer);
        resolve(result);
      }
    };

    const req = http.request(url, { method: 'POST' }, (res) => {
      res.setEncoding('utf-8');
      res.on('data', (chunk) => {
        body += chunk;
      });
      res.on('end', () => finish({ aborted: false, body }));
      res.on('error', () => finish({ aborted: true, body }));
    });

    timer = setTimeout(() => {
      req.destroy();
      finish({ aborted: true, body });
    }, deadlineMs);

    req.on('error', (err: any) => {
      if (settled) {
        return;
      }
      if (err?.code === 'ECONNRESET' || err?.code === 'ECONNABORTED') {
        finish({ aborted: true, body });
        return;
      }
      clearTimeout(timer);
      reject(err);
    });

    req.end();
  });
}

// Waiting on a load event rather than a fixed sleep: the native side logs it
// from a Tokio task after the socket is already gone, so it can land a beat
// after the client has resolved.
async function waitForLoadEvent(
  loadEvents: string[],
  event: string,
  timeoutMs = 10_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (loadEvents.includes(event)) {
      return;
    }
    await sleep(25);
  }
  throw new Error(
    `Timed out waiting for the '${event}' load event; saw: ${JSON.stringify(
      loadEvents
    )}`
  );
}

describe('/v1/cubesql continue wait vs. client deadline', () => {
  jest.setTimeout(60 * 1000);

  // CUB-4099. Reproduces the production configuration: `continueWaitTimeout` is
  // 60s while the caller gives up after 30s, so Cube never reaches the point
  // where it would answer `Continue wait` and every attempt dies as a
  // disconnect instead. Same ordering here, two decimal orders faster.
  test.each([
    ['throwContinueWait off', undefined],
    ['throwContinueWait on', true],
  ])(
    'a deadline shorter than continueWaitTimeout ends the attempt as a disconnect, not an error (%s)',
    async (_name, throwContinueWait) => {
      const harness = await startCubeSqlServer({ throwContinueWait });

      try {
        const clientDeadlineMs = Math.floor(CONTINUE_WAIT_TIMEOUT_MS / 3);
        const result = await requestWithDeadline(
          harness.url,
          clientDeadlineMs
        );

        // The client gave up first: nothing was delivered, so it cannot have
        // been told why. Whatever the attempt is recorded as, the caller does
        // not see it.
        expect(result.aborted).toBe(true);
        expect(result.body).toEqual('');

        // The attempt produced no result, so it has to be closed out as a
        // `Continue wait` — the event query history reads for exactly that.
        await waitForLoadEvent(harness.loadEvents, 'Continue wait');

        expect(harness.loadEvents).toContain('Load Request');
        // The regression this guards: a disconnect used to be reported as
        // `Cube SQL Error` with the message `Client disconnected`, which feeds
        // error rates and shows the request as failed in query history.
        expect(harness.loadEvents).not.toContain('Cube SQL Error');
        // Keeps the test honest — it would pass vacuously if the query had
        // simply finished before the deadline.
        expect(harness.loadEvents).not.toContain('Load Request Success');
      } finally {
        await harness.close();
      }
    }
  );

  // The control, and the actual fix for CUB-4099's cause: give the caller a
  // deadline longer than `continueWaitTimeout` and the disconnect stops
  // happening altogether, because Cube answers first. This is what the
  // Genentech deployment cannot do at `continueWaitTimeout: 60` against a 30s
  // caller deadline.
  it('a deadline longer than continueWaitTimeout gets Continue wait over the wire instead', async () => {
    const harness = await startCubeSqlServer({ throwContinueWait: true });

    try {
      const result = await requestWithDeadline(
        harness.url,
        CONTINUE_WAIT_TIMEOUT_MS * 4
      );

      // The connection survived, and the client was told to come back.
      expect(result.aborted).toBe(false);
      const payloads = result.body
        .split('\n')
        .filter((line) => line.trim().length > 0)
        .map((line) => JSON.parse(line));
      expect(payloads).toContainEqual(
        expect.objectContaining({ error: 'Continue wait' })
      );

      expect(harness.loadEvents).not.toContain('Cube SQL Error');
    } finally {
      await harness.close();
    }
  });
});
