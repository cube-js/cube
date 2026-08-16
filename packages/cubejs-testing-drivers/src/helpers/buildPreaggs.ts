import * as http from 'http';
import { clearInterval } from 'timers';
import { CubejsServerCoreExposed } from '../types/CubejsServerCoreExposed';

export async function postRequest(
  port: number,
  path: string,
  tkn: string,
  data: unknown,
): Promise<http.IncomingMessage> {
  return new Promise(
    (resolve: (res: http.IncomingMessage) => void, reject) => {
      const options = {
        hostname: 'localhost',
        port,
        path,
        method: 'POST',
        headers: {
          authorization: tkn,
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(JSON.stringify(data)),
        },
      };
      const req = http.request(options, (res) => {
        resolve(res);
      });
      req.on('error', (e) => {
        reject(`Problem with request: ${e.message}.`);
      });
      req.write(JSON.stringify(data));
      req.end();
    },
  );
}

export async function readData(
  res: http.IncomingMessage,
): Promise<Buffer> {
  return new Promise((resolve) => {
    let buffer = Buffer.from([]);
    res.on('data', (chunk: Buffer) => {
      buffer = Buffer.concat([buffer, Buffer.from(chunk)]);
    });
    res.on('end', () => {
      resolve(buffer);
    });
  });
}

export async function buildPreaggs(
  port: number,
  token: string,
  selector: any,
) {
  return new Promise((resolve, reject) => {
    postRequest(
      port,
      '/cubejs-api/v1/pre-aggregations/jobs',
      token,
      { action: 'post', selector },
    ).then((post) => readData(post)).then((_body) => {
      const body = _body.toString();
      let jobs: string[];
      // Nothing downstream settles the outer promise, and the 120s backstop below
      // is only armed once there are tokens to poll, so a body that is not JSON at
      // all - a proxy error page, or an empty response from a cube that died during
      // startup - would otherwise hang the build until jest's own timeout without
      // ever printing the response.
      try {
        jobs = JSON.parse(body);
      } catch (e) {
        reject(`Cube pre-aggregations build failed, response was not JSON: ${body}`);
        return;
      }
      // A rejected selector (e.g. naming a pre-aggregation the fixture does not
      // declare) answers with an error object rather than a token array. Without
      // this check that object is forwarded as `tokens` on the next request and
      // the build dies 120s later on an unrelated complaint about `tokens`, hiding
      // the selector that actually failed.
      if (!Array.isArray(jobs)) {
        reject(`Cube pre-aggregations build failed: ${body}`);
        return;
      }
      if (jobs.length === 0) {
        resolve(true);
        return;
      }
      let timeout: NodeJS.Timeout;
      // Every path that settles the outer promise goes through this, so neither the
      // poll nor the backstop outlives the result.
      let stop: () => void;
      const interval = setInterval(async () => {
        const inProcess = [];
        let statusBody = '';
        let statuses: any;
        // Same reasoning as the parse above, one round later: a throw in here is an
        // unhandled rejection inside the interval callback, so the build would go on
        // polling and only die on the 120s backstop below with `timeout.` as its
        // whole explanation.
        try {
          const get = await postRequest(
            port,
            '/cubejs-api/v1/pre-aggregations/jobs',
            token,
            { action: 'get', resType: 'object', tokens: jobs },
          );
          statusBody = (await readData(get)).toString();
          statuses = JSON.parse(statusBody);
        } catch (e) {
          stop();
          reject(`Cube pre-aggregations build failed: ${statusBody || e}`);
          return;
        }
        // An error response ({"error": "..."}) has no per-token job objects, so
        // reading `status` off it would throw an opaque TypeError from inside this
        // interval and leave the build to fail by timeout with the actual cause
        // never surfacing. Reject with the body instead.
        if (statuses.error) {
          stop();
          reject(`Cube pre-aggregations build failed: ${statusBody}`);
          return;
        }
        // Checked before the tally rather than inside it: a `failure` status is
        // neither `done` nor `missing_partition`, so rejecting from within the loop
        // left `inProcess` non-empty and both timers armed, and the helper went on
        // polling for the remaining ~120s after the promise had already settled -
        // interleaving its noise with the output someone is reading to find out why
        // the build failed.
        const failed = Object.keys(statuses).find(
          (t: string) => statuses[t].status.indexOf('failure') >= 0
        );
        if (failed) {
          stop();
          reject(`Cube pre-aggregations build failed: ${statuses[failed].status}`);
          return;
        }
        Object.keys(statuses).forEach((t: string) => {
          const { status } = statuses[t];
          if (status !== 'done' && status !== 'missing_partition') {
            inProcess.push(t);
          }
        });
        if (inProcess.length === 0) {
          stop();
          resolve(true);
        }
      }, 1000);

      stop = () => {
        clearInterval(interval);
        clearTimeout(timeout);
      };
      timeout = setTimeout(() => {
        clearInterval(interval);
        reject('Cube pre-aggregations build failed: timeout.');
      }, 120000);
    }).catch(reject);
  });
}

export async function hookPreaggs(
  core: CubejsServerCoreExposed,
  preagg: string,
) {
  const tokens: string[] = await core
    .getRefreshScheduler()
    .postBuildJobs(
      {
        authInfo: { tenantId: 'tenant1' },
        securityContext: { tenantId: 'tenant1' },
        requestId: 'XXX',
      },
      {
        timezones: ['UTC'],
        preAggregations: [{ id: preagg }],
        throwErrors: false,
      }
    );

  return new Promise((resolve, reject) => {
    const interval = setInterval(async () => {
      const inProcess = [];
      const selectors: {
        token: string,
        table: string,
        status: string,
        selector: any,
      }[] = await core
        .apiGateway()
        .preAggregationsJobsGET(
          {
            authInfo: { tenantId: 'tenant1' },
            securityContext: { tenantId: 'tenant1' },
            requestId: 'XXX',
          },
          tokens,
        );
  
      selectors.forEach((info) => {
        const { status } = info;
        if (status.indexOf('failure') >= 0) {
          reject(`Cube pre-aggregations build failed: ${status}`);
        }
        if (status !== 'done' && status !== 'missing_partition') {
          inProcess.push(info);
        }
        if (inProcess.length === 0) {
          clearInterval(interval);
          resolve(true);
        }
      });
    }, 1000);

    setTimeout(() => {
      clearInterval(interval);
      reject('Cube pre-aggregations build failed: timeout.');
    }, 60000);
  });
}
