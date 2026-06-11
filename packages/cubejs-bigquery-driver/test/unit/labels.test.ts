/**
 * Unit tests for BigQuery job label support via the queryMetadata hook.
 *
 * Labels attached to BigQuery jobs surface in INFORMATION_SCHEMA.JOBS, enabling
 * cost attribution by tenant/team and per-request trace correlation via the
 * x-request-id / traceparent header (which Cube uses verbatim as requestId and
 * passes into the queryMetadata hook as a context field).
 *
 * The @google-cloud/bigquery client is mocked so no GCP credentials are required.
 */

import { BigQueryDriver } from '../../src/BigQueryDriver';

// ── Mock @google-cloud/bigquery ──────────────────────────────────────────────

let capturedJobQuery: Record<string, any> | null = null;

const mockJob = {
  getMetadata: jest.fn().mockResolvedValue([
    { status: { state: 'DONE' } },
  ]),
  getQueryResults: jest.fn().mockResolvedValue([[{ result: 1 }]]),
};

const mockCreateQueryJob = jest.fn(async (q: Record<string, any>) => {
  capturedJobQuery = q;
  return [mockJob];
});

jest.mock('@google-cloud/bigquery', () => ({
  BigQuery: jest.fn().mockImplementation(() => ({
    createQueryJob: mockCreateQueryJob,
    createQueryStream: jest.fn((q: Record<string, any>) => {
      const { Readable } = require('stream');
      return new Readable({ read() { this.push(null); } });
    }),
    getDatasets: jest.fn().mockResolvedValue([[]]),
  })),
}));

jest.mock('@google-cloud/storage', () => ({
  Storage: jest.fn().mockImplementation(() => ({})),
}));

// ── Helpers ───────────────────────────────────────────────────────────────────

function makeDriver(opts: Record<string, any> = {}): BigQueryDriver {
  return new BigQueryDriver({
    projectId: 'test-project',
    ...opts,
  } as any);
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// Silence the pre-existing CUBEJS_DB_BQ_EXPORT_BUCKET deprecation warning that
// fires whenever BigQueryDriver is constructed (unrelated to this feature).
beforeAll(() => {
  jest.spyOn(console, 'warn').mockImplementation(() => {});
});

afterAll(() => {
  jest.restoreAllMocks();
});

beforeEach(() => {
  capturedJobQuery = null;
  mockCreateQueryJob.mockClear();
});

describe('BigQueryDriver job labels via queryMetadata', () => {
  describe('attaching queryMetadata as labels', () => {
    it('attaches queryMetadata entries as BigQuery job labels', async () => {
      const driver = makeDriver();
      await driver.query('SELECT 1', [], {
        queryMetadata: { request_id: 'trace-abc-123', tenant: 'acme' },
      } as any);
      expect(capturedJobQuery?.labels).toEqual({ request_id: 'trace-abc-123', tenant: 'acme' });
    });

    it('omits labels when queryMetadata is undefined', async () => {
      const driver = makeDriver();
      await driver.query('SELECT 1', [], {} as any);
      expect(capturedJobQuery?.labels).toBeUndefined();
    });

    it('omits labels when queryMetadata is empty', async () => {
      const driver = makeDriver();
      await driver.query('SELECT 1', [], { queryMetadata: {} } as any);
      expect(capturedJobQuery?.labels).toBeUndefined();
    });

    it('omits labels when no options are provided', async () => {
      const driver = makeDriver();
      await driver.query('SELECT 1', []);
      expect(capturedJobQuery?.labels).toBeUndefined();
    });
  });

  describe('label value sanitization', () => {
    it('lowercases uppercase characters', async () => {
      const driver = makeDriver();
      await driver.query('SELECT 1', [], {
        queryMetadata: { request_id: 'ABC-123' },
      } as any);
      expect(capturedJobQuery?.labels?.request_id).toBe('abc-123');
    });

    it('replaces illegal characters with underscores', async () => {
      const driver = makeDriver();
      await driver.query('SELECT 1', [], {
        queryMetadata: { request_id: 'span:1/req.2' },
      } as any);
      expect(capturedJobQuery?.labels?.request_id).toBe('span_1_req_2');
    });

    it('truncates values longer than 63 characters', async () => {
      const driver = makeDriver();
      const longId = 'a'.repeat(100);
      await driver.query('SELECT 1', [], {
        queryMetadata: { request_id: longId },
      } as any);
      expect(capturedJobQuery?.labels?.request_id).toHaveLength(63);
    });

    it('sanitizes all metadata values', async () => {
      const driver = makeDriver();
      await driver.query('SELECT 1', [], {
        queryMetadata: { team: 'Analytics Team', env: 'PROD' },
      } as any);
      expect(capturedJobQuery?.labels).toEqual({ team: 'analytics_team', env: 'prod' });
    });
  });
});
