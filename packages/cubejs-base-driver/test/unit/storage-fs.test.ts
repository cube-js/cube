import { S3 } from '@aws-sdk/client-s3';
import { Storage } from '@google-cloud/storage';
import { DefaultAzureCredential } from '@azure/identity';

import { normalizeS3ClientConfig } from '../../src/storage-fs/aws.fs';
import { hasGCSCredentials } from '../../src/storage-fs/gcs.fs';

describe('storage-fs credential normalization', () => {
  describe('normalizeS3ClientConfig', () => {
    test('drops static credentials when both key and secret are blank', () => {
      const config = normalizeS3ClientConfig({
        credentials: { accessKeyId: '', secretAccessKey: '' },
        region: 'us-east-1',
      });

      expect(config.credentials).toBeUndefined();
      expect(config.region).toBe('us-east-1');
    });

    test('drops static credentials when only one of key/secret is set', () => {
      expect(
        normalizeS3ClientConfig({ credentials: { accessKeyId: 'AKIA', secretAccessKey: '' } }).credentials
      ).toBeUndefined();
      expect(
        normalizeS3ClientConfig({ credentials: { accessKeyId: '', secretAccessKey: 'secret' } }).credentials
      ).toBeUndefined();
    });

    test('keeps fully-populated static credentials', () => {
      const credentials = { accessKeyId: 'AKIA', secretAccessKey: 'secret' };
      expect(normalizeS3ClientConfig({ credentials }).credentials).toEqual(credentials);
    });

    test('keeps a credential provider function untouched', () => {
      // e.g. fromTemporaryCredentials / fromWebToken — a function, resolved lazily.
      const provider = async () => ({ accessKeyId: 'AKIA', secretAccessKey: 'secret' });
      expect(normalizeS3ClientConfig({ credentials: provider }).credentials).toBe(provider);
    });

    test('drops a blank region', () => {
      expect(normalizeS3ClientConfig({ region: '' }).region).toBeUndefined();
      expect(normalizeS3ClientConfig({ region: '   ' }).region).toBeUndefined();
      expect(normalizeS3ClientConfig({ region: 'eu-west-1' }).region).toBe('eu-west-1');
    });

    test('does not mutate the input config', () => {
      const input = { credentials: { accessKeyId: '', secretAccessKey: '' }, region: '' };
      normalizeS3ClientConfig(input);
      expect(input.credentials).toEqual({ accessKeyId: '', secretAccessKey: '' });
      expect(input.region).toBe('');
    });

    // This is the actual reproduction of the Freshworks / CUB-3000 failure mode:
    // the AWS SDK must accept a config with no credentials so it can resolve the
    // default provider chain (web identity token file) instead of throwing.
    test('the S3 client constructs without throwing when credentials are omitted', () => {
      expect(() => new S3(normalizeS3ClientConfig({
        credentials: { accessKeyId: '', secretAccessKey: '' },
        region: 'us-east-1',
      }))).not.toThrow();
    });
  });

  describe('hasGCSCredentials', () => {
    test('treats empty/undefined/empty-object as absent', () => {
      expect(hasGCSCredentials(undefined)).toBe(false);
      expect(hasGCSCredentials('')).toBe(false);
      expect(hasGCSCredentials({})).toBe(false);
    });

    test('treats a non-empty credentials object as present', () => {
      expect(hasGCSCredentials({ project_id: 'p', client_email: 'e' })).toBe(true);
    });

    test('the GCS client constructs without throwing when credentials are absent', () => {
      // `new Storage(undefined)` is what extractFilesFromGCS does when no
      // credentials are configured; it must fall back to ADC, not throw.
      expect(() => new Storage(undefined)).not.toThrow();
    });
  });

  describe('azure DefaultAzureCredential', () => {
    test('constructs without throwing when no static key/secret is provided', () => {
      // extractFilesFromAzure falls through to DefaultAzureCredential when only
      // clientId/tenantId (or nothing) are provided; it must construct so the
      // federated token file (AZURE_FEDERATED_TOKEN_FILE) can be resolved.
      // Options are built as a variable to mirror extractFilesFromAzure (and to
      // pass clientId/tenantId, which the SDK reads at runtime).
      const opts = { clientId: 'c', tenantId: 't', tokenFilePath: undefined };
      expect(() => new DefaultAzureCredential({})).not.toThrow();
      expect(() => new DefaultAzureCredential(opts)).not.toThrow();
    });
  });
});
