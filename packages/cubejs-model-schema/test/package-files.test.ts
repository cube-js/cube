import crypto from 'crypto';

import { packageFiles } from '../src/package-files';

describe('index.json', () => {
  it('names the manifest and the SHA-256 of the exact text written there', () => {
    const { manifest, manifestText, indexText } = packageFiles({ type: 'object' }, '1.2.3');
    const index = JSON.parse(indexText);

    expect(manifest).toBe('manifests/1.2.3.json');
    expect(index).toEqual({
      manifestFormatVersion: 1,
      latest: '1.2.3',
      manifests: { '1.2.3': 'manifests/1.2.3.json' },
      sha256: { '1.2.3': crypto.createHash('sha256').update(manifestText).digest('hex') },
    });
  });

  // Consumers update only when the hash changes, so a release that leaves the schema alone must
  // keep it.
  it('keeps the hash across versions when the schema is the same', () => {
    const hashOf = (schema: unknown, version: string) => JSON.parse(packageFiles(schema, version).indexText).sha256[version];

    expect(hashOf({ type: 'object' }, '1.2.3')).toBe(hashOf({ type: 'object' }, '1.2.4'));
    expect(hashOf({ type: 'object' }, '1.2.3')).not.toBe(hashOf({ type: 'string' }, '1.2.3'));
  });
});
