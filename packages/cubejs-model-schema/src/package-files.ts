import crypto from 'crypto';

const MANIFEST_FORMAT_VERSION = 1;

export function packageFiles(schema: unknown, version: string) {
  const manifest = `manifests/${version}.json`;
  const manifestText = `${JSON.stringify(schema, null, 2)}\n`;
  const index = {
    manifestFormatVersion: MANIFEST_FORMAT_VERSION,
    latest: version,
    manifests: { [version]: manifest },
    // The manifest holds no version, so releases that leave the model language alone share a hash.
    sha256: { [version]: crypto.createHash('sha256').update(manifestText).digest('hex') },
  };
  return { manifest, manifestText, indexText: `${JSON.stringify(index, null, 2)}\n` };
}
