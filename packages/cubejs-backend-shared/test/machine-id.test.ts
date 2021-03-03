import { machineId, machineIdSync } from '../src';

const originalPattern: Record<string, RegExp> = {
  darwin: /^[0-9,A-z]{8}-[0-9,A-z]{4}-[0-9,A-z]{4}-[0-9,A-z]{4}-[0-9,A-z]{12}$/,
  win32: /^[0-9,A-z]{8}-[0-9,A-z]{4}-[0-9,A-z]{4}-[0-9,A-z]{4}-[0-9,A-z]{12}$/,
  linux: /^[0-9,A-z]{32}$/,
  freebsd: /^[0-9,A-z]{8}-[0-9,A-z]{4}-[0-9,A-z]{4}-[0-9,A-z]{4}-[0-9,A-z]{12}$/
};

const hashPattern = /^[0-9,A-z]{64}$/;

function getOriginal() {
  if (process.platform in originalPattern) {
    return originalPattern[process.platform];
  }

  throw new Error('Unsupported platform');
}

describe('machineId (async)', () => {
  it('should return original unique id', async () => {
    expect(await machineId(true)).toMatch(getOriginal());
  });

  it('should return unique sha256-hash', async () => {
    expect(await machineId()).toMatch(hashPattern);
  });
});

describe('machineIdSync', () => {
  it('should return original unique id', async () => {
    expect(machineIdSync(true)).toMatch(getOriginal());
  });

  it('should return unique sha256-hash', async () => {
    expect(machineIdSync()).toMatch(hashPattern);
  });
});
