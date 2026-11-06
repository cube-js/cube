import { AthenaDriver } from '../../src/AthenaDriver';

describe('AthenaDriver readOnly', () => {
  it('defaults to readOnly', () => {
    expect(new AthenaDriver({ exportBucket: 's3://b' }).readOnly()).toBe(true);
    expect(new AthenaDriver({}).readOnly()).toBe(true);
  });

  it('respects an explicit readOnly: false', () => {
    expect(new AthenaDriver({ readOnly: false }).readOnly()).toBe(false);
  });
});
