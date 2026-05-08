import {
  bridgeHarnessAvailable,
  fieldNames,
  listBridgeFields,
  parseBridge,
} from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

const EXPECTED_FIELDS: string[] = [];

describeBridge('bridge object: SecurityContext', () => {
  it('exposes the expected field set via the bridge meta', () => {
    expect(fieldNames(listBridgeFields('securityContext'))).toEqual(EXPECTED_FIELDS);
  });

  it('parses a fully-populated fixture without error', () => {
    expect(() => parseBridge('securityContext', {})).not.toThrow();
  });
});
