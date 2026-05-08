import {
  bridgeHarnessAvailable,
  fieldNames,
  listBridgeFields,
  parseBridge,
} from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

const EXPECTED_FIELDS: string[] = [];

describeBridge('bridge object: FilterParams', () => {
  it('exposes the expected field set via the bridge meta', () => {
    expect(fieldNames(listBridgeFields('filterParams'))).toEqual(EXPECTED_FIELDS);
  });

  it('parses a fully-populated fixture without error', () => {
    expect(() => parseBridge('filterParams', {})).not.toThrow();
  });
});
