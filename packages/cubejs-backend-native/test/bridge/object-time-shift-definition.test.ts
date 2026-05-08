import {
  bridgeHarnessAvailable,
  fieldNames,
  listBridgeFields,
  parseBridge,
} from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

const EXPECTED_FIELDS = ['interval', 'name', 'sql', 'timeshift_type'];

describeBridge('bridge object: TimeShiftDefinition', () => {
  it('exposes the union of trait fields and static fields via the bridge meta', () => {
    expect(fieldNames(listBridgeFields('timeShiftDefinition'))).toEqual(
      EXPECTED_FIELDS
    );
  });

  it('reports js-name overrides for serde-renamed fields', () => {
    const meta = listBridgeFields('timeShiftDefinition');
    const tsType = meta.find((m) => m.name === 'timeshift_type');
    expect(tsType?.jsName).toBe('type');
    expect(tsType?.kind).toBe('static');
    expect(tsType?.optional).toBe(true);
  });

  it('parses a fully-populated fixture without error', () => {
    const fixture = {
      sql: () => 'date_trunc',
      interval: '1 day',
      type: 'prior',
      name: 'last_day',
    };
    expect(() => parseBridge('timeShiftDefinition', fixture)).not.toThrow();
  });

  it('parses a fixture with all optional fields omitted', () => {
    expect(() => parseBridge('timeShiftDefinition', {})).not.toThrow();
  });
});
