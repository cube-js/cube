import {
  bridgeHarnessAvailable,
  fieldNames,
  listBridgeFields,
  parseBridge,
} from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

// Required-field guard. Adding a new field via #[nbridge(...)] in
// member_order_by.rs without extending this list will fail the meta check.
const EXPECTED_FIELDS = ['dir', 'sql'];

describeBridge('bridge object: MemberOrderBy', () => {
  it('exposes the expected field set via the bridge meta', () => {
    expect(fieldNames(listBridgeFields('memberOrderBy'))).toEqual(EXPECTED_FIELDS);
  });

  it('parses a fully-populated fixture without error', () => {
    const fixture = {
      sql: () => 'foo.bar',
      dir: 'asc',
    };
    expect(() => parseBridge('memberOrderBy', fixture)).not.toThrow();
  });

  it('rejects a fixture missing the required sql field', () => {
    expect(() => parseBridge('memberOrderBy', { dir: 'asc' })).toThrow(
      /Field sql is required/
    );
  });

  it('rejects a fixture missing the required dir field', () => {
    expect(() => parseBridge('memberOrderBy', { sql: () => 'x' })).toThrow(
      /Field dir is required/
    );
  });
});
