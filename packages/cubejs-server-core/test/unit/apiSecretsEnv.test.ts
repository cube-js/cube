import { parseApiSecretsEnv } from '../../src/core/apiSecretsEnv';

describe('parseApiSecretsEnv', () => {
  it('returns undefined for unset env', () => {
    expect(parseApiSecretsEnv(undefined)).toBeUndefined();
  });

  it('returns undefined for empty string', () => {
    expect(parseApiSecretsEnv('')).toBeUndefined();
  });

  it('returns undefined when all entries are blank', () => {
    expect(parseApiSecretsEnv(',  ,,')).toBeUndefined();
  });

  it('splits, trims, and filters empty entries', () => {
    expect(parseApiSecretsEnv(' a , b , c ')).toEqual(['a', 'b', 'c']);
  });

  it('drops duplicates while preserving first-seen order', () => {
    expect(parseApiSecretsEnv('a,b,a,c,b')).toEqual(['a', 'b', 'c']);
  });

  it('handles a single secret', () => {
    expect(parseApiSecretsEnv('only')).toEqual(['only']);
  });
});
