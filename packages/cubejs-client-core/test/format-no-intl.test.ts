import { describe, it, expect, beforeAll, afterAll, vi } from 'vitest';

describe('formatValue without Intl', () => {
  const originalIntl = globalThis.Intl;

  beforeAll(() => {
    vi.resetModules();

    // @ts-expect-error — intentionally removing Intl to simulate environments where it is unavailable
    delete globalThis.Intl;
  });

  afterAll(() => {
    globalThis.Intl = originalIntl;
  });

  it('detectLocale falls back to en-US and formatting works', async () => {
    const { formatValue } = await import('../src/format');

    // number type uses the detected locale (should be en-US fallback)
    expect(formatValue(1234.56, { type: 'number' })).toBe('1,234.56');
  });

  it('currency formatting falls back to en-US locale definition', async () => {
    const { formatValue } = await import('../src/format');

    expect(formatValue(1234.56, { type: 'number', format: 'currency' })).toBe('$1,234.56');
  });

  it('percent formatting works without Intl', async () => {
    const { formatValue } = await import('../src/format');

    expect(formatValue(0.1234, { type: 'number', format: 'percent' })).toBe('12.34%');
  });

  it('time formatting works without Intl', async () => {
    const { formatValue } = await import('../src/format');

    expect(formatValue('2024-03-15T00:00:00.000', { type: 'time', granularity: 'day' })).toBe('2024-03-15');
  });

  it('null/undefined still return emptyPlaceholder', async () => {
    const { formatValue } = await import('../src/format');

    expect(formatValue(null, { type: 'number' })).toBe('∅');
    expect(formatValue(undefined, { type: 'number' })).toBe('∅');
  });

  // Known locale (de-DE) — pre-built d3 definition is used,
  // getCurrencySymbol falls back to the static currencySymbols map.
  it('known locale (de-DE) uses pre-built locale definition', async () => {
    const { formatValue } = await import('../src/format');

    expect(formatValue(1234.56, { type: 'number', format: 'number', locale: 'de-DE' })).toBe('1.234,56');
    expect(formatValue(1234.56, { type: 'number', format: 'currency', currency: 'EUR', locale: 'de-DE' })).toBe('€1.234,56');
    expect(formatValue(0.1234, { type: 'number', format: 'percent', locale: 'de-DE' })).toBe('12,34%');
  });

  // Unknown locale (sv-SE) — getD3NumericLocaleFromIntl throws,
  // falls back entirely to en-US.
  it('unknown locale (sv-SE) falls back to en-US', async () => {
    const { formatValue } = await import('../src/format');

    expect(formatValue(1234.56, { type: 'number', format: 'number', locale: 'sv-SE' })).toBe('1,234.56');
    expect(formatValue(1234.56, { type: 'number', format: 'currency', currency: 'USD', locale: 'sv-SE' })).toBe('$1,234.56');
  });
});
