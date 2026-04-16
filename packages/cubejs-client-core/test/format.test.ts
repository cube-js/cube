import { describe, it, expect } from 'vitest';
import { formatValue, formatDateByGranularity, getFormat } from '../src/format';

describe('formatValue', () => {
  it('format null', () => {
    expect(formatValue(null, { type: 'number' })).toBe('∅');
    expect(formatValue(undefined, { type: 'number' })).toBe('∅');
  });

  it('format: currency (defaults to USD)', () => {
    expect(formatValue(0, { type: 'number', format: 'currency' })).toBe('$0.00');
    expect(formatValue(-42.5, { type: 'number', format: 'currency' })).toBe('−$42.50');
    expect(formatValue('1234.56', { type: 'number', format: 'currency' })).toBe('$1,234.56');
    expect(formatValue(1234.56, { type: 'number', format: 'currency' })).toBe('$1,234.56');
  });

  it('format: currency with currency code', () => {
    expect(formatValue(1234.56, { type: 'number', format: 'currency', currency: 'EUR' })).toBe('€1,234.56');
    expect(formatValue(1234.56, { type: 'number', format: 'currency', currency: 'GBP' })).toBe('£1,234.56');
    expect(formatValue(1234.56, { type: 'number', format: 'currency', currency: 'JPY' })).toBe('¥1,234.56');
    expect(formatValue(1234.56, { type: 'number', format: 'currency', currency: 'USD' })).toBe('$1,234.56');
  });

  it('format: percent', () => {
    expect(formatValue(0.1234, { type: 'number', format: 'percent' })).toBe('12.34%');
    expect(formatValue(0, { type: 'number', format: 'percent' })).toBe('0.00%');
    expect(formatValue(1, { type: 'number', format: 'percent' })).toBe('100.00%');
  });

  it('format: number', () => {
    expect(formatValue(1234567.89, { type: 'number', format: 'number' })).toBe('1,234,567.89');
    expect(formatValue(1234, { type: 'number', format: 'number' })).toBe('1,234.00');
    expect(formatValue('999.1', { type: 'number', format: 'number' })).toBe('999.10');
  });

  it('format: custom-numeric', () => {
    expect(formatValue(1234.5, { type: 'number', format: { type: 'custom-numeric', value: '.2f' } })).toBe('1234.50');
    expect(formatValue(1234, { type: 'number', format: { type: 'custom-numeric', value: '$,.2f' } })).toBe('$1,234.00');
    expect(formatValue(0.5, { type: 'number', format: { type: 'custom-numeric', value: '.0%' } })).toBe('50%');
    expect(formatValue(1500, { type: 'number', format: { type: 'custom-numeric', value: '.2s' } })).toBe('1.5k');
  });

  it('format: custom-time', () => {
    expect(formatValue('2024-03-15T10:30:00.000', { type: 'time', format: { type: 'custom-time', value: '%Y-%m-%d' } })).toBe('2024-03-15');
    expect(formatValue('2024-03-15T10:30:00.000', { type: 'time', format: { type: 'custom-time', value: '%H:%M' } })).toBe('10:30');
  });

  it('passthrough formats', () => {
    expect(formatValue('https://img.example.com/photo.png', { type: 'string', format: 'imageUrl' })).toBe('https://img.example.com/photo.png');
    expect(formatValue('https://example.com', { type: 'string', format: 'link' })).toBe('https://example.com');
    expect(formatValue('https://example.com', { type: 'string', format: { type: 'link', label: 'Example' } })).toBe('https://example.com');
  });

  it('format: id (integer, no thousands separator)', () => {
    expect(formatValue(12345, { type: 'number', format: 'id' })).toBe('12345');
    expect(formatValue('12345', { type: 'number', format: 'id' })).toBe('12345');
    expect(formatValue(12345.78, { type: 'number', format: 'id' })).toBe('12346');
    expect(formatValue(0, { type: 'number', format: 'id' })).toBe('0');
  });

  it('type-based fallback: number', () => {
    expect(formatValue(1234.56, { type: 'number' })).toBe('1,234.56');
  });

  it('type-based fallback: time with grain', () => {
    expect(formatValue('2024-03-15T00:00:00.000', { type: 'time', granularity: 'day' })).toBe('2024-03-15');
    expect(formatValue('2024-03-01T00:00:00.000', { type: 'time', granularity: 'month' })).toBe('2024 Mar');
    expect(formatValue('2024-01-01T00:00:00.000', { type: 'time', granularity: 'year' })).toBe('2024');
    expect(formatValue('2024-03-11T00:00:00.000', { type: 'time', granularity: 'week' })).toBe('2024-03-11 W11');
    expect(formatValue('2024-03-01T00:00:00.000', { type: 'time', granularity: 'quarter' })).toBe('2024-Q1');
    expect(formatValue('2024-03-15T14:00:00.000', { type: 'time', granularity: 'hour' })).toBe('2024-03-15 14:00:00');
    expect(formatValue('2024-03-15T14:30:00.000', { type: 'time', granularity: 'minute' })).toBe('2024-03-15 14:30:00');
    expect(formatValue('2024-03-15T14:30:45.000', { type: 'time' })).toBe('2024-03-15 14:30:45');
  });

  it('format with nl-NL locale', () => {
    const locale = 'nl-NL';
    expect(formatValue(1234.56, { type: 'number', format: 'currency', currency: 'EUR', locale })).toBe('€1.234,56');
    expect(formatValue(0, { type: 'number', format: 'currency', currency: 'EUR', locale })).toBe('€0,00');
    expect(formatValue(1234.56, { type: 'number', format: 'currency', currency: 'USD', locale })).toBe('US$1.234,56');
    expect(formatValue(1234.56, { type: 'number', format: 'number', locale })).toBe('1.234,56');
    expect(formatValue(1234.56, { type: 'number', locale })).toBe('1.234,56');
  });

  it('format with en-IN locale (non-uniform digit grouping)', () => {
    const locale = 'en-IN';
    expect(formatValue(1234567.89, { type: 'number', format: 'number', locale })).toBe('12,34,567.89');
    expect(formatValue(1234567.89, { type: 'number', format: 'currency', currency: 'INR', locale })).toBe('₹12,34,567.89');
    expect(formatValue(1234567.89, { type: 'number', locale })).toBe('12,34,567.89');
  });

  it('invalid date input returns Invalid date', () => {
    expect(formatValue('not-a-date', { type: 'time' })).toBe('Invalid date');
    expect(formatValue('not-a-date', { type: 'time', granularity: 'day' })).toBe('Invalid date');
    expect(formatValue('not-a-date', { type: 'time', format: { type: 'custom-time', value: '%Y-%m-%d' } })).toBe('Invalid date');
  });

  it('custom emptyPlaceholder', () => {
    expect(formatValue(null, { type: 'number', emptyPlaceholder: 'N/A' })).toBe('N/A');
    expect(formatValue(undefined, { type: 'time', emptyPlaceholder: '-' })).toBe('-');
  });

  it('default fallback', () => {
    expect(formatValue('hello', { type: 'string' })).toBe('hello');
    expect(formatValue(42, { type: 'number' })).toBe('42.00');
    expect(formatValue(true, { type: 'boolean' })).toBe('true');
    expect(formatValue('', { type: 'string' })).toBe('');
  });

  it('boolean: coerces numeric 0/1 from SQL drivers', () => {
    expect(formatValue(false, { type: 'boolean' })).toBe('false');
    expect(formatValue(1, { type: 'boolean' })).toBe('true');
    expect(formatValue(0, { type: 'boolean' })).toBe('false');
    expect(formatValue('true', { type: 'boolean' })).toBe('true');
    expect(formatValue('false', { type: 'boolean' })).toBe('false');
    expect(formatValue('1', { type: 'boolean' })).toBe('true');
    expect(formatValue('0', { type: 'boolean' })).toBe('false');
  });
});

describe('formatDateByGranularity', () => {
  it('formats each predefined granularity', () => {
    const iso = '2024-03-15T14:30:45.000';
    expect(formatDateByGranularity(iso, 'second')).toBe('2024-03-15 14:30:45');
    expect(formatDateByGranularity(iso, 'minute')).toBe('2024-03-15 14:30:45');
    expect(formatDateByGranularity(iso, 'hour')).toBe('2024-03-15 14:30:45');
    expect(formatDateByGranularity(iso, 'day')).toBe('2024-03-15');
    expect(formatDateByGranularity(iso, 'week')).toBe('2024-03-15 W11');
    expect(formatDateByGranularity(iso, 'month')).toBe('2024 Mar');
    expect(formatDateByGranularity(iso, 'quarter')).toBe('2024-Q1');
    expect(formatDateByGranularity(iso, 'year')).toBe('2024');
  });

  it('accepts Date, ISO string, and epoch-number inputs', () => {
    const date = new Date('2024-03-15T00:00:00.000');
    expect(formatDateByGranularity(date, 'day')).toBe('2024-03-15');
    expect(formatDateByGranularity(date.getTime(), 'day')).toBe('2024-03-15');
    expect(formatDateByGranularity('2024-03-15T00:00:00.000', 'day')).toBe('2024-03-15');
  });

  it('falls back to second-grain format for missing or unknown granularity', () => {
    expect(formatDateByGranularity('2024-03-15T14:30:45.000')).toBe('2024-03-15 14:30:45');
    expect(formatDateByGranularity('2024-03-15T14:30:45.000', 'decade' as any)).toBe('2024-03-15 14:30:45');
  });

  it('returns "Invalid date" on bad input', () => {
    expect(formatDateByGranularity('not-a-date', 'day')).toBe('Invalid date');
  });
});

describe('getFormat', () => {
  it('time dimension: returns d3 format string per granularity', () => {
    expect(getFormat({ type: 'time', granularity: 'day' }).formatString).toBe('%Y-%m-%d');
    expect(getFormat({ type: 'time', granularity: 'month' }).formatString).toBe('%Y %b');
    expect(getFormat({ type: 'time', granularity: 'year' }).formatString).toBe('%Y');
    expect(getFormat({ type: 'time', granularity: 'hour' }).formatString).toBe('%Y-%m-%d %H:%M:%S');
    expect(getFormat({ type: 'time' }).formatString).toBe('%Y-%m-%d %H:%M:%S');
  });
  
  it('time dimension: formatFunc delegates to formatDateByGranularity', () => {
    const { formatFunc } = getFormat({ type: 'time', granularity: 'month' });
    expect(formatFunc('2024-03-01T00:00:00.000')).toBe('2024 Mar');
  });

  it('number with currency format', () => {
    const { formatString, formatFunc } = getFormat({ type: 'number', format: 'currency' });
    expect(formatString).toBe('$,.2f');
    expect(formatFunc(1234.56)).toBe('$1,234.56');
    expect(formatFunc('1234.56')).toBe('$1,234.56');
  });

  it('number with percent format', () => {
    const { formatString, formatFunc } = getFormat({ type: 'number', format: 'percent' });
    expect(formatString).toBe('.2%');
    expect(formatFunc(0.1234)).toBe('12.34%');
  });

  it('number with no explicit format falls back to default number format', () => {
    const { formatString, formatFunc } = getFormat({ type: 'number' });
    expect(formatString).toBe(',.2f');
    expect(formatFunc(1234.56)).toBe('1,234.56');
  });

  it('custom-numeric format exposes the spec as formatString', () => {
    const { formatString, formatFunc } = getFormat({ type: 'number', format: { type: 'custom-numeric', value: '.2s' } });
    expect(formatString).toBe('.2s');
    expect(formatFunc(1500)).toBe('1.5k');
  });

  it('custom-time format exposes the spec as formatString', () => {
    const { formatString, formatFunc } = getFormat({ type: 'time', format: { type: 'custom-time', value: '%Y-%m-%d' } });
    expect(formatString).toBe('%Y-%m-%d');
    expect(formatFunc('2024-03-15T10:30:00.000')).toBe('2024-03-15');
  });

  it('string fallback returns identity formatFunc', () => {
    const { formatString, formatFunc } = getFormat({ type: 'string' });
    expect(formatString).toBeNull();
    expect(formatFunc('hello')).toBe('hello');
  });

  it('locale option is honored by formatFunc', () => {
    const { formatFunc } = getFormat({ type: 'number', format: 'currency', currency: 'EUR' }, { locale: 'nl-NL' });
    expect(formatFunc(1234.56)).toBe('€1.234,56');
  });
});
