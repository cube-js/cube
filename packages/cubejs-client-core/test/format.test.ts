import { describe, it, expect } from 'vitest';
import { formatValue } from '../src/format';

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
    expect(formatValue(12345, { type: 'number', format: 'id' })).toBe('12345');
    expect(formatValue('https://example.com', { type: 'string', format: 'link' })).toBe('https://example.com');
    expect(formatValue('https://example.com', { type: 'string', format: { type: 'link', label: 'Example' } })).toBe('https://example.com');
  });

  it('type-based fallback: number', () => {
    expect(formatValue(1234.56, { type: 'number' })).toBe('1,234.56');
  });

  it('type-based fallback: time with grain', () => {
    expect(formatValue('2024-03-15T00:00:00.000', { type: 'time', granularity: 'day' })).toBe('2024-03-15');
    expect(formatValue('2024-03-01T00:00:00.000', { type: 'time', granularity: 'month' })).toBe('2024-03');
    expect(formatValue('2024-01-01T00:00:00.000', { type: 'time', granularity: 'year' })).toBe('2024');
    expect(formatValue('2024-03-11T00:00:00.000', { type: 'time', granularity: 'week' })).toBe('2024-03-11');
    expect(formatValue('2024-03-01T00:00:00.000', { type: 'time', granularity: 'quarter' })).toBe('2024-Q1');
    expect(formatValue('2024-03-15T14:00:00.000', { type: 'time', granularity: 'hour' })).toBe('2024-03-15 14:00:00');
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
});
