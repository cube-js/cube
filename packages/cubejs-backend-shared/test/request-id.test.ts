import { isValidRequestId } from '../src';

describe('isValidRequestId', () => {
  it('accepts base64 and . _ : -, including traceparent', () => {
    expect(isValidRequestId('5f1c2c1e-0b8b-4f5e-9f4a-9a1c2b3d4e5f-span-1')).toBe(true);
    expect(isValidRequestId('00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01')).toBe(true);
    expect(isValidRequestId('My_Req.1:2')).toBe(true);
    expect(isValidRequestId('aGVsbG8+d29ybGQ/IQ==')).toBe(true);
  });

  it('rejects spaces, control and non-ASCII characters', () => {
    expect(isValidRequestId('a b')).toBe(false);
    expect(isValidRequestId('a\nb')).toBe(false);
    expect(isValidRequestId('a\tb')).toBe(false);
    expect(isValidRequestId('café')).toBe(false);
    expect(isValidRequestId('中')).toBe(false);
  });

  it('rejects quotes and other SQL metacharacters', () => {
    for (const c of ['\'', '"', '`', '\\', ';', '*', '(', ')', '?', '$', '%', '#']) {
      expect(isValidRequestId(`req${c}1`)).toBe(false);
    }
  });

  it('rejects empty and overly long ids', () => {
    expect(isValidRequestId('')).toBe(false);
    expect(isValidRequestId('a'.repeat(128))).toBe(true);
    expect(isValidRequestId('a'.repeat(129))).toBe(false);
  });
});
