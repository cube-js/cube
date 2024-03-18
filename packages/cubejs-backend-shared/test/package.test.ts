import { isFilePath, isSslKey } from '../src';

test('isFilePath', () => {
  expect(isFilePath('./file.path')).toBe(true);
  expect(isFilePath('../file.path')).toBe(true);
  expect(isFilePath(`-----BEGIN CERTIFICATE-----MIIDXDCCAkSgAwIBAgI-----END CERTIFICATE-----`)).toBe(false);
});

test('isSslKey', () => {
  expect(isSslKey(`-----BEGIN RSA PRIVATE KEY-----\nAbcDEF\n-----END RSA PRIVATE KEY-----`)).toBe(true);
  expect(isSslKey(`-----BEGIN EC PRIVATE KEY-----\nAbcDEF\n-----END EC PRIVATE KEY-----`)).toBe(true);
  expect(isSslKey(`-----BEGIN PRIVATE KEY-----\nAbcDEF\n-----END PRIVATE KEY-----`)).toBe(false);
  expect(isSslKey('./file.path')).toBe(false)
})