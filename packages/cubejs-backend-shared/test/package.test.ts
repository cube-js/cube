import { isFilePath } from '../src';

test('isFilePath', () => {
  expect(isFilePath('./file.path')).toBe(true);
  expect(isFilePath('../file.path')).toBe(true);
  expect(isFilePath(`-----BEGIN CERTIFICATE-----MIIDXDCCAkSgAwIBAgI-----END CERTIFICATE-----`)).toBe(false);
});
