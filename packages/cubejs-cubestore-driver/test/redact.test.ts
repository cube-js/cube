import fs from 'fs';
import path from 'path';

import { redactUrlCredentials } from '../src/redact';

type Cases = {
  urls: [string, string][];
  text: [string, string][];
  unchanged: string[];
};

// The cases Cube Store's `redact_url_credentials` is tested against, so the two ports cannot drift.
const cases: Cases = JSON.parse(fs.readFileSync(
  path.resolve(__dirname, '../../../../rust/cubestore/cubestore/src/util/redact_cases.json'),
  'utf8'
));

describe('redactUrlCredentials', () => {
  it.each(cases.urls)('redacts %s', (url, expected) => {
    expect(redactUrlCredentials(url)).toEqual(expected);
    expect(redactUrlCredentials(expected)).toEqual(expected);
  });

  it.each(cases.text)('redacts each URL in %s', (text, expected) => {
    expect(redactUrlCredentials(text)).toEqual(expected);
  });

  it.each(cases.unchanged)('leaves %p unchanged', (text) => {
    expect(redactUrlCredentials(text)).toEqual(text);
  });
});
