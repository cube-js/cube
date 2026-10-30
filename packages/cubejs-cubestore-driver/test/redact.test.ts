import { redactUrlCredentials } from '../src/redact';

describe('redactUrlCredentials', () => {
  it.each([
    [
      'https://bucket.s3.eu-west-1.amazonaws.com/x/data.csv.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAEXAMPLE%2F20261029%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Security-Token=tokenexample&X-Amz-Signature=deadbeef',
      'https://bucket.s3.eu-west-1.amazonaws.com/x/data.csv.gz?<redacted>',
    ],
    [
      'https://bucket.s3.amazonaws.com/x/data.csv?AWSAccessKeyId=AKIAEXAMPLE&Expires=1761696000&Signature=deadbeef',
      'https://bucket.s3.amazonaws.com/x/data.csv?<redacted>',
    ],
    [
      'https://storage.googleapis.com/bucket/x/data.csv.gz?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=example%40project.iam.gserviceaccount.com&X-Goog-Signature=deadbeef',
      'https://storage.googleapis.com/bucket/x/data.csv.gz?<redacted>',
    ],
    [
      'https://account.blob.core.windows.net/container/x/data.csv.gz?sv=2022-11-02&sp=r&se=2026-10-30T00:00:00Z&skoid=skoidexample&sig=deadbeef%3D',
      'https://account.blob.core.windows.net/container/x/data.csv.gz?<redacted>',
    ],
    [
      'HTTPS://bucket.s3.amazonaws.com/x/data.csv?Signature=deadbeef',
      'HTTPS://bucket.s3.amazonaws.com/x/data.csv?<redacted>',
    ],
    [
      'http://user:secretexample@host.example.com:8080/x/data.csv?sig=deadbeef#part',
      'http://<redacted>@host.example.com:8080/x/data.csv?<redacted>',
    ],
    [
      'https://account.blob.core.windows.net/container/my file.csv?sig=deadbeef',
      'https://account.blob.core.windows.net/container/my file.csv?<redacted>',
    ],
  ])('redacts %s', (url, expected) => {
    expect(redactUrlCredentials(url)).toEqual(expected);
    expect(redactUrlCredentials(expected)).toEqual(expected);
  });

  it('redacts every URL in an error message', () => {
    expect(redactUrlCredentials(
      'Create table failed: Internal: error sending request for url ' +
      '(https://h1.example.com/a.csv?X-Amz-Signature=deadbeef)\n' +
      "LOCATION 'https://h2.example.com/b.csv?sig=deadbeef', 'stream://kafka/topic'"
    )).toEqual(
      'Create table failed: Internal: error sending request for url ' +
      '(https://h1.example.com/a.csv?<redacted>\n' +
      "LOCATION 'https://h2.example.com/b.csv?<redacted>', 'stream://kafka/topic'"
    );
  });

  it.each([
    [
      'https://a.example.com/x https://user:secretexample@b.example.com/y',
      'https://a.example.com/x https://<redacted>@b.example.com/y',
    ],
    [
      'https://a.example.com/x and https://b.example.com/y?sig=deadbeef done',
      'https://a.example.com/x and https://b.example.com/y?<redacted> done',
    ],
    [
      'https://a.example.com/x?redirect=https://user:secretexample@b.example.com/y&sig=deadbeef',
      'https://a.example.com/x?<redacted>',
    ],
    [
      'url (https://a.example.com/x?sig=deadbeef) after 3 retries',
      'url (https://a.example.com/x?<redacted> after 3 retries',
    ],
    [
      'url (https://a.example.com/x?<redacted>) after 3 retries',
      'url (https://a.example.com/x?<redacted> after 3 retries',
    ],
  ])('redacts each URL in %s on its own', (text, expected) => {
    expect(redactUrlCredentials(text)).toEqual(expected);
  });

  it.each([
    'stream://kafka/topic/0',
    'temp://upload-1.csv',
    's3://bucket/key.csv?versionId=1',
    '/data/tmp/file.csv.gz',
    'https://host.example.com/x/data.csv',
    'Internal: Unable to import from http location, status code: 403 Forbidden',
  ])('leaves %s unchanged', (text) => {
    expect(redactUrlCredentials(text)).toEqual(text);
  });
});
