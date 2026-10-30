const REDACTED = '<redacted>';

// Same rules as `redact_url_credentials` in Cube Store, which a Cube Store older than this driver
// does not apply. Only quotes, a backslash or a line break end a URL, so a space in a path cannot
// leave the query after it unredacted.
const HTTP_URL = /(https?:\/\/)([^"'`\\\r\n]*)/gi;

/**
 * Replaces the userinfo and the query string of every `http://` / `https://` URL in `text` with
 * `<redacted>`: presigned S3, GCS and Azure SAS URLs carry their credential and signature there.
 */
export function redactUrlCredentials(text: string): string {
  return text.replace(HTTP_URL, (url: string, scheme: string, rest: string) => {
    const authorityEnd = rest.search(/[/?#\s]/);
    const authority = authorityEnd === -1 ? rest : rest.slice(0, authorityEnd);
    const at = authority.lastIndexOf('@');
    const queryStart = rest.indexOf('?');

    if (at === -1 && queryStart === -1) {
      return url;
    }

    const afterUserinfo = at === -1 ? rest : rest.slice(at + 1);
    const userinfo = at === -1 ? '' : `${REDACTED}@`;
    const query = afterUserinfo.indexOf('?');

    return query === -1
      ? `${scheme}${userinfo}${afterUserinfo}`
      : `${scheme}${userinfo}${afterUserinfo.slice(0, query + 1)}${REDACTED}`;
  });
}
