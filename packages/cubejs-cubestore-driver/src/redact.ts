const REDACTED = '<redacted>';

const SCHEME = /https?:\/\//gi;

// Same rules as `redact_url_credentials` in Cube Store, which a Cube Store older than this driver
// does not apply. A path may hold a space, so only a quote, a backslash, a line break or the next
// URL ends it. A query may hold a raw URL, so it ends at whitespace alone.
const SPAN_END = /["'`\\\r\n]/;

function findScheme(text: string, from: number): RegExpExecArray | null {
  SCHEME.lastIndex = from;
  return SCHEME.exec(text);
}

/**
 * Replaces the userinfo and the query string of every `http://` / `https://` URL in `text` with
 * `<redacted>`: presigned S3, GCS and Azure SAS URLs carry their credential and signature there.
 */
export function redactUrlCredentials(text: string): string {
  let out: string | null = null;
  let copied = 0;
  let scheme = findScheme(text, 0);

  while (scheme) {
    const authorityStart = scheme.index + scheme[0].length;
    const spanLength = text.slice(authorityStart).search(SPAN_END);
    const span = text.slice(authorityStart, spanLength === -1 ? text.length : authorityStart + spanLength);
    const nextUrl = findScheme(span, 1)?.index ?? -1;
    let queryStart = span.indexOf('?');
    if (nextUrl !== -1 && queryStart > nextUrl) {
      queryStart = -1;
    }

    let urlEnd = nextUrl === -1 ? span.length : nextUrl;
    if (queryStart !== -1) {
      const queryLength = span.slice(queryStart).search(/\s/);
      urlEnd = queryLength === -1 ? span.length : queryStart + queryLength;
    }
    const pos = authorityStart + urlEnd;

    const url = span.slice(0, urlEnd);
    const authorityLength = url.search(/[/?#\s]/);
    const at = url.slice(0, authorityLength === -1 ? url.length : authorityLength).lastIndexOf('@');

    if (at !== -1 || queryStart !== -1) {
      out = (out ?? '') + text.slice(copied, authorityStart);
      const pathStart = at + 1;
      if (at !== -1) {
        out += `${REDACTED}@`;
      }
      if (queryStart !== -1) {
        out += `${url.slice(pathStart, queryStart + 1)}${REDACTED}`;
        copied = pos;
      } else {
        copied = authorityStart + pathStart;
      }
    }

    scheme = findScheme(text, pos);
  }

  return out === null ? text : out + text.slice(copied);
}
