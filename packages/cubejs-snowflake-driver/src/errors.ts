const CREDENTIALS_HINT =
  'This most commonly means the Snowflake credentials are wrong, expired or ' +
  'revoked — verify the account, username and credentials (password / private ' +
  'key / OAuth token)';

/**
 * Detects the opaque errors snowflake-sdk throws from inside its own
 * error-formatting code (e.g. the OAuth `prepareError` path dereferencing a
 * missing `error_description`) instead of a real authentication-failure
 * message. These are `TypeError`s of the V8 undefined-property shape and carry
 * no actionable information on their own — the operator has no way to tell the
 * real cause is a bad Snowflake credential.
 *
 * Matched by shape rather than an exact string so it stays robust across
 * snowflake-sdk versions and the different fields the SDK dereferences. Because
 * the same shape can also come from an *unrelated* bug, we never discard the
 * original message on a match — we only *append* the credentials hint to it
 * (see `describeCause`), so a genuine null-deref keeps its pinpoint message.
 */
function isOpaqueSdkTypeError(cause: unknown): boolean {
  if (cause instanceof AggregateError) {
    return cause.errors.some((e: unknown) => isOpaqueSdkTypeError(e));
  }
  if (!(cause instanceof TypeError)) {
    return false;
  }
  // Newer V8: "Cannot read properties of undefined (reading 'replace')"
  // Older V8: "Cannot read property 'replace' of undefined"
  return /Cannot read properties of undefined \(reading '.*'\)/.test(cause.message) ||
    /Cannot read property '.*' of undefined/.test(cause.message);
}

/**
 * Extracts the most useful message from anything that can be thrown/rejected —
 * `Error`, `AggregateError` (flattened; falling back to the aggregate's own
 * message), a plain object carrying a `message`, or a string. Returns an empty
 * string when there is genuinely no usable text, so the caller can fall back to
 * the hint.
 *
 * This runs on the error path, so it must never throw itself: a hostile or
 * exotic `cause` (a cyclic `AggregateError`, a throwing `message` getter) is
 * swallowed into the empty-string fallback rather than masking the real
 * connection failure with a secondary error.
 */
function messageOf(cause: unknown): string {
  try {
    if (cause instanceof AggregateError) {
      const flattened = cause.errors
        .map((e: unknown) => messageOf(e))
        .filter((m) => m.length > 0)
        .join(', ');
      // Fall back to the aggregate's own message (e.g. "All endpoints failed").
      return flattened || (typeof cause.message === 'string' ? cause.message.trim() : '');
    }

    if (cause instanceof Error) {
      return typeof cause.message === 'string' ? cause.message.trim() : '';
    }

    if (typeof cause === 'string') {
      return cause.trim();
    }

    // Non-Error rejections/throws (e.g. snowflake-sdk's HTTP layer can reject
    // with a plain `{ message, code }` object) still carry a real message.
    if (cause && typeof cause === 'object' && 'message' in cause) {
      const { message } = cause as { message: unknown };
      if (typeof message === 'string') {
        return message.trim();
      }
    }
  } catch {
    // A throwing getter / exotic object — treat as "no usable message".
    return '';
  }

  return '';
}

function describeCause(cause: unknown): string {
  const message = messageOf(cause);

  // Opaque SDK internal TypeError: keep whatever message it has (in case it's
  // actually a real deref bug) but append the credentials hint, which is the
  // likely cause when the SDK crashes inside its own auth-error formatting.
  if (isOpaqueSdkTypeError(cause)) {
    if (!message) {
      return CREDENTIALS_HINT;
    }
    // Avoid a doubled period when the message already ends with punctuation.
    const separator = /[.!?]$/.test(message) ? ' ' : '. ';
    return `${message}${separator}${CREDENTIALS_HINT}`;
  }

  // Any real message is preserved verbatim; only fall back to the hint when
  // there is genuinely nothing usable to show.
  return message || CREDENTIALS_HINT;
}

export class SnowflakeError extends Error {
  public name = 'SnowflakeError';

  public constructor(message: string, options?: ErrorOptions) {
    super(message, options);
  }
}

/**
 * Wraps errors thrown by `snowflake-sdk` while establishing a connection into
 * an actionable message.
 *
 * The snowflake-sdk does not surface authentication/connection failures
 * uniformly. Some failure shapes crash *inside the SDK's own error-formatting
 * code* and throw an opaque `TypeError: Cannot read properties of undefined
 * (reading 'replace')` that gives the operator no hint that the problem is
 * with the Snowflake credentials (see CUB-1676 — reproducible on the OAuth
 * path when an error response lacks `error_description`, at snowflake-sdk
 * `authentication/authentication_util.js` `prepareError`). Even the SDK's
 * "clean" errors (e.g. `RequestFailedError`) are not clearly attributed to
 * Snowflake once they bubble up through the API.
 *
 * This attributes the failure to Snowflake, preserves the original message
 * whenever there is one (adding the credentials hint only for the opaque case
 * or when there is no message at all), and retains the original thrown value on
 * `cause` — inspectable in a debugger or by a cause-aware logger (note that a
 * bare `error.stack` does not include the cause's stack).
 */
export class ConnectionError extends SnowflakeError {
  public readonly name = 'ConnectionError';

  public constructor(cause: unknown) {
    super(`Unable to connect to Snowflake: ${describeCause(cause)}`, {
      // `Error`'s `cause` option accepts any value — retain the original
      // (including a non-Error `{ message, code }` object) so its diagnostic
      // fields are not lost.
      ...(cause != null ? { cause } : {}),
    });
  }
}
