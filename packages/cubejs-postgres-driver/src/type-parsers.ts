/** OID 1082 — Postgres emits `YYYY-MM-DD`. */
export const dateTypeParser = (val: string): string => `${val}T00:00:00.000`;

/** OID 1114 — `YYYY-MM-DD HH:mm:ss` or `YYYY-MM-DD HH:mm:ss.f{1,6}`, no TZ. */
export const timestampTypeParser = (val: string): string => {
  if (val.length === 19) {
    return `${val.slice(0, 10)}T${val.slice(11, 19)}.000`;
  }

  // val[19] is '.'; pad / truncate fractional digits to exactly 3.
  const ms = `${val.slice(20, 23)}00`.slice(0, 3);
  return `${val.slice(0, 10)}T${val.slice(11, 19)}.${ms}`;
};

// Hand-rolled zero-padders for the TIMESTAMPTZ hot path. `String(n).padStart`
// allocates an extra intermediate string per call; with six pad calls per value
// that measured ~15–20% slower in our microbenchmark than these range-checked
// template literals, so we keep the explicit versions.
const pad2 = (n: number): string => (n < 10 ? `0${n}` : `${n}`);
const pad3 = (n: number): string => {
  if (n < 10) return `00${n}`;
  if (n < 100) return `0${n}`;

  return `${n}`;
};
const pad4 = (n: number): string => {
  if (n < 1000) {
    if (n < 10) return `000${n}`;
    if (n < 100) return `00${n}`;

    return `0${n}`;
  }

  return `${n}`;
};

/**
 * OID 1184 — same as TIMESTAMP, suffixed with `(+|-)HH`, `(+|-)HH:MM`, or
 * `(+|-)HH:MM:SS`. We shift the value into UTC before formatting.
 */
export const timestampTzTypeParser = (val: string): string => {
  const len = val.length;

  // Timezone sign sits past the HH:MM:SS portion (index 19).
  let tzIdx = 19;
  for (; tzIdx < len; tzIdx++) {
    const c = val.charCodeAt(tzIdx);
    if (c === 43 /* + */ || c === 45 /* - */) break;
  }

  const sign = val.charCodeAt(tzIdx) === 43 ? 1 : -1;
  const tzHours = parseInt(val.slice(tzIdx + 1, tzIdx + 3), 10);
  let tzMinutes = 0;
  let tzSeconds = 0;

  if (len > tzIdx + 3) {
    tzMinutes = parseInt(val.slice(tzIdx + 4, tzIdx + 6), 10);
    if (len > tzIdx + 6) {
      tzSeconds = parseInt(val.slice(tzIdx + 7, tzIdx + 9), 10);
    }
  }

  const offsetMs = sign * (tzHours * 3600000 + tzMinutes * 60000 + tzSeconds * 1000);
  if (offsetMs === 0) {
    // Fast path: the driver pins session timezone to UTC by default, so Postgres emits `+00`,
    // `+00:00`, or `+00:00:00` for every TIMESTAMPTZ on the wire.
    return timestampTypeParser(val.slice(0, tzIdx));
  }

  const year = parseInt(val.slice(0, 4), 10);
  const month = parseInt(val.slice(5, 7), 10);
  const day = parseInt(val.slice(8, 10), 10);
  const hour = parseInt(val.slice(11, 13), 10);
  const minute = parseInt(val.slice(14, 16), 10);
  const second = parseInt(val.slice(17, 19), 10);

  let ms = 0;
  if (tzIdx > 19) {
    // val[19] is '.'; fractional digits run from index 20 up to tzIdx.
    ms = parseInt(`${val.slice(20, 23)}00`.slice(0, 3), 10);
  }

  // `Date.UTC(year, ...)` maps years 0-99 to 1900+year for legacy reasons,
  // which would corrupt pre-100 AD dates that Postgres can emit.
  let utc: Date;

  if (year >= 100) {
    utc = new Date(Date.UTC(year, month - 1, day, hour, minute, second, ms) - offsetMs);
  } else {
    utc = new Date(0);
    utc.setUTCFullYear(year, month - 1, day);
    utc.setUTCHours(hour, minute, second, ms);

    if (offsetMs !== 0) {
      utc.setTime(utc.getTime() - offsetMs);
    }
  }

  const yyyy = pad4(utc.getUTCFullYear());
  const MM = pad2(utc.getUTCMonth() + 1);
  const dd = pad2(utc.getUTCDate());
  const HH = pad2(utc.getUTCHours());
  const mm = pad2(utc.getUTCMinutes());
  const ss = pad2(utc.getUTCSeconds());
  const sss = pad3(utc.getUTCMilliseconds());

  return `${yyyy}-${MM}-${dd}T${HH}:${mm}:${ss}.${sss}`;
};
