import type { Column } from 'snowflake-sdk';
import { pad2, pad3, pad4 } from '@cubejs-backend/shared';

export type HydrationConfiguration = {
  types: string[], toValue: (column: Column) => ((value: any) => any) | null
};

export function formatUtcTimestamp(value: Date): string {
  const y = pad4(value.getUTCFullYear());
  const mo = pad2(value.getUTCMonth() + 1);
  const d = pad2(value.getUTCDate());
  const h = pad2(value.getUTCHours());
  const mi = pad2(value.getUTCMinutes());
  const s = pad2(value.getUTCSeconds());
  const ms = pad3(value.getUTCMilliseconds());
  return `${y}-${mo}-${d}T${h}:${mi}:${s}.${ms}`;
}

// It's not possible to declare own map converters by passing config to snowflake-sdk
export const hydrators: HydrationConfiguration[] = [
  {
    types: ['fixed', 'real'],
    toValue: (column) => {
      if (column.isNullable()) {
        return (value) => {
          // We use numbers as strings by fetchAsString
          if (value === 'NULL') {
            return null;
          }

          return value;
        };
      }

      // Nothing to fix, let's skip this field
      return null;
    },
  },
  {
    // The TIMESTAMP_* variation associated with TIMESTAMP, default to TIMESTAMP_NTZ.
    // DATE values arrive as Dates pinned to UTC midnight — same formatter, output
    // stays compatible with the prior formatToTimeZone behavior.
    types: [
      'date',
      // TIMESTAMP_LTZ internally stores UTC time with a specified precision.
      'timestamp_ltz',
      // TIMESTAMP_NTZ internally stores “wallclock” time with a specified precision.
      // All operations are performed without taking any time zone into account.
      'timestamp_ntz',
      // TIMESTAMP_TZ internally stores UTC time together with an associated time zone offset.
      // When a time zone is not provided, the session time zone offset is used.
      'timestamp_tz',
    ],
    toValue: () => (value) => (value ? formatUtcTimestamp(value) : null),
  },
  {
    types: ['object'], // Workaround for HLL_SNOWFLAKE
    toValue: () => (value) => {
      if (!value) {
        return null;
      }

      return JSON.stringify(value);
    },
  }
];
