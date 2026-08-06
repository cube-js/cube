import { MemberReference } from '../descriptors/MemberReference';
import { ValueWithComments } from '../descriptors/ValueWithComments';
import {
  SchemaDescriptor
} from '../ScaffoldingTemplate';
import { BaseSchemaFormatter } from './BaseSchemaFormatter';

/**
 * Plain scalars that a YAML loader resolves to something other than a string — null, a
 * boolean, a number or a timestamp — so a title or member name of this shape has to be
 * quoted to stay a string. Includes the YAML 1.1 integer forms loaders still accept
 * (binary, octal), since resolution is what matters here, not which spec version
 * nominally applies. `yes`/`on`/`off` are strings and stay unquoted.
 */
const YAML_TYPE_SHAPED = new RegExp(
  [
    '^(?:',
    '|~|null|Null|NULL|true|True|TRUE|false|False|FALSE',
    // Decimal, with an optional fraction and exponent.
    '|[-+]?(?:\\d[\\d_]*(?:\\.[\\d_]*)?|\\.[\\d_]+)(?:[eE][-+]?\\d+)?',
    // Hex, octal (both spellings) and binary.
    '|[-+]?0[xX][0-9a-fA-F_]+|[-+]?0o?[0-7_]+|[-+]?0[bB][01_]+',
    // Infinity and not-a-number.
    '|[-+]?\\.(?:inf|Inf|INF)|\\.(?:nan|NaN|NAN)',
    // A date or date-time resolves to a Date, not to a string.
    '|\\d{4}-\\d{1,2}-\\d{1,2}(?:[Tt\\s].*)?',
    ')$',
  ].join('')
);

function isPlainObject(value) {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  return Object.getPrototypeOf(value) === Object.getPrototypeOf({});
}

export class YamlSchemaFormatter extends BaseSchemaFormatter {
  public fileExtension(): string {
    return 'yml';
  }

  protected cubeReference(cube: string): string {
    return `{${cube}}`;
  }

  protected renderFile(fileDescriptor: Record<string, unknown>): string {
    const { cube, sql, preAggregations: _, ...descriptor } = fileDescriptor;

    return `cubes:\n  - name: ${cube}${this.render(
      {
        ...(sql ? { sql } : null),
        ...descriptor,
      },
      2
    )}\n`;
  }

  protected render(
    value: SchemaDescriptor,
    level = 0,
    parent?: SchemaDescriptor,
    flow = false
  ) {
    const indent = Array(level * 2)
      .fill(0)
      .reduce((memo) => `${memo} `, '');

    if (value instanceof MemberReference) {
      return value.member;
    } else if (value instanceof ValueWithComments) {
      const comments = `\n${value.comments
        .map((comment) => `${indent}# ${comment}`)
        .join('\n')}\n`;

      return value.value ? `${this.render(value.value)}${comments}` : comments;
    } else if (Array.isArray(value)) {
      if (
        value.every(
          (v) => typeof v !== 'object' || v instanceof MemberReference
        )
      ) {
        // The caller separates keys itself — a newline here doubles the blank line.
        // Pass the array as parent so scalar elements skip the leading-space branch
        // that indents a value after its key, and `flow` so they quote on the
        // characters that would otherwise end an item in a flow sequence.
        return ` [${value
          .map((v) => this.render(v, level + 1, value, true))
          .join(', ')}]`;
      }

      return `\n${value
        .map((v) => `${indent}- ${this.render(v, level + 1, value)}`)
        .join('\n')}`;
    } else if (typeof value === 'object') {
      if (parent) {
        return `${!Array.isArray(parent) ? '\n' : ''}${Object.entries(value)
          .map(
            ([k, v], index) => `${
              Array.isArray(parent) && index === 0 ? '' : `${indent}`
            }${k}:${this.render(v, level + 1, value)}`
          )
          .join('\n')}\n`;
      }

      const newLineKeys = Object.keys(value).includes('data_source') ? ['data_source'] : ['sql_table'];
      const content = Object.keys(value)
        .map((key) => {
          if (!isPlainObject(value[key])) {
            const newLine = newLineKeys.includes(key) ? '\n' : '';
            return `${indent}${key}:${this.render(
              value[key],
              level + 1,
              value
            )}${newLine}`;
          }

          const entries = Object.entries(value[key] || {});

          // An empty object renders inline as `[]`, which the array branch leaves
          // unseparated; a non-empty one ends in its own newline. Keep both apart
          // from the next key.
          return `${indent}${key}:${this.render(
            entries.map(([ok, ov]) => ({
              name: ok,
              // @ts-ignore
              ...Object.entries(ov)
                .filter(([, v]) => v != null)
                .reduce((memo, [k, v]) => ({ ...memo, [k]: v }), {}),
            })),
            level + 1,
            value
          )}${entries.length ? '' : '\n'}`;
        })
        .join('\n');

      return `\n${content}`;
    }

    return `${Array.isArray(parent) ? '' : ' '}${this.escapedValue(
      value,
      flow
    )}`;
  }

  private escapedValue(
    value: string | number | boolean,
    flow = false
  ): string | number | boolean {
    if (typeof value !== 'string') {
      return value;
    }

    return this.needsQuotes(value, flow)
      ? `"${value.replace(/\\/g, '\\\\').replace(/"/g, '\\"')}"`
      : value;
  }

  /**
   * Whether a plain scalar has to be quoted to survive a YAML round-trip.
   *
   * Values reach here from the warehouse — `title` is the titleized column name — so
   * a column named `revenue: usd` must not be allowed to turn the generated file into
   * something YAML parses differently, or fails to parse at all.
   *
   * Each test below is a case that demonstrably does not round-trip; anything else stays
   * unquoted, so ordinary member names and SQL expressions are unaffected.
   */
  private needsQuotes(value: string, flow: boolean): boolean {
    return (
      // An indicator character only has meaning in the first position: it would start a
      // sequence entry, an alias, a tag, a block scalar, a quoted string or a comment.
      // `-` also covers the bare `-`, which is an empty sequence entry.
      /^[-?:,[\]{}#&*!|>'"%@`]/.test(value) ||
      // `key: value` inside a scalar makes it a mapping; a trailing colon does the same.
      /:(?:\s|$)/.test(value) ||
      // A `#` after whitespace starts a comment and truncates the rest of the line.
      // A bare `a#b` is a legal plain scalar, so it stays unquoted.
      /\s#/.test(value) ||
      // Surrounding whitespace is stripped from a plain scalar, and a tab or newline
      // cannot appear in one at all.
      /^\s|\s$|[\n\t]/.test(value) ||
      // A plain scalar matching a YAML type resolves to that type, not to a string.
      YAML_TYPE_SHAPED.test(value) ||
      // `{` opens a flow mapping, and a `"` would be read as a quoted scalar.
      /[{}"]/.test(value) ||
      // Inside a flow sequence these end the item; in a block scalar they are literal.
      (flow && /[,[\]]/.test(value))
    );
  }
}
