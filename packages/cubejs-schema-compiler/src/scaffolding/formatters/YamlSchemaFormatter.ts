import { MemberReference } from '../descriptors/MemberReference';
import { ValueWithComments } from '../descriptors/ValueWithComments';
import {
  SchemaDescriptor
} from '../ScaffoldingTemplate';
import { BaseSchemaFormatter } from './BaseSchemaFormatter';

/**
 * Plain scalars that a YAML loader resolves to something other than a string — null, a
 * boolean, a number or a timestamp — so a title or member name of this shape has to be
 * quoted to stay a string. Covers the YAML 1.1 binary and octal integers loaders still
 * accept, since resolution is what matters here, not which spec version nominally
 * applies; sexagesimal (`1:30`) is left out because js-yaml 4 — the loader
 * `YamlCompiler` uses — dropped it. `yes`/`on`/`off` are strings and stay unquoted.
 */
const YAML_TYPE_SHAPED = new RegExp(
  [
    '^(?:',
    // The empty alternative is deliberate: a bare empty scalar loads as null.
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

    // The cube name is interpolated rather than rendered, so it needs the predicate
    // applied by hand — it comes from the table name the same way a member name comes
    // from a column, and a cube that names itself `2024` (a number) is not the cube
    // another file's join refers to as `"2024"` (a string).
    return `cubes:\n  - name: ${this.escapedValue(
      cube as string
    )}${this.render(
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
      // Quote on the same rule as the member name this points at, or the two stop
      // denoting the same member — see the drill-member cases in
      // `scaffolding-template.test.ts`.
      return this.escapedValue(value.member, flow);
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

    // YAML's double-quoted style is a superset of JSON's string escaping, so
    // `JSON.stringify` is exactly the escaper this needs — including the quotes. Quoting
    // alone would not be enough: a raw line break inside the quotes is folded to a space
    // (so the value does not come back as the string that went in) and the other C0
    // controls make js-yaml reject the file outright. Escaping is what keeps a value one
    // line and one string.
    return this.needsQuotes(value, flow) ? JSON.stringify(value) : value;
  }

  /**
   * Whether a plain scalar has to be quoted to survive a YAML round-trip.
   *
   * Values reach here from the warehouse — `title` is the titleized column name — so
   * a column named `revenue: usd` must not be allowed to turn the generated file into
   * something YAML parses differently, or fails to parse at all.
   *
   * Each test below is a case that does not round-trip, except the first-position set,
   * which over-quotes on purpose: `-abc` and `?abc` are legal plain scalars in YAML 1.2,
   * but quoting is the safe direction and the rule stays one character wide. Anything
   * else stays unquoted, so ordinary member names and SQL expressions are unaffected.
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
      // Surrounding whitespace is stripped from a plain scalar. A control character has
      // to be escaped rather than merely quoted: a line break — a lone CR is one — would
      // otherwise fold to a space, and the C0/C1 controls and an unpaired surrogate are
      // rejected outright by the loader as non-printable. The surrogate range also
      // matches a well-formed astral pair, which only costs that value a pair of quotes.
      // eslint-disable-next-line no-control-regex
      /^\s|\s$|[\x00-\x1f\x7f-\x9f]|[\uD800-\uDFFF]/.test(value) ||
      // A plain scalar matching a YAML type resolves to that type, not to a string.
      YAML_TYPE_SHAPED.test(value) ||
      // `{` opens a flow mapping, and a `"` would be read as a quoted scalar.
      /[{}"]/.test(value) ||
      // Inside a flow sequence these end the item; in a block scalar they are literal.
      (flow && /[,[\]]/.test(value))
    );
  }
}
