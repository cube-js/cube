const PUNCTUATION = /[^\p{L}\p{N}]+/ug;
const REGEX = /([\p{Lu}]+[\p{Ll}\p{N}]*|[\p{Ll}\p{N}]+)/gu;
const LAZY_UPPERCASE_REGEX = /([\p{Lu}]{2,}(?![\p{Ll}\p{N}])|[\p{Lu}]+[\p{Ll}\p{N}]*|[\p{Ll}\p{N}]+)/gu;
const PRESERVE_UPPERCASE_REGEX = /([\p{Lu}]{2,}|[\p{Lu}][\p{Ll}]*|[\p{Ll}\p{N}]+)/gu;

const splitString = (value: string, options: any = {}) => {
  // eslint-disable-next-line no-nested-ternary
  const regex = options.preserveConsecutiveUppercase
    ? PRESERVE_UPPERCASE_REGEX
    : (options.lazyUppercase !== false ? LAZY_UPPERCASE_REGEX : REGEX);

  const input = value.trim();
  const words = value ? (input.match(regex) || []).filter(Boolean) : [];
  const output = words.filter(Boolean);

  if (output.length === 0 && value.length > 0) {
    return [value.replace(PUNCTUATION, '')];
  }

  return output;
};

const transformWords = (input: string, options, joinChar = '', transformFn = s => s) => (input ? splitString(input, options).map(transformFn).join(joinChar) : '');

const lowercase = (input = '', options) => input.toLocaleLowerCase(options?.locale);

type Options = {
  uppercase?: boolean;
  locale?: string;
};

export function toSnakeCase(input = '', options: Options = {}) {
  const output = lowercase(transformWords(input, options, '_'), options);

  if (options?.uppercase) {
    return output.toLocaleUpperCase(options?.locale);
  }

  return output;
}

const NON_SCALAR_TYPE_SUBSTRINGS = /json|struct|bytea|blob|varbinary|varbyte|geometry|geography/i;
const NON_SCALAR_TYPE_WORDS = new RegExp(`\\b(${[
  'array', 'list', 'object', 'map', 'tuple', 'record', 'row', 'super', 'variant', 'vector',
  'binary', 'bytes', 'raw', 'bfile', 'image', 'xml', 'xmltype', 'hllsketch',
  'point', 'linestring', 'polygon', 'multipoint', 'multilinestring', 'multipolygon',
].join('|')})\\b`, 'i');
// Enum labels are part of some type names, e.g. `Enum8('record' = 1)`.
const QUOTED_LITERALS = /'(?:[^']|'')*'/g;

/**
 * Whether a raw warehouse column type holds a document, a collection, binary data or a
 * shape rather than a single readable value: `jsonb`, `VARIANT`, `text[]`, `ARRAY`,
 * `Map(String, UInt8)`, `row(x varchar)`, `bytea`, `geography`. Reads the type name only,
 * never the column name.
 */
export function isNonScalarColumnType(dbType: string | undefined): boolean {
  if (!dbType) {
    return false;
  }

  const type = dbType.replace(QUOTED_LITERALS, '');

  return /\[\d*\]/.test(type) ||
    NON_SCALAR_TYPE_SUBSTRINGS.test(type) ||
    NON_SCALAR_TYPE_WORDS.test(type);
}
