/**
 * V8 keeps at most 128 properties on a fast-mode object, so a wider shape is a dictionary itself
 * and cloning it costs an order of magnitude more than filling a fresh object.
 */
export const MAX_OBJECT_SHAPE_COLUMNS = 127;

/**
 * Builds an object holding every column of a result set, to be cloned per row with the object
 * spread:
 *
 * ```js
 * const objectShape = buildObjectShape(names);
 * const row = objectShape === null ? {} : { ...objectShape };
 * for (let i = 0; i < names.length; i++) row[names[i]] = values[i];
 * ```
 *
 * Filling a clone overwrites fields that already exist instead of growing the object, which is what
 * keeps the row in fast-properties mode. Adding properties one at a time to a fresh `{}` drops the
 * object into a dictionary past ~19 of them, and everything that later reads the row by name pays
 * for it -- `rowsToColumnarBuffer` reads every cell of every row by name.
 *
 * Only the spread clones the shape. `Object.assign({}, shape)` grows a fresh object and lands back
 * in a dictionary, so it is not a substitute.
 *
 * `JSON.parse` is what makes the shape worth having: it returns a fast-mode object holding all the
 * columns, and it defines a `__proto__` column as an own data property rather than invoking
 * `Object.prototype`'s setter, so a column aliased to `__proto__` stays a normal cell. Names are
 * escaped with `JSON.stringify`, which is what makes them safe to take from the wire.
 *
 * @see https://v8.dev/blog/fast-properties for what fast-properties and dictionary mode mean.
 *
 * @returns the shape to clone, or `null` when cloning cannot pay off and rows should be built from
 * a plain `{}`.
 */
export function buildObjectShape(names: ReadonlyArray<string>): Record<string, unknown> | null {
  if (names.length === 0 || names.length > MAX_OBJECT_SHAPE_COLUMNS) {
    return null;
  }

  return JSON.parse(`{${names.map((name) => `${JSON.stringify(name)}:null`).join(',')}}`);
}
