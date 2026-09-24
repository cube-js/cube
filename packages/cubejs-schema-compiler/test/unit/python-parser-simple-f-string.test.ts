import babelGenerator from '@babel/generator';
import * as t from '@babel/types';

import { PythonParser, transpileSimpleFString } from '../../src/parser/PythonParser';

const withoutPositions = (node: t.Node) => JSON.parse(JSON.stringify(node, (key, value) => (
  ['loc', 'start', 'end', 'extra'].includes(key) ? undefined : value
)));

describe('transpileSimpleFString', () => {
  it.each([
    'f"count"',
    'f"{CUBE}.id"',
    'f"{CUBE}.customer_id = {customers.id}"',
    'f"{orders.amount} / NULLIF({count}, 0)"',
    'f"CASE WHEN {amount} < 100 THEN \'small\' ELSE \'large\' END"',
    'f"{a.b.c}"',
    'f"x{a}"',
    'f"{a}x"',
    'f"multi\nline {CUBE}.x\n"',
    'f"$ {x}"',
    'f"100%"',
    'f"  padded  "',
  ])('matches the parser for %s', (code) => {
    const fast = transpileSimpleFString(code);
    const parsed = new PythonParser(code).transpileToJs();

    expect(fast).not.toBeNull();
    expect(withoutPositions(fast!)).toEqual(withoutPositions(parsed));
    expect(babelGenerator(fast!).code).toEqual(babelGenerator(parsed).code);
  });

  it.each([
    // Not an f-string
    'CUBE.id',
    '"plain"',
    'f""',
    // Quotes, escapes and backticks need the lexer
    'f"{CUBE}.\\"quoted\\""',
    'f"\\`tick\\`"',
    'f"back\\\\slash"',
    // Anything but a dotted name inside braces
    'f"{x()}"',
    'f"{ x }"',
    'f"{1x}"',
    'f"{x.1}"',
    'f"{a + b}"',
    'f"{\'lit\'}"',
    // Keywords parse differently from names
    'f"{None}"',
    'f"{x.True}"',
    'f"{lambda}"',
    // Adjacent expressions and stray braces are reported by the parser
    'f"{a}{b}"',
    'f"a}b"',
    'f"a{b"',
  ])('leaves %s to the parser', (code) => {
    expect(transpileSimpleFString(code)).toBeNull();
  });
});
