import { bridgeHarnessAvailable, parseArgsNames } from './helpers';

const describeBridge = bridgeHarnessAvailable ? describe : describe.skip;

describeBridge('bridge: args_names parser', () => {
  describe('working forms', () => {
    it('parses arrow with a single parenthesized arg', () => {
      expect(parseArgsNames((x: any) => x)).toEqual(['x']);
    });

    it('parses arrow with multiple args', () => {
      expect(parseArgsNames((x: any, y: any) => [x, y])).toEqual(['x', 'y']);
    });

    it('parses arrow with no args', () => {
      expect(parseArgsNames(() => 42)).toEqual([]);
    });

    it('parses async arrow', () => {
      expect(parseArgsNames(async (x: any) => x)).toEqual(['x']);
    });

    it('recognises FILTER_PARAMS / FILTER_GROUP / SECURITY_CONTEXT names', () => {
      expect(
        parseArgsNames(
          (CUBE: any, FILTER_PARAMS: any, SECURITY_CONTEXT: any) => [CUBE, FILTER_PARAMS, SECURITY_CONTEXT]
        )
      ).toEqual(['CUBE', 'FILTER_PARAMS', 'SECURITY_CONTEXT']);
    });
  });

  // The skipped tests below assert how a correct parser should behave.
  // They are buggy today because both this bridge and the JS-side
  // schema-compiler use the same regex; a fix must touch both.
  describe('known bugs (skip = expected behavior after fix)', () => {
    // The named-function branch of the regex captures [A-Za-z0-9_,]* —
    // no whitespace allowed. V8 renders `function f(x, y)` with a space
    // after the comma, so capture stops after the first arg.
    it.skip('parses named function declaration with multiple args', () => {
      function named(x: any, y: any) {
        return [x, y];
      }
      expect(parseArgsNames(named)).toEqual(['x', 'y']);
    });

    it.skip('parses async named function declaration with multiple args', () => {
      async function named(x: any, y: any) {
        return [x, y];
      }
      expect(parseArgsNames(named)).toEqual(['x', 'y']);
    });

    // Default args land inside the (...) capture as a single token "x = 1"
    // and survive the comma split unchanged. Special names like
    // SECURITY_CONTEXT in this position fail to dispatch.
    it.skip('parses default args, returning just the identifier', () => {
      expect(parseArgsNames((x: any = 1) => x)).toEqual(['x']);
    });

    // Rest args keep their leading dots after capture+split, yielding
    // "...args" instead of "args".
    it.skip('parses rest args, dropping the spread dots', () => {
      expect(parseArgsNames((...args: any[]) => args)).toEqual(['args']);
    });

    // Destructuring patterns get split by the comma inside the braces,
    // breaking the pattern into half-tokens.
    it.skip('parses destructuring args, returning the destructured identifiers', () => {
      expect(parseArgsNames(({ a, b }: any) => [a, b])).toEqual(['a', 'b']);
    });

    // Anonymous function expressions (`function (x){}`) match no branch
    // of the regex at all. Today Rust silently returns []; the JS side
    // throws `Can't match args for: ...`. Neither is the desired
    // behavior — the parser should just return the args.
    it.skip('parses anonymous function expressions', () => {
      // eslint-disable-next-line func-names, prefer-arrow-callback
      const fn = function (x: any) { return x; };
      expect(parseArgsNames(fn)).toEqual(['x']);
    });
  });
});
