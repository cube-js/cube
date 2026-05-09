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

  describe('edge cases', () => {
    it('parses named function declaration with multiple args', () => {
      function named(x: any, y: any) {
        return [x, y];
      }
      expect(parseArgsNames(named)).toEqual(['x', 'y']);
    });

    it('parses async named function declaration with multiple args', () => {
      async function named(x: any, y: any) {
        return [x, y];
      }
      expect(parseArgsNames(named)).toEqual(['x', 'y']);
    });

    it('parses default args, returning just the identifier', () => {
      expect(parseArgsNames((x: any = 1) => x)).toEqual(['x']);
    });

    it('parses rest args, dropping the spread dots', () => {
      expect(parseArgsNames((...args: any[]) => args)).toEqual(['args']);
    });

    it('parses destructuring args, returning the destructured identifiers', () => {
      expect(parseArgsNames(({ a, b }: any) => [a, b])).toEqual(['a', 'b']);
    });

    it('parses anonymous function expressions', () => {
      // eslint-disable-next-line func-names, prefer-arrow-callback
      const fn = function (x: any) { return x; };
      expect(parseArgsNames(fn)).toEqual(['x']);
    });
  });
});
