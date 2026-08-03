import { AsyncDebounce } from '../src';

describe('AsyncDebounce decorator', () => {
  test('should preserve this context', async () => {
    class TestClass {
      private value = 'test-value';

      @AsyncDebounce()
      public async getValue() {
        return this.value;
      }
    }

    const instance = new TestClass();
    const result = await instance.getValue();

    expect(result).toBe('test-value');
  });
});
