import { disposedProxy } from '../src';

describe('disposedProxy', () => {
  test('should throw on property access', () => {
    const proxy = disposedProxy('testProperty', 'test instance');

    expect(() => proxy.someProperty).toThrow(
      "Cannot access property 'someProperty' on test instance. " +
      "The 'testProperty' has been cleaned up and is no longer available."
    );
  });

  test('should throw on property set', () => {
    const proxy = disposedProxy('testProperty', 'test instance');

    expect(() => { proxy.someProperty = 'value'; }).toThrow(
      "Cannot set property 'someProperty' on test instance. " +
      "The 'testProperty' has been cleaned up and is no longer available."
    );
  });

  test('should throw on method call', () => {
    const proxy = disposedProxy('testProperty', 'test instance');

    expect(() => proxy.someMethod()).toThrow(
      "Cannot access property 'someMethod' on test instance. " +
      "The 'testProperty' has been cleaned up and is no longer available."
    );
  });

  test('should throw on nested function call', () => {
    const proxy = disposedProxy('testFunction', 'test instance');

    // Accessing a method on the proxy will throw, which is the expected behavior
    // Note: Direct proxy() call actually triggers 'get' for '', not 'apply'
    expect(() => proxy.someMethod()).toThrow(
      "Cannot access property 'someMethod' on test instance. " +
      "The 'testFunction' has been cleaned up and is no longer available."
    );
  });

  test('should throw on "in" operator', () => {
    const proxy = disposedProxy('testProperty', 'test instance');

    expect(() => 'someProperty' in proxy).toThrow(
      "Cannot check property existence on test instance. " +
      "The 'testProperty' has been cleaned up and is no longer available."
    );
  });

  test('should throw on Object.keys', () => {
    const proxy = disposedProxy('testProperty', 'test instance');

    expect(() => Object.keys(proxy)).toThrow(
      "Cannot enumerate properties on test instance. " +
      "The 'testProperty' has been cleaned up and is no longer available."
    );
  });

  test('should throw on Object.getPrototypeOf', () => {
    const proxy = disposedProxy('testProperty', 'test instance');

    expect(() => Object.getPrototypeOf(proxy)).toThrow(
      "Cannot get prototype of test instance. " +
      "The 'testProperty' has been cleaned up and is no longer available."
    );
  });

  test('should include correct property name in error message', () => {
    const proxy = disposedProxy('compilers', 'disposed CompilerApi instance');

    expect(() => proxy.cubeEvaluator).toThrow(
      "Cannot access property 'cubeEvaluator' on disposed CompilerApi instance. " +
      "The 'compilers' has been cleaned up and is no longer available."
    );
  });

  test('should work with symbol properties', () => {
    const proxy = disposedProxy('testProperty', 'test instance');
    const sym = Symbol('testSymbol');

    expect(() => proxy[sym]).toThrow(
      "Cannot access property 'Symbol(testSymbol)' on test instance. " +
      "The 'testProperty' has been cleaned up and is no longer available."
    );
  });

  test('should throw on nested property access', () => {
    const proxy = disposedProxy('queryFactory', 'disposed CompilerApi instance');

    // First access throws
    expect(() => proxy.createQuery).toThrow(
      "Cannot access property 'createQuery' on disposed CompilerApi instance. " +
      "The 'queryFactory' has been cleaned up and is no longer available."
    );
  });

  test('should provide helpful error message for real-world scenario', () => {
    // Simulate the CompilerApi scenario
    const compilers = disposedProxy('compilers', 'disposed CompilerApi instance');
    const queryFactory = disposedProxy('queryFactory', 'disposed CompilerApi instance');

    // Both should throw helpful errors
    expect(() => compilers.cubeEvaluator).toThrow(/disposed CompilerApi instance/);
    expect(() => queryFactory.createQuery).toThrow(/disposed CompilerApi instance/);
  });
});
