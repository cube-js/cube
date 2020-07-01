export function buildName(firstName: string, lastName?: string) {
  if (lastName) {
    return firstName + ' ' + lastName;
  } else {
    return firstName;
  }
}

/**
 * This is an internal function.
 */
export function internalFunction(): void {}

/**
 * This is a simple exported function.
 */
export function exportedFunction(): void {}

/**
 * This is a function with multiple arguments and a return value.
 *
 * @param paramZ - This is a string parameter.
 * @param paramG - This is a parameter flagged with any.
 *     This sentence is placed in the next line.
 *
 * @param paramA
 *   This is a **parameter** pointing to an interface.
 *
 *   ~~~
 *   const value:BaseClass = new BaseClass('test');
 *   functionWithArguments('arg', 0, value);
 *   ~~~
 *
 * @returns This is the return value of the function.
 */
export function functionWithParameters(paramZ: string, paramG: any, paramA: Object): number {
  return 0;
}

/**
 * This is a function that is assigned to a variable.
 *
 * @param someParam  This is some numeric parameter.
 * @return This is the return value of the function.
 */
export const variableFunction = function(someParam: number): number {
  return 0;
};

/**
 * This is a function with a parameter that is optional.
 *
 * @param requiredParam  A normal parameter.
 * @param optionalParam  An optional parameter.
 */
export function functionWithOptionalValue(requiredParam: string, optionalParam?: string) {}

/**
 * This is a function with a parameter that has a default value.
 *
 * @param valueA  A parameter with a default string value.
 * @param valueB  A parameter with a default numeric value.
 * @param valueC  A parameter with a default NaN value.
 * @param valueD  A parameter with a default boolean value.
 * @param valueE  A parameter with a default null value.
 * @return This is the return value of the function.
 */
export function functionWithDefaults(
  valueA: string = 'defaultValue',
  valueB: number = 100,
  valueC: number = Number.NaN,
  valueD: boolean = true,
  valueE: boolean = null,
): string {
  return valueA;
}

/**
 * This is a function with rest parameter.
 *
 * @param rest  The rest parameter.
 * @return This is the return value of the function.
 */
export function functionWithRest(...rest: string[]): string {
  return rest.join(', ');
}

/**
 * This is the first signature of a function with multiple signatures.
 *
 * @param value  The name value.
 */
export function multipleSignatures(value: string): string;

/**
 * This is the second signature of a function with multiple signatures.
 *
 * @param value       An object containing the name value.
 * @param value.name  A value of the object.
 */
export function multipleSignatures(value: { name: string }): string;

/**
 * This is the actual implementation, this comment will not be visible
 * in the generated documentation.
 *
 * @return This is the return value of the function.
 */
export function multipleSignatures(): string {
  if (arguments.length > 0) {
    if (typeof arguments[0] == 'object') {
      return arguments[0].name;
    } else {
      return arguments[0];
    }
  }

  return '';
}

/**
 * This is a function that is extended by a module.
 *
 * @param arg An argument.
 */
export function moduleFunction(arg: string): string {
  return '';
}

/**
 * This is the module extending the function moduleFunction().
 */
export module moduleFunction {
  /**
   * This variable is appended to a function.
   */
  export let functionVariable: string;

  /**
   * This function is appended to another function.
   */
  export function append() {}

  /**
   * This function is appended to another function.
   */
  export function prepend() {}
}
