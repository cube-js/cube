import spawn from 'cross-spawn';

export function getRealType(value: any): string {
  if (value === null) {
    return 'null';
  }

  return typeof value;
}

export async function executeCommand(
  command: string,
  args: string | string[],
  options = {}
) {
  const argsArray: string[] = typeof args === 'string' ? args.split(' ') : args;
  const child = spawn(
    command,
    argsArray,
    { stdio: 'inherit', ...options }
  );

  return new Promise<void>((resolve, reject) => {
    child.on('close', (code) => {
      if (code !== 0) {
        reject(
          new Error(
            `${command} ${argsArray.join(' ')} failed with exit code ${code}. Please check your console.`
          )
        );
        return;
      }
      resolve();
    });
  });
}

export function assertNonNullable<T>(name: string, x: T): asserts x is NonNullable<T> {
  if (x === undefined || x === null) {
    throw new Error(`${name} is not defined.`);
  }
}

export function checkNonNullable<T>(name: string, x: T): NonNullable<T> {
  assertNonNullable(name, x);
  return x!;
}
