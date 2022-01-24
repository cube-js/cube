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

// If x is nullable, throws and error, else return x with a nonnulable type.
export function checkNonNullable<T>(name: string, x: T): NonNullable<T> {
  if (x === undefined || x === null) {
    throw new Error(`${name} is not defined.`);
  } else {
    return x!;
  }
}
