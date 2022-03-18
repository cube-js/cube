import spawn from 'cross-spawn';
import { Readable } from 'stream';
import shell from "shelljs";

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

// Executes `command` in `dir`, returns the error code. Preserves working directory.
export function execInDir(dir: string, command: string): number {
  const crtDir = process.cwd();
  try {
    process.chdir(dir);
    const result = shell.exec(command);
    return result.code;
  } finally {
    process.chdir(crtDir);
  }
}

export function assertNonNullable<T>(name: string, x: T): asserts x is NonNullable<T> {
  if (x === undefined || x === null) {
    throw new Error(`${name} is not defined.`);
  }
}

// If x is nullable, throws and error, else return x with a nonnulable type.
export function checkNonNullable<T>(name: string, x: T): NonNullable<T> {
  assertNonNullable(name, x);
  return x;
}

export async function streamToArray<T>(stream: Readable): Promise<T[]> {
  const result: T[] = [];
  for await (const x of stream) {
    result.push(x);
  }
  return result;
}

// https://nodejs.org/api/stream.html#readablewrapstream
// https://nodejs.org/api/stream.html#compatibility-with-older-nodejs-versions
export async function oldStreamToArray<T>(stream: NodeJS.ReadableStream): Promise<T[]> {
  return streamToArray(new Readable().wrap(stream));
}
