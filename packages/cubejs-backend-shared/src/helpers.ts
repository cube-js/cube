import spawn from 'cross-spawn';
import stream from "stream";

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

export async function streamToArray<T>(xstream: stream.Readable): Promise<T[]> {
  const result: T[] = [];
  for await (const x of xstream) {
    result.push(x);
  }
  return result;
}

export async function nodeStreamToArray<T>(nodeStream: NodeJS.ReadableStream): Promise<T[]> {
  return streamToArray(new stream.Readable().wrap(nodeStream));
}

