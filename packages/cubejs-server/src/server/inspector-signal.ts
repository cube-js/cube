import inspector from 'inspector';

/** Never throws: it runs from a signal handler, where an exception would kill the process. */
export function toggleInspector(port: number = process.debugPort): void {
  try {
    if (inspector.url()) {
      inspector.close();
      console.log('Inspector closed');

      return;
    }

    // A busy port does not throw: Node.js reports it on stderr and leaves the inspector closed
    inspector.open(port, '127.0.0.1');

    const url = inspector.url();
    if (url) {
      console.log(`Inspector listening on ${url}`);
    } else {
      console.error(`Unable to open inspector on 127.0.0.1:${port}`);
    }
  } catch (e: any) {
    console.error(`Unable to toggle inspector: ${e.message || e}`);
  }
}
