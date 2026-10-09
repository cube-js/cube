/* globals describe,test,expect,afterEach,jest */

import inspector from 'inspector';
import net from 'net';

import { toggleInspector } from '../src/server/inspector-signal';

describe('toggleInspector', () => {
  afterEach(() => {
    inspector.close();
    jest.restoreAllMocks();
  });

  test('opens the inspector on loopback, then closes it on the next call', () => {
    jest.spyOn(console, 'log').mockImplementation(() => undefined);

    // Port 0 lets the OS pick a free port, so the test does not depend on 9229 being free
    toggleInspector(0);

    const url = inspector.url();
    expect(url).toMatch(/^ws:\/\/127\.0\.0\.1:\d+\//);
    expect(console.log).toHaveBeenCalledWith(`Inspector listening on ${url}`);

    toggleInspector(0);

    expect(inspector.url()).toBeUndefined();
    expect(console.log).toHaveBeenCalledWith('Inspector closed');
  });

  test('reports a busy port instead of claiming the inspector is open', async () => {
    jest.spyOn(console, 'log').mockImplementation(() => undefined);
    jest.spyOn(console, 'error').mockImplementation(() => undefined);

    const server = net.createServer();
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const { port } = server.address();

    try {
      expect(() => toggleInspector(port)).not.toThrow();

      expect(inspector.url()).toBeUndefined();
      expect(console.error).toHaveBeenCalledWith(`Unable to open inspector on 127.0.0.1:${port}`);
      expect(console.log).not.toHaveBeenCalled();
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  });
});
