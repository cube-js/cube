import EditorWorker from './workers/editor.worker?worker';
import CSSWorker from './workers/css.worker?worker';
import JSONWorker from './workers/json.worker?worker';
import TSWorker from './workers/ts.worker?worker';

const FakeWorker = {
  onmessage: () => {},
  postMessage: () => {},
  terminate: () => {},
  addEventListener: () => {},
  removeEventListener: () => {},
  dispatchEvent: () => false,
};

export function setupWorkers() {
  if (!window.MonacoEnvironment) {
    window.MonacoEnvironment = {
      // @ts-expect-error global variable type not defined
      getWorker(_: never, label: string) {
        switch (label) {
          case 'editorWorkerService':
            return new EditorWorker();
          case 'css':
          case 'less':
          case 'scss':
            return new CSSWorker();
          case 'javascript':
          case 'typescript':
            return new TSWorker();
          // Turn this off as it doesn't work with Jinja
          case 'yml':
          case 'yaml':
            return FakeWorker;
          case 'json':
            return new JSONWorker();
          default:
            throw new Error(`Unknown Monaco worker label: ${label}`);
        }
      },
    };
  }
}
