import { CubeStoreHandler } from './process';

(async () => {
  const handler = new CubeStoreHandler({
    stdout: (v) => {
      console.log(v.toString());
    },
    stderr: (v) => {
      console.log(v.toString());
    },
    onRestart: () => console.log('Cube Store Restarting'),
  });

  await handler.acquire();
})();
