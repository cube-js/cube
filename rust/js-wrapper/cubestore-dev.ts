import { startCubeStoreHandler } from './process';

(async () => {
  const handler = await startCubeStoreHandler({
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
