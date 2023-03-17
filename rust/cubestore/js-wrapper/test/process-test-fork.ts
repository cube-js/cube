import { CubeStoreHandler } from '../src/process';

(async () => {
  const handler = new CubeStoreHandler({
    stdout: (v) => {
      console.log(v.toString());
    },
    stderr: (v) => {
      console.log(v.toString());
    },
    onRestart: () => {
      //
    },
  });

  await handler.acquire();

  process.exit(1);
})();
