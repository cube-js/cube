import { startCubeStore } from './process';

(async () => {
  const process = await startCubeStore();

  process.stdout.on('data', (v) => {
    console.log(v.toString());
  });
  process.stderr.on('data', (v) => {
    console.log(v.toString());
  });
})();
