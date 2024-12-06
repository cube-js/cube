import { BaseDriver } from '@cubejs-backend/base-driver';
import { CubeStoreDriver } from '@cubejs-backend/cubestore-driver';

export async function getDriver(type: string): Promise<{
  source: BaseDriver,
  storage: BaseDriver,
}> {
  return import(`@cubejs-backend/${type}-driver`).then((module) => {
    // eslint-disable-next-line new-cap
    const source: BaseDriver = new module.default();
    source.setLogger((msg: unknown, event: unknown) => console.log(`${msg}: ${JSON.stringify(event)}`));
    const storage = new CubeStoreDriver();
    return { source, storage };
  });
}
