import { BaseDriver } from '@cubejs-backend/base-driver';

export async function getDriver(type: string): Promise<BaseDriver> {
  return import(`@cubejs-backend/${type}-driver`).then((module) => {
    // eslint-disable-next-line new-cap
    const driver: BaseDriver = new module.default();
    return driver;
  });
}
