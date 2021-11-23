import { ConnectionTest } from './ConnectionTest';

export class PostgresConnectionTest extends ConnectionTest {
  public constructor() {
    super();
  }

  public createDriver = async (options) => {
    const { PostgresDriver } = await import('@cubejs-backend/postgres-driver');
    return new PostgresDriver(options);
  };

  public handleErrors = (e, config, _msg?) => {
    let msg = _msg ?? '';

    if (e.code === '28P01') {
      msg = [
        `Authentication failed for user "${config.user}", this might be caused by`,
        '',
        '  - The username doesn\'t exist',
        '  - The password is incorrect',
      ].join('\n');
    }

    // Database not found
    if (e.code === '3D000') {
      msg = [
        `The database "${config.database}" could not be found, this might be caused by`,
        '',
        '  - The database doesn\'t exist',
      ].join('\n');
    }

    return msg;
  };
}
