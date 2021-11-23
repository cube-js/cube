import { ConnectionTest } from './ConnectionTest';

export class ClickHouseConnectionTest extends ConnectionTest {
  public createDriver = async (options) => {
    const { ClickHouseDriver } = await import('@cubejs-backend/clickhouse-driver');
    return new ClickHouseDriver(options);
  };

  public handleErrors = (e, config, _msg?) => {
    let msg = _msg ?? '';

    if (e.code === 'ERR_SOCKET_BAD_PORT') {
      msg = [
        '',
        '',
        `  - ${e.message}`,
      ].join('\n');
    }

    if (e.code === 516) {
      msg = [
        `Authentication failed for user "${config.user}", this might be caused by`,
        '',
        '  - The username doesn\'t exist',
        '  - The password is incorrect',
      ].join('\n');
    }

    // Database not found
    if (e.code === 81) {
      msg = [
        `The database "${config.database}" could not be found, this might be caused by`,
        '',
        '  - The database doesn\'t exist',
      ].join('\n');
    }

    return msg;
  };
}
