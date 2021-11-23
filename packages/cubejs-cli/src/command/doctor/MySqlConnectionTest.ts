import { ConnectionTest } from './ConnectionTest';

export class MySqlConnectionTest extends ConnectionTest {
  public createDriver = async (options) => {
    const { MySqlDriver } = await import('@cubejs-backend/mysql-driver');
    return new MySqlDriver(options);
  };

  public handleErrors = (e, config, _msg?) => {
    let msg = _msg ?? '';

    if (e.code === 'ER_ACCESS_DENIED_ERROR') {
      msg = [
        `Authentication failed for user "${config.user}", this might be caused by`,
        '',
        '  - The username doesn\'t exist',
        '  - The password is incorrect',
      ].join('\n');
    }

    // Database not found
    if (e.code === 'ER_BAD_DB_ERROR') {
      msg = [
        `The database "${config.database}" could not be found, this might be caused by`,
        '',
        '  - The database doesn\'t exist',
      ].join('\n');
    }

    return msg;
  };
}
