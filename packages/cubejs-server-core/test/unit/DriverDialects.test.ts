import { BaseQuery } from '@cubejs-backend/schema-compiler';

import DriverDependencies from '../../src/core/DriverDependencies';
import { lookupDriverClass } from '../../src/core/DriverResolvers';

// Reading a static method needs none of these: native bindings are not built everywhere (`java`
// needs a JDK at install time), and snowflake-sdk starts async platform detection on load, which
// outlives the test.
jest.mock('java', () => ({}));
jest.mock('sqlite3', () => ({}));
jest.mock('snowflake-sdk', () => ({}));

describe('Driver dialects', () => {
  test.each(Object.keys(DriverDependencies))('%s driver implements its own static dialectClass()', (dbType) => {
    const driverClass = lookupDriverClass(dbType);

    // An inherited dialectClass() silently gives e.g. Redshift the Postgres dialect.
    expect(Object.prototype.hasOwnProperty.call(driverClass, 'dialectClass')).toBe(true);

    const dialectClass = driverClass.dialectClass();
    if (dbType === 'jdbc') {
      // The dialect depends on the database behind the JDBC URL.
      expect(dialectClass).toBeUndefined();
    } else {
      expect(dialectClass.prototype).toBeInstanceOf(BaseQuery);
    }
  });
});
