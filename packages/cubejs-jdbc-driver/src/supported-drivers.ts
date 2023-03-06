export interface DriverOptionsInterface {
  driverClass: string;
  prepareConnectionQueries: string[];
  mavenDependency: Record<string, any>;
  properties: Record<string, any>;
  jdbcUrl: () => string;
}

export const SupportedDrivers: Record<string, DriverOptionsInterface> = {
  mysql: {
    driverClass: 'com.mysql.jdbc.Driver',
    prepareConnectionQueries: ['SET time_zone = \'+00:00\''],
    mavenDependency: {
      groupId: 'mysql',
      artifactId: 'mysql-connector-java',
      version: '8.0.13'
    },
    properties: {
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
    },
    jdbcUrl: () => `jdbc:mysql://${process.env.CUBEJS_DB_HOST}:3306/${process.env.CUBEJS_DB_NAME}`
  },
  athena: {
    driverClass: 'com.qubole.jdbc.jdbc41.core.QDriver',
    prepareConnectionQueries: [],
    mavenDependency: {
      groupId: 'com.syncron.amazonaws',
      artifactId: 'simba-athena-jdbc-driver',
      version: '2.0.2'
    },
    jdbcUrl: () => `jdbc:awsathena://AwsRegion=${process.env.CUBEJS_AWS_REGION}`,
    properties: {
      UID: process.env.CUBEJS_AWS_KEY,
      PWD: process.env.CUBEJS_AWS_SECRET,
      S3OutputLocation: process.env.CUBEJS_AWS_S3_OUTPUT_LOCATION
    }
  },
  sparksql: {
    driverClass: 'org.apache.hive.jdbc.HiveDriver',
    prepareConnectionQueries: [],
    mavenDependency: {
      groupId: 'org.apache.hive',
      artifactId: 'hive-jdbc',
      version: '2.3.5'
    },
    jdbcUrl: () => `jdbc:hive2://${process.env.CUBEJS_DB_HOST}:${process.env.CUBEJS_DB_PORT || '10000'}/${process.env.CUBEJS_DB_NAME}`,
    properties: {
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
    }
  },
  hive: {
    driverClass: 'org.apache.hive.jdbc.HiveDriver',
    prepareConnectionQueries: [],
    mavenDependency: {
      groupId: 'org.apache.hive',
      artifactId: 'hive-jdbc',
      version: '2.3.5'
    },
    jdbcUrl: () => `jdbc:hive2://${process.env.CUBEJS_DB_HOST}:${process.env.CUBEJS_DB_PORT || '10000'}/${process.env.CUBEJS_DB_NAME}`,
    properties: {
      user: process.env.CUBEJS_DB_USER,
      password: process.env.CUBEJS_DB_PASS,
    }
  }
};
