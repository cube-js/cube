import path from 'path';
import { download, getEnv } from '@cubejs-backend/shared';

function acceptedByEnv() {
  const acceptStatus = getEnv('mongodbJDBCAcceptPolicy');
  if (acceptStatus) {
    console.log('You accepted Terms & Conditions for JDBC driver from Mongo by CUBEJS_DB_MONGO_DB_JDBC_ACCEPT_POLICY');
  }

  if (acceptStatus === false) {
    console.log('You declined Terms & Conditions for JDBC driver from Mongo by CUBEJS_DB_MONGO_DB_JDBC_ACCEPT_POLICY');
    console.log('Installation will be skipped');
  }

  return acceptStatus;
}

export const driverName = 'mongodb-jdbc-2.0.0-all.jar';

export async function downloadJDBCDriver(): Promise<string | null> {
  const driverAccepted = acceptedByEnv();

  if (driverAccepted) {
    console.log(`Downloading ${driverName}`);

    const filePath = await download(
      `https://repo1.maven.org/maven2/org/mongodb/mongodb-jdbc/2.0.0/${driverName}`,
      {
        showProgress: true,
        filename: driverName,
        cwd: path.resolve(path.join(__dirname, '..', 'download')),
      }
    );

    return path.resolve(filePath);
  }

  return null;
}
