const { Client } = require('pg');

(async () => {
  console.log('start');

  try {
    console.log('connecting');

    const client = new Client({
      connectTimeout: 1000,
      host: '127.0.0.1',
      port: 3306,
      user: 'ovr',
      password: 'test',
    });
    await client.connect();

    console.log('connected');
  } catch (e) {
    console.log(e);
  }
})();
