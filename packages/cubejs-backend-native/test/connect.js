const mysql = require('mysql2/promise');

(async () => {
    console.log('start');

    try {
        console.log('connecting');

        await mysql.createConnection({
            connectTimeout: 1000,
            host: '127.0.0.1',
            port: 3306,
            user: 'ovr',
            password: 'test',
          });

          console.log('connected');
    } catch (e) {
        console.log(e);
    }
})();