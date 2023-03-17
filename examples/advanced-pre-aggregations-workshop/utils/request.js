const https = require('https');

function request(options) {
  return new Promise((resolve, reject) => {
    https
      .request(options, function (response) {
        let responseBody = '';
        response.on('data', function (data) {
          responseBody += data;
        });
        response.on('end', function () {
          resolve(responseBody);
        });
        response.on('error', function (e) {
          throw e;
        });
      })
      .end();
  });
}

module.exports = request;
