const crypto = require('crypto');

class BaseQueueDriver {
  redisHash(queryKey) {
    return typeof queryKey === 'string' && queryKey.length < 256 ?
      queryKey :
      crypto.createHash('md5').update(JSON.stringify(queryKey)).digest("hex");
  }
}

module.exports = BaseQueueDriver;
