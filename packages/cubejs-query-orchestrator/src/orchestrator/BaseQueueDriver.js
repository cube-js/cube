import crypto from 'crypto';

export class BaseQueueDriver {
  redisHash(queryKey) {
    return typeof queryKey === 'string' && queryKey.length < 256 ?
      queryKey :
      crypto.createHash('md5').update(JSON.stringify(queryKey)).digest('hex');
  }
}
