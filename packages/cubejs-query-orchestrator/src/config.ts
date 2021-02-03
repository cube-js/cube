function envIntWithDefault(envVariable: string, defaultValue: number) {
  return (typeof process.env[envVariable] !== 'undefined') ? parseInt(process.env[envVariable], 10) : defaultValue;
}

function envBoolWithDefault(envVariable: string, defaultValue: boolean) {
  return (typeof process.env[envVariable] !== 'undefined') ? process.env[envVariable].toUpperCase() === 'TRUE' : defaultValue;
}

export default {
  CUBEJS_REDIS_POOL_MIN: envIntWithDefault('CUBEJS_REDIS_POOL_MIN', 2),
  CUBEJS_REDIS_POOL_MAX: envIntWithDefault('CUBEJS_REDIS_POOL_MAX', 1000),
  CUBEJS_REDIS_IDLE_TIMEOUT_SECONDS: envIntWithDefault('CUBEJS_REDIS_IDLE_TIMEOUT_SECONDS', 5),
  CUBEJS_REDIS_SOFT_IDLE_TIMEOUT_SECONDS: envIntWithDefault('CUBEJS_REDIS_SOFT_IDLE_TIMEOUT_SECONDS', -1),
  CUBEJS_REDIS_USE_IOREDIS: envBoolWithDefault('CUBEJS_REDIS_USE_IOREDIS', false),
  REDIS_URL: process.env.REDIS_URL,
  CUBEJS_DB_SSL: envBoolWithDefault('CUBEJS_DB_SSL', false),
  CUBEJS_DB_SSL_REJECT_UNAUTHORIZED: envBoolWithDefault('CUBEJS_DB_SSL_REJECT_UNAUTHORIZED', false),
  NODE_ENV: process.env.NODE_ENV,
  CUBEJS_CACHE_AND_QUEUE_DRIVER: process.env.CUBEJS_CACHE_AND_QUEUE_DRIVER,
  CUBEJS_REDIS_SENTINEL: process.env.CUBEJS_REDIS_SENTINEL,
  REDIS_PASSWORD: process.env.REDIS_PASSWORD,
  REDIS_TLS: envBoolWithDefault('REDIS_TLS', false),
  CUBEJS_REDIS_USE_IOREDIS_DEBUG: envBoolWithDefault('CUBEJS_REDIS_USE_IOREDIS_DEBUG', false)
};
