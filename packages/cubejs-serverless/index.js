const ProviderToHandlerPackage = {
  aws: '@cubejs-backend/serverless-aws',
  google: '@cubejs-backend/serverless-google'
};

// bump commit

const platform = process.env.CUBEJS_SERVERLESS_PLATFORM || process.env.SERVERLESS_EXPRESS_PLATFORM || 'aws';
const handlerPackage = ProviderToHandlerPackage[platform];

if (!handlerPackage) {
  throw new Error(`Handler not found for ${platform} platform`);
}

// eslint-disable-next-line import/no-dynamic-require
const HandlerClass = require(handlerPackage);

module.exports = new HandlerClass();
