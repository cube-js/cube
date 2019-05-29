const jwt = require('jsonwebtoken');
require('dotenv').config();
const chalk = require('chalk');

const defaultExpiry = '30 days';

const parsePayload = (payloadArray = []) => {
  const result = {};

  payloadArray.forEach((entry = '') => {
    const [key, value] = entry.split('=');
    if (key && value) result[key] = value;
  });

  return result;
};

const token = async (options = {}) => {
  const { expiry = defaultExpiry, secret = process.env.CUBEJS_API_SECRET } = options;
  if (!secret) throw new Error('No app secret found').message;

  const extraOptions = {};
  if (expiry !== "0") extraOptions.expiresIn = expiry;

  const payload = parsePayload(options.payload);

  console.log("Generating Cube.js JWT token");
  console.log("Expires in: ", chalk.green(expiry));
  console.log("Payload: ", chalk.green(JSON.stringify(payload)));
  console.log("");

  const signedToken = jwt.sign(payload, secret, extraOptions);
  console.log(signedToken);
  return signedToken;
};

const collect = (val, memo) => [val, ...memo];

exports.token = token;
exports.defaultExpiry = defaultExpiry;
exports.collect = collect;
