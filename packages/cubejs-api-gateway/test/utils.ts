// eslint-disable-next-line @typescript-eslint/no-unused-vars
import jwt, { SignOptions } from 'jsonwebtoken';

export function generateAuthToken(payload: object = {}, options?: SignOptions, secret: string = 'secret') {
  return jwt.sign(payload, secret, {
    expiresIn: '10000d',
    ...options,
  });
}
