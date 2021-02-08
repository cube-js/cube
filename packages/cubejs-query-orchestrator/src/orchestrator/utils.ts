/* eslint-disable no-restricted-syntax */
import * as querystring from 'querystring';

function parseHostPort(addr: string): { host: string, port: number } {
  if (addr.includes(':')) {
    const parts = addr.split(':');

    if (parts.length === 2) {
      return {
        host: parts[0],
        port: parseInt(parts[1], 10),
      };
    }

    throw new Error(
      `Unsupported host:port part inside REDIS_URL: ${addr}`
    );
  }

  return {
    host: addr,
    port: 6379,
  };
}

function parseAddrPart(addr: string): { host: string, port: number, username?: string, password?: string } {
  if (addr.includes('@')) {
    const parts = addr.split('@');
    if (parts.length !== 2) {
      throw new Error(
        `Unsupported host part inside REDIS_URL: ${addr}`
      );
    }

    const credentials = parts[0].split(':');
    if (credentials.length !== 2) {
      throw new Error(
        `Unsupported credentials part inside REDIS_URL: ${addr}`
      );
    }

    return {
      username: credentials[0],
      password: credentials[1],
      ...parseHostPort(parts[1]),
    };
  }

  return parseHostPort(addr);
}

export interface RedisParsedResult {
  ssl: boolean,
  password?: string,
  username?: string,
  host?: string,
  port?: number,
  /**
   * Local domain socket path. If set the port, host and family will be ignored.
   */
  path?: string,
  sentinels?: { host: string, port: number }[],
  db?: number,
  name?: string,
}

function parseHostPartBasic(addUrl: string, result: RedisParsedResult) {
  const { host, port, password, username } = parseAddrPart(addUrl);

  result.password = password;
  result.username = username;
  result.host = host;
  result.port = port;

  return result;
}

function parseHostPartSentinel(addUrl: string, result: RedisParsedResult) {
  const servers = addUrl.split(',');

  result.sentinels = servers.map((addr) => parseHostPort(addr));

  return result;
}

function parseUrl(
  url: string,
  result: RedisParsedResult,
  parseAddPartFn: (addr: string, result: RedisParsedResult) => RedisParsedResult,
): RedisParsedResult {
  if (url.includes('/')) {
    const parts = url.split('/');
    if (parts.length === 2) {
      result.db = parseInt(<string>parts[1], 10);
    } else if (parts.length === 3) {
      result.name = <string>parts[1];
      result.db = parseInt(<string>parts[2], 10);
    } else {
      throw new Error(
        `Unsupported REDIS_URL: "${url}"`
      );
    }

    return parseAddPartFn(parts[0], result);
  }

  return parseAddPartFn(url, result);
}

function parseUnixUrl(url: string, result: RedisParsedResult) {
  if (url.includes('?')) {
    const parts = url.split('?');
    if (parts.length === 2) {
      const query = querystring.parse(parts[1]);

      for (const key of Object.keys(query)) {
        switch (key.toLowerCase()) {
          case 'db':
            result.db = parseInt(<string>query[key], 10);
            break;
          default:
            break;
        }
      }

      return {
        ...result,
        path: parts[0],
      };
    }

    throw new Error(
      `Unsupported REDIS_URL: "${url}"`
    );
  }

  result.path = url;

  return result;
}

export function parseRedisUrl(url: Readonly<string>): RedisParsedResult {
  const result: RedisParsedResult = {
    username: undefined,
    password: undefined,
    host: undefined,
    port: undefined,
    ssl: false,
    sentinels: undefined,
    db: undefined,
    name: undefined,
  };

  if (!url) {
    return result;
  }

  if (url.startsWith('redis://')) {
    return parseUrl(url.substr('redis://'.length), result, parseHostPartBasic);
  }

  if (url.startsWith('rediss://')) {
    result.ssl = true;

    return parseUrl(url.substr('rediss://'.length), result, parseHostPartBasic);
  }

  if (url.startsWith('redis+sentinel://')) {
    return parseUrl(url.substr('redis+sentinel://'.length), result, parseHostPartSentinel);
  }

  if (url.startsWith('unix://')) {
    return parseUnixUrl(url.substr('unix://'.length), result);
  }

  return parseUrl(url, result, parseHostPartBasic);
}
