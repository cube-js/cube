import fs from 'fs';
import path from 'path';

export interface CheckAuthPayload {
    authorization: string
}

export interface LoadPayload {
    authorization: string,
    request_id: string,
    query: any
}

export interface MetaPayload {
    authorization: string
}

export type SQLInterfaceOptions = {
    port?: number,
    checkAuth: (payload: CheckAuthPayload) => unknown | Promise<unknown>,
    load: (payload: LoadPayload) => unknown | Promise<unknown>,
    meta: (payload: MetaPayload) => unknown | Promise<unknown>,
};

function loadNative() {
    // Development version
    if (fs.existsSync(path.join(__dirname, '/../../index.node'))) {
        return require(path.join(__dirname, '/../../index.node'))
    }

    if (fs.existsSync(path.join(__dirname, '/../../native/index.node'))) {
        return require(path.join(__dirname, '/../../native/index.node'))
    }

    throw new Error('Unable to load @cubejs-backend/native, probably your system is not supported.');
}

export function isSupported(): boolean {
    return fs.existsSync(path.join(__dirname, '/../../index.node')) || fs.existsSync(path.join(__dirname, '/../../native/index.node'));
}

function wrapNativeFunctionWithChannelCallback(
    fn: (extra: any) => unknown | Promise<unknown>
) {
    const native = loadNative();

    return async (extra: any, channel: any) => {
        try {
            const result = await fn(JSON.parse(extra));
            channel.resolve(JSON.stringify(result));
          } catch (e: any) {
            channel.reject(e.message || 'Unknown JS exception');

            throw e;
          }
    };
};

type LogLevel = 'error' | 'warn' | 'info' | 'debug' | 'trace';

export const setLogLevel = (level: LogLevel): void => {
    const native = loadNative();
    native.setLogLevel(level);
};

export const registerInterface = async (options: SQLInterfaceOptions) => {
    if (typeof options !== 'object' && options == null) {
        throw new Error('Argument options must be an object');
    }

    if (typeof options.checkAuth != 'function') {
        throw new Error('options.checkAuth must be a function');
    }

    if (typeof options.load != 'function') {
        throw new Error('options.load must be a function');
    }

    if (typeof options.meta != 'function') {
        throw new Error('options.meta must be a function');
    }

    const native = loadNative();
    return native.registerInterface({
        ...options,
        checkAuth: wrapNativeFunctionWithChannelCallback(options.checkAuth),
        load: wrapNativeFunctionWithChannelCallback(options.load),
        meta: wrapNativeFunctionWithChannelCallback(options.meta),
    });
};
