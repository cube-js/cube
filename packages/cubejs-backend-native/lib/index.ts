import fs from 'fs';
import path from 'path';

export type SQLInterfaceOptions = {
    checkAuth: (payload: string) => void | Promise<void>,
    load: (extra: any) => void | Promise<void>,
    meta: (extra: any) => void | Promise<void>,
};

function loadNative() {
    // Development version
    if (fs.existsSync(path.resolve('index.node'))) {
        return require('../../index.node')
    }

    if (fs.existsSync(path.resolve('native/index.node'))) {
        return require('../../native/index.node')
    }

    throw new Error('Unable to load @cubejs-backend/native, probably your system is not supported.');
}

export function isSupported(): boolean {
    return fs.existsSync(path.resolve('index.node')) || fs.existsSync(path.resolve('native/index.node'));
}

function wrapNativeFunctionWithChannelCallback(
    fn: (extra: any) => void | Promise<void>
) {
    const native = loadNative();

    return async (extra: any, channel: any) => {
        try {
            const result = await fn(JSON.parse(extra));
            native.channel_resolve(channel, JSON.stringify(result));
          } catch (e) {
            native.channel_reject(channel);

            throw e;
          }
    };
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
        checkAuth: wrapNativeFunctionWithChannelCallback(options.checkAuth),
        load: wrapNativeFunctionWithChannelCallback(options.load),
        meta: wrapNativeFunctionWithChannelCallback(options.meta),
    });
};
