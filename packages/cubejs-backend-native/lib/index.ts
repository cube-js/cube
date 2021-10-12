export type SQLInterfaceOptions = {
    checkAuth: (payload: string) => void | Promise<void>,
    load: (extra: any) => void | Promise<void>,
    meta: (extra: any) => void | Promise<void>,
};

function wrapNativeFunctionWithChannelCallback(
    fn: (extra: any) => void | Promise<void>
) {
    const native = require('../../index.node');

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

    const native = require('../../index.node');
    return native.registerInterface({
        checkAuth: wrapNativeFunctionWithChannelCallback(options.checkAuth),
        load: wrapNativeFunctionWithChannelCallback(options.load),
        meta: wrapNativeFunctionWithChannelCallback(options.meta),
    });
};
