import fs from 'fs';
import path from 'path';

export interface BaseMeta {
    // postgres or mysql
    protocol: string,
    // always sql
    apiType: string,
    // Application name, for example Metabase
    appName?: string,
}

export interface LoadRequestMeta extends BaseMeta {
    // Security Context switching
    changeUser?: string,
}

export interface Request<Meta> {
    id: string,
    meta: Meta,
}

export interface CheckAuthPayload {
    request: Request<undefined>,
    user: string|null
}

export interface LoadPayload {
    request: Request<LoadRequestMeta>,
    user: string,
    query: any,
}

export interface MetaPayload {
    request: Request<undefined>,
    user: string|null
}

export type SQLInterfaceOptions = {
    port?: number,
    pgPort?: number,
    nonce?: string,
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

    throw new Error(
      `Unable to load @cubejs-backend/native, probably your system (${process.arch}-${process.platform}) with Node.js ${process.version} is not supported.`
    );
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

            if (process.env.CUBEJS_NATIVE_INTERNAL_DEBUG) {
                console.debug("[js] channel.resolve", {
                    result
                });
            }

            if (!result) {
                channel.resolve("");
            } else {
                channel.resolve(JSON.stringify(result));
            }
          } catch (e: any) {
            channel.reject(e.message || 'Unknown JS exception');

            // throw e;

            console.debug("[js] channel.reject", {
                e
            });
          }
    };
};

type LogLevel = 'error' | 'warn' | 'info' | 'debug' | 'trace';

export const setupLogger = (logger: (extra: any) => unknown, logLevel: LogLevel): void => {
    const native = loadNative();
    native.setupLogger({logger: wrapNativeFunctionWithChannelCallback(logger), logLevel});
}

export type SqlInterfaceInstance = { __typename: 'sqlinterfaceinstance' };

export const registerInterface = async (options: SQLInterfaceOptions): Promise<SqlInterfaceInstance> => {
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

export const shutdownInterface = async (instance: SqlInterfaceInstance): Promise<void> => {
    const native = loadNative();

    await native.shutdownInterface(instance);

    await new Promise((resolve) => setTimeout(resolve, 2000));
}
