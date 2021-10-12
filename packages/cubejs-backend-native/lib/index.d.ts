export type AsyncChannel = {};

export declare function channel_resolve(channel: AsyncChannel, result: string): void;
export declare function channel_reject(channel: AsyncChannel): void;

export type InterfaceOption = {
    checkAuth: (extra: any, channel: AsyncChannel) => void | Promise<void>,
    load: (extra: any, channel: AsyncChannel) => void | Promise<void>,
    meta: (extra: any, channel: AsyncChannel) => void | Promise<void>,
};

export declare function registerInterface(options: InterfaceOption): Promise<void>;
