export type AsyncChannel = {};

export declare function channel_resolve(channel: AsyncChannel, result: string): void;
export declare function channel_reject(channel: AsyncChannel): void;

export type InterfaceOption = {

};

export declare function registerInterface(
    options: InterfaceOption,
    transport_load: (extra: any, channel: Channel) => void,
    transport_meta: (extra: any, channel: Channel) => void,
): Promise<void>;
