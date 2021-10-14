declare module 'index.node' {
    type AsyncChannel = {};
    function channel_resolve(channel: AsyncChannel, result: string): void;
    function channel_reject(channel: AsyncChannel): void;
}