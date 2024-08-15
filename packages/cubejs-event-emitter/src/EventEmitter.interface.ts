export type EventEmitterType = 'redis' | 'memory';

export type EventEmitterOptions = {
    type: EventEmitterType
};

export interface EventEmitterInterface {
    on (event: string, listener: (args: any) => void): this
    emit (event: string, ...args: any): boolean
}
