import { EventEmitter } from 'events';
import { EventEmitterInterface, EventEmitterOptions } from './EventEmitter.interface';

export interface DefaultEventEmitterOptions extends EventEmitterOptions {
  type: 'memory';
}

export class DefaultEventEmitter extends EventEmitter implements EventEmitterInterface {}
