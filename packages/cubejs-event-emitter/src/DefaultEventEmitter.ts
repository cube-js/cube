import { EventEmitter } from 'events';
import { EventEmitterInterface } from './EventEmitter.interface';

export class DefaultEventEmitter extends EventEmitter implements EventEmitterInterface {}
