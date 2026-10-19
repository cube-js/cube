import R from 'ramda';
import { DynamicReference } from '../compiler/DynamicReference';
import { AbstractExtension } from './extension.abstract';

export class Reflection extends AbstractExtension {
  // Reference names followed by a callback; R.dropLast can't split that tuple type.
  public dynRef = (...args: any[]) => {
    if (args.length < 2) {
      throw new Error('List of references and a function are expected in form: dynRef(\'ref\', (r) => (...))');
    }

    const references = R.dropLast(1, args);
    const fn = args[args.length - 1];

    if (typeof fn !== 'function') {
      throw new Error('Last argument should be a function');
    }

    return new DynamicReference(references, fn);
  };
}
