import R from 'ramda';
import { DynamicReference } from '../compiler/DynamicReference';
import { AbstractExtension } from './extension.abstract';

export class Reflection extends AbstractExtension {
  public dynRef = (...args) => {
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
