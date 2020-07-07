import { DeclarationReflection } from 'typedoc';
import { type } from './type';

export function ifIsFunctionType(this: DeclarationReflection, truthy: boolean, options: any) {
  const isFunctionType = type.call(this).toString() === 'function';

  if (isFunctionType && truthy) {
    return options.fn(this);
  }
  return !isFunctionType && !truthy ? options.fn(this) : options.inverse(this);
}
