import {
  DeclarationReflection,
} from 'typedoc';

export function memberSymbol(this: DeclarationReflection) {
  return '>';
}
