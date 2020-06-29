import { DeclarationReflection, ReferenceType } from 'typedoc/dist/lib/models';

import { spaces } from './spaces';

export function hierachyLevel(this: ReferenceType) {
  const reflection = this.reflection as DeclarationReflection;
  const symbol = reflection && reflection.extendedTypes ? `${spaces(2)}↳` : '*';
  return symbol;
}
