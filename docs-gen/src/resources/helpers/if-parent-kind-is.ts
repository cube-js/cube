import { DeclarationReflection } from 'typedoc';

export function ifParentKindIs(this: DeclarationReflection, kindString: string, truthy: boolean = true, options: any) {
  const equals = this.parent && this.parent.kindString === kindString;
  
  return !equals && !truthy ? options.fn(this) : options.inverse(this);
}
