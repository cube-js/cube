import { DeclarationReflection, ReflectionKind } from 'typedoc';

export function ifParentIsObjectLiteral(this: DeclarationReflection, truthy: boolean, options: any) {
  const parentIsObjectLiteral = this.parent && this.parent.parent && this.parent.parent.kind === ReflectionKind.ObjectLiteral;
  if (parentIsObjectLiteral && truthy) {
    return options.fn(this);
  }
  return !parentIsObjectLiteral && !truthy ? options.fn(this) : options.inverse(this);
}
