import { DeclarationReflection, ReflectionKind } from 'typedoc';

export function ifIsLiteralType(this: DeclarationReflection, truthy: boolean, options: any) {
  const isLiteralType = this.kind === ReflectionKind.ObjectLiteral || this.kind === ReflectionKind.TypeLiteral;
  if (isLiteralType && truthy) {
    return options.fn(this);
  }
  return !isLiteralType && !truthy ? options.fn(this) : options.inverse(this);
}
