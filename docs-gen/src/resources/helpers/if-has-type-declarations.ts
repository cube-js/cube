import { ReflectionType, SignatureReflection } from 'typedoc/dist/lib/models';

export function ifHasTypeDeclarations(this: SignatureReflection, truthy: boolean, options: any) {
  const parameterDeclarations =
    this.parameters &&
    this.parameters.map(parameter => {
      const type = parameter.type as ReflectionType;
      return (
        parameter.type &&
        type.declaration &&
        ((type.declaration.children && type.declaration.children.length > 0) ||
          (type.declaration.signatures && type.declaration.signatures.length > 0))
      );
    });
  const hasTypeDeclarations = parameterDeclarations && parameterDeclarations.some(parameterDeclaration => parameterDeclaration);

  if (hasTypeDeclarations && truthy) {
    return options.fn(this);
  }
  return !hasTypeDeclarations && !truthy ? options.fn(this) : options.inverse(this);
}
