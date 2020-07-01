import { DeclarationReflection, ReflectionKind } from 'typedoc';

export function ifParentIsModule(this: DeclarationReflection, truthy: boolean, options: any) {
  const parentIsModule = this.parent && this.parent.kind === ReflectionKind.Module;
  if (parentIsModule && truthy) {
    return options.fn(this);
  }
  return !parentIsModule && !truthy ? options.fn(this) : options.inverse(this);
}
