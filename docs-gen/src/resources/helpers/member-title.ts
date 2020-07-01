import { DeclarationReflection, ReflectionKind } from 'typedoc';

export function memberTitle(this: DeclarationReflection) {
  if (this.parent?.kind === ReflectionKind.Enum) {
    return '';
  }

  const md = [];

  if (this.flags) {
    md.push(this.flags.map((flag) => `\`${flag}\``).join(' '));
  }
  md.push(this.name);
  return md.join(' ');
}
