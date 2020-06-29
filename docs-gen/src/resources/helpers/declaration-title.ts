import { DeclarationReflection, ReflectionKind } from 'typedoc';
import { heading } from './heading';
import { memberSymbol } from './member-symbol';
import { type } from './type';

export function declarationTitle(this: DeclarationReflection, showSymbol: boolean) {
  const isOptional = this.flags.map(flag => flag).includes('Optional');

  const md = [];

  if (this.parent && this.parent.kind !== ReflectionKind.ObjectLiteral && this.kind === ReflectionKind.ObjectLiteral) {
    md.push(heading(3));
  }

  if (showSymbol) {
    md.push(memberSymbol.call(this));
  }

  md.push(`**${this.name}**${isOptional ? '? ' : ''}:`);

  if (this.type) {
    md.push(`*${type.call(this.type)}*`);
  }
  if (this.defaultValue) {
    md.push(`= ${this.defaultValue}`);
  }
  return md.join(' ');
}
