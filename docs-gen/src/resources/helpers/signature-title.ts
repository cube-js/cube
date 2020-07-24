import { SignatureReflection } from 'typedoc';

import { memberSymbol } from './member-symbol';
import { type } from './type';
import { ReflectionType } from 'typedoc/dist/lib/models';
import paramTypeToString from './param-type-to-string';

export function signatureTitle(this: SignatureReflection, showSymbol: boolean = false) {
  const md = [];

  if (showSymbol) {
    md.push(`${memberSymbol.call(this)} `);
  }

  // eg: `static`
  if (this.parent?.flags) {
    md.push(
      this.parent.flags
        .map((flag) => `\`${flag}\``)
        .join(' ')
        .toLowerCase()
    );
    md.push(' ');
  }

  if (this.name === '__get') {
    md.push(`**get ${this.parent.name}**`);
  } else if (this.name === '__set') {
    md.push(`**set ${this.parent.name}**`);
  } else if (this.name !== '__call') {
    md.push(`**${this.name}**`);
  }
  if (this.typeParameters) {
    md.push(`‹${this.typeParameters.map((typeParameter) => `**${typeParameter.name}**`).join(', ')}›`);
  }
  const params = this.parameters
    ? this.parameters
        .map((param) => {
          const paramsmd = [];
          if (param.flags.isRest) {
            paramsmd.push('...');
          }
          paramsmd.push(`**${param.name}`);
          if (param.flags.isOptional) {
            paramsmd.push('?');
          }
          paramsmd.push(`**: ${paramTypeToString(param)}`);
          return paramsmd.join('');
        })
        .join(', ')
    : '';
  md.push(`(${params})`);

  if (this.type) {
    md.push(!showSymbol ? ' =>' : ':');

    if (this.type instanceof ReflectionType && type.call(this.type).toString() === 'function') {
      const declarations = this.type.declaration.signatures?.map((sig) => signatureTitle.call(sig, false, true));
      md.push(declarations.join(' | ').replace(/\n/, ''));
    } else {
      md.push(` *${type.call(this.type)}*`);
    }
  }
  return md.join('') + '\n';
}
