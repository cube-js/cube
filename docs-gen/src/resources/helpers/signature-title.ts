import { SignatureReflection } from 'typedoc';

import { memberSymbol } from './member-symbol';
import { type } from './type';

export function signatureTitle(this: SignatureReflection, showSymbol: boolean = false, useArrow: boolean = false) {
  const md = [];

  if (showSymbol) {
    md.push(`${memberSymbol.call(this)} `);
  }

  // eg: `static`
  if (this.parent?.flags) {
    md.push(this.parent.flags.map((flag) => `\`${flag}\``).join(' ').toLowerCase());
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
          paramsmd.push(`**: ${type.call(param.type)}`);
          return paramsmd.join('');
        })
        .join(', ')
    : '';
  md.push(`(${params})`);
  if (this.type) {
    md.push(useArrow ? ' =>' : ':');
    md.push(` *${type.call(this.type)}*`);
  }
  return md.join('') + '\n';
}
