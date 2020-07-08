import { DeclarationReflection, ReflectionKind } from 'typedoc';
import { heading } from './heading';

export function memberTitle(this: DeclarationReflection) {
  if (this.parent?.kindOf(ReflectionKind.Enum)) {
    return '';
  }

  const md = [];

  let headingLevel = 3;
  if (!(this as any).stickToParent) {
    if (this.parent?.kindOf(ReflectionKind.Module)) {
      headingLevel = 2;

      if (this.kind === ReflectionKind.TypeAlias) {
        headingLevel = 4;
      }
    }
  }

  md.push(heading(headingLevel));
  md.push(this.name);
  return md.join(' ');
}
