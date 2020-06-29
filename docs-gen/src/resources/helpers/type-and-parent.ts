import { SignatureReflection } from 'typedoc';
import { ArrayType, ReferenceType } from 'typedoc/dist/lib/models/types';

import MarkdownTheme from '../../theme';

export function typeAndParent(this: ArrayType | ReferenceType) {
  if (this instanceof ReferenceType && this.reflection) {
    const md = [];
    if (this.reflection instanceof SignatureReflection) {
      if (this.reflection.parent.parent.url) {
        md.push(
          `[${this.reflection.parent.parent.name}](${MarkdownTheme.handlebars.helpers.relativeURL(
            this.reflection.parent.parent.url,
          )})`,
        );
      } else {
        md.push(this.reflection.parent.parent.name);
      }
    } else {
      if (this.reflection.parent.url) {
        md.push(
          `[${this.reflection.parent.name}](${MarkdownTheme.handlebars.helpers.relativeURL(
            this.reflection.parent.url,
          )})`,
        );
      } else {
        md.push(this.reflection.parent.name);
      }
      if (this.reflection.url) {
        md.push(`[${this.reflection.name}](${MarkdownTheme.handlebars.helpers.relativeURL(this.reflection.url)})`);
      } else {
        md.push(this.reflection.name);
      }
    }
    return md.join('.');
  }
  return 'void';
}
