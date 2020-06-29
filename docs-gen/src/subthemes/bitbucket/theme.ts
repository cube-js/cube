import { Reflection } from 'typedoc/dist/lib/models';
import { Renderer } from 'typedoc/dist/lib/output/renderer';

import MarkdownTheme from '../../theme';

export default class BitbucketTheme extends MarkdownTheme {
  constructor(renderer: Renderer, basePath: string) {
    super(renderer, basePath);
  }

  toAnchorRef(reflection: Reflection) {
    function parseAnchorRef(ref: string) {
      return ref.replace(/"/g, '').replace(/ /g, '-');
    }
    let anchorPrefix = '';
    reflection.flags.forEach(flag => (anchorPrefix += `${flag}-`));
    const prefixRef = parseAnchorRef(anchorPrefix);
    const reflectionRef = parseAnchorRef(reflection.name);
    const anchorRef = prefixRef + reflectionRef;
    return 'markdown-header-' + anchorRef.toLowerCase();
  }
}
