import * as path from 'path';
import * as fs from 'fs-extra';
import { Renderer } from 'typedoc';
import { Converter } from 'typedoc/dist/lib/converter';
import { Component, ConverterComponent } from 'typedoc/dist/lib/converter/components';

@Component({ name: 'markdown' })
export class MarkdownPlugin extends ConverterComponent {
  initialize() {
    this.listenTo(this.owner, {
      [Converter.EVENT_BEGIN]: this.onBegin,
      [Converter.EVENT_RESOLVE_BEGIN]: this.onResolveBegin,
      [Converter.EVENT_END]: this.onEnd,
    });
  }

  /**
   * Overide the default assets for any custom themes to inherit
   */
  onBegin() {
    Renderer.getDefaultTheme = () => path.join(__dirname, 'resources');
  }

  /**
   * Read the theme option and load the paths of any recognised built in themes
   * Otherwise pass the path through to the Renderer
   */
  onResolveBegin() {
    const options = this.application.options;
    const theme = (options.getValue('platform') as string) || (options.getValue('theme') as string);

    // if the theme is 'default' or 'markdown' load the base markdown theme
    if (theme === 'default' || theme === 'markdown') {
      options.setValue('theme', path.join(__dirname));
    }

    // load any built in sub themes
    const subThemes = ['docusaurus', 'docusaurus2', 'vuepress', 'gitbook', 'bitbucket'];
    if (subThemes.includes(theme)) {
      options.setValue('theme', path.join(__dirname, 'subthemes', theme));
    }

    options.setValue('out', options.getValue('out') + '/__tmp__');
  }

  onEnd() {
    const { options } = this.application;
    const tmpOut = options.getValue('out');
    
    setImmediate(() => {
      if (fs.existsSync(tmpOut)) {
        fs.readdirSync(tmpOut).map((fileName) => {
          const pathArr = tmpOut.split('/');
          pathArr.splice(-1, 1);
          const out = path.join(...pathArr);
          const currentPath = path.join(out, fileName);
 
          fs.copyFileSync(path.join(tmpOut, fileName), currentPath);
        });
        
        fs.removeSync(tmpOut);
      }
    })
  }
}
