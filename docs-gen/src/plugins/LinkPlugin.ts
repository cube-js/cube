import { camelize, dasherize, underscore } from 'inflection';
import { DeclarationReflection, Reflection, ReflectionKind, ParameterReflection, SignatureReflection } from 'typedoc';
import { Context, Converter } from 'typedoc/dist/lib/converter';
import { ConverterComponent } from 'typedoc/dist/lib/converter/components';
import { Comment } from 'typedoc/dist/lib/models/comments';
import { Component } from 'typedoc/dist/lib/utils';

const linkRegex = /{@see\s([^}]*)}/g;

@Component({ name: 'link' })
export class LinkPlugin extends ConverterComponent {
  static anchorName(link) {
    return (
      '#' +
      dasherize(underscore(link.replace(/[A-Z]{2,}(?=[A-Z])/, (v) => camelize(v.toLowerCase())).replace(/#/g, '-')))
    );
  }

  static toLink(name, reflection: Reflection | string) {
    let link = name;

    if (reflection instanceof Reflection) {
      if (reflection.kindOf(ReflectionKind.TypeAlias) && !(reflection as any).stickToParent) {
        link = `Types${name}`;
      }
      if ((reflection as any).stickToParent) {
        link = (reflection as any).stickToParent + name;
      }
    }

    return `[${name}](${LinkPlugin.anchorName(link)})`;
  }

  private static replaceAnnotations(comment: Comment, reflections: Reflection[]) {
    const replacer = (_, name) => {
      const reflection = reflections.find((reflection) => reflection.name === name);
      return this.toLink(name, reflection);
    }
    comment.text = comment.text.replace(linkRegex, replacer);
    comment.shortText = comment.shortText.replace(linkRegex, replacer);
  }

  initialize() {
    this.listenTo(this.owner, {
      [Converter.EVENT_RESOLVE_END]: this.onEndResolve,
    });
  }

  onEndResolve(context: Context) {
    const reflections = Object.values(context.project.reflections);
    
    reflections.forEach((reflection) => {
      reflection.comment && LinkPlugin.replaceAnnotations(reflection.comment, reflections);
      
      if (reflection instanceof DeclarationReflection) {
        reflection.signatures?.forEach((sig) => {
          sig.comment && LinkPlugin.replaceAnnotations(sig.comment, reflections);
        });
      }
      
      if (reflection instanceof SignatureReflection) {
        reflection.parameters?.forEach((param) => {
          param.comment && LinkPlugin.replaceAnnotations(param.comment, reflections);
        });
      }
    });
  }
}
