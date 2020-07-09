import { camelize, dasherize, underscore } from 'inflection';
import { DeclarationReflection, Reflection, ReflectionKind } from 'typedoc';
import { Context, Converter } from 'typedoc/dist/lib/converter';
import { ConverterComponent } from 'typedoc/dist/lib/converter/components';
import { Comment } from 'typedoc/dist/lib/models/comments';
import { Component } from 'typedoc/dist/lib/utils';

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
    }

    return `[${name}](${LinkPlugin.anchorName(link)})`;
  }

  private static replaceAnnotations(comment: Comment, reflections: Reflection[]) {
    comment.shortText = comment.shortText.replace(/{@see\s([^}]*)}/g, (_, name) => {
      const reflection = reflections.find((reflection) => reflection.name === name);
      return this.toLink(name, reflection);
    });
  }

  initialize() {
    this.listenTo(this.owner, {
      [Converter.EVENT_RESOLVE_END]: this.onEndResolve,
    });
  }

  onEndResolve(context: Context) {
    const reflections = Object.values(context.project.reflections);

    reflections.forEach((reflection) => {
      if (reflection instanceof DeclarationReflection) {
        reflection.signatures?.forEach((sig) => {
          sig.comment && LinkPlugin.replaceAnnotations(sig.comment, reflections);
        });
      }
    });
  }
}
