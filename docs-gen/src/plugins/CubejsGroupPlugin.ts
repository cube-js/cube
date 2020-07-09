import { ContainerReflection, DeclarationReflection, Reflection, ReflectionKind } from 'typedoc';
import { Context, Converter } from 'typedoc/dist/lib/converter';
import { ConverterComponent } from 'typedoc/dist/lib/converter/components';
import { Comment, ReferenceType, ReflectionGroup, SourceDirectory } from 'typedoc/dist/lib/models';
import { Component } from 'typedoc/dist/lib/utils';

const STICKY_TAG_NAME = 'stickytypes';

@Component({ name: 'cubejs-group' })
export default class CubejsGroupPlugin extends ConverterComponent {
  /**
   * Define the sort order of reflections.
   */
  static WEIGHTS = [
    ReflectionKind.Class,
    ReflectionKind.Function,
    ReflectionKind.Global,
    ReflectionKind.Module,
    ReflectionKind.Namespace,
    ReflectionKind.Interface,
    ReflectionKind.Enum,
    ReflectionKind.EnumMember,
    ReflectionKind.TypeAlias,

    ReflectionKind.Constructor,
    ReflectionKind.Event,
    ReflectionKind.Property,
    ReflectionKind.Variable,
    ReflectionKind.Accessor,
    ReflectionKind.Method,
    ReflectionKind.ObjectLiteral,

    ReflectionKind.Parameter,
    ReflectionKind.TypeParameter,
    ReflectionKind.TypeLiteral,
    ReflectionKind.CallSignature,
    ReflectionKind.ConstructorSignature,
    ReflectionKind.IndexSignature,
    ReflectionKind.GetSignature,
    ReflectionKind.SetSignature,
  ];

  /**
   * Define the singular name of individual reflection kinds.
   */
  static SINGULARS = (function () {
    const singulars = {};
    singulars[ReflectionKind.Enum] = 'Enumeration';
    singulars[ReflectionKind.EnumMember] = 'Enumeration member';
    return singulars;
  })();

  /**
   * Define the plural name of individual reflection kinds.
   */
  static PLURALS = (function () {
    const plurals = {};
    plurals[ReflectionKind.Class] = 'Classes';
    plurals[ReflectionKind.Property] = 'Properties';
    plurals[ReflectionKind.Enum] = 'Enumerations';
    plurals[ReflectionKind.EnumMember] = 'Enumeration members';
    plurals[ReflectionKind.TypeAlias] = 'Type aliases';
    return plurals;
  })();

  static orderByName = new Map<string, number>();

  /**
   * Create a new CubejsGroupPlugin instance.
   */
  initialize() {
    this.listenTo(
      this.owner,
      {
        [Converter.EVENT_RESOLVE]: this.onResolve,
        [Converter.EVENT_RESOLVE_END]: this.onEndResolve,
      },
      null,
      1
    );
  }

  private populateOrder(children: Reflection[] = []) {
    const MAGIC = 100_000;

    function findOrderAndRemove(comment?: Comment) {
      const orderTag = (comment?.tags || []).find((tag) => tag.tagName === 'order');

      if (orderTag) {
        comment.removeTags('order');
        // CommentPlugin.removeTags(comment, 'order');
        return parseInt(orderTag.text, 10) - MAGIC;
      }
    }

    function getOrder(reflection: Reflection) {
      if (reflection.hasComment()) {
        return findOrderAndRemove(reflection.comment);
      } else if (reflection instanceof DeclarationReflection) {
        return findOrderAndRemove(reflection.signatures?.[0]?.comment);
      }

      return 0;
    }

    children.forEach((reflection) => {
      if (!CubejsGroupPlugin.orderByName.has(reflection.name)) {
        CubejsGroupPlugin.orderByName.set(reflection.name, getOrder(reflection) || 0);
      }
    });
  }

  private onResolve(context: Context, reflection: ContainerReflection) {
    reflection.kindString = CubejsGroupPlugin.getKindSingular(reflection.kind);

    if (reflection.children && reflection.children.length > 0) {
      this.populateOrder(reflection.children);
      reflection.children.sort(CubejsGroupPlugin.sortCallback);
      reflection.groups = CubejsGroupPlugin.getReflectionGroups(reflection.children);
    }
  }

  /**
   * Triggered when the converter has finished resolving a project.
   *
   * @param context  The context object describing the current state the converter is in.
   */
  private onEndResolve(context: Context) {
    function walkDirectory(directory: SourceDirectory) {
      directory.groups = CubejsGroupPlugin.getReflectionGroups(directory.getAllReflections());

      for (const key in directory.directories) {
        if (!directory.directories.hasOwnProperty(key)) {
          continue;
        }
        walkDirectory(directory.directories[key]);
      }
    }

    const project = context.project;
    if (project.children && project.children.length > 0) {
      this.populateOrder(project.children);
      project.children.sort(CubejsGroupPlugin.sortCallback);
      project.groups = CubejsGroupPlugin.getReflectionGroups(project.children);
    }

    walkDirectory(project.directory);
    project.files.forEach((file) => {
      file.groups = CubejsGroupPlugin.getReflectionGroups(file.reflections);
    });
  }

  private static getStickyTypes(reflection: DeclarationReflection): string[] {
    const typeNames = [];
    let comment: Comment;

    if (reflection.comment?.getTag(STICKY_TAG_NAME) != null) {
      comment = reflection.comment;
    }

    if (!comment) {
      reflection.signatures?.some((sig) => {
        if (sig.comment?.getTag(STICKY_TAG_NAME) != null) {
          comment = sig.comment;
          return true;
        }
        return false;
      });
    }

    if (comment) {
      const { text } = comment.getTag(STICKY_TAG_NAME);
      comment.removeTags(STICKY_TAG_NAME);
      // CommentPlugin.removeTags(comment, STICKY_TAG_NAME);
      
      if (text.trim()) {
        return text.split(',').map((name) => name.trim());
      }
      
      reflection.signatures?.forEach((sig) => {
        // Parameter types
        sig.parameters?.forEach((param) => {
          if (param.type instanceof ReferenceType) {
            typeNames.push(param.type.name);
          }
        });

        // Return type
        if (sig.type && sig.type instanceof ReferenceType) {
          typeNames.push(sig.type.name);
        }
      });

      reflection.extendedTypes?.forEach((type: ReferenceType) => {
        type.typeArguments?.forEach((typeArgument: any) => {
          typeArgument.name && typeNames.push(typeArgument.name);
        });
      });
    }
    
    return typeNames;
  }

  /**
   * Create a grouped representation of the given list of reflections.
   *
   * Reflections are grouped by kind and sorted by weight and name.
   *
   * @param reflections  The reflections that should be grouped.
   * @returns An array containing all children of the given reflection grouped by their kind.
   */
  static getReflectionGroups(reflections: Reflection[]): ReflectionGroup[] {
    const groups = new Map<ReflectionKind, ReflectionGroup>();
    const handledReflections = new Set<string>();
    const reflectionByName = new Map<string, Reflection>();

    reflections.forEach((child) => reflectionByName.set(child.name, child));

    reflections.forEach((child) => {
      if (handledReflections.has(child.name)) {
        return;
      }

      let typeNames = [];
      if (child instanceof DeclarationReflection) {
        typeNames = CubejsGroupPlugin.getStickyTypes(child);
      }

      if (!groups.has(child.kind)) {
        groups.set(child.kind, new ReflectionGroup(CubejsGroupPlugin.getKindPlural(child.kind), child.kind));
      }

      groups.get(child.kind).children.push(child);

      typeNames.forEach((name) => {
        if (reflectionByName.has(name)) {
          (reflectionByName.get(name) as any).stickToParent = child.name;
          groups.get(child.kind).children.push(reflectionByName.get(name));
          handledReflections.add(name);
        }
      });
    });

    return [...groups.values()];
  }

  /**
   * Transform the internal typescript kind identifier into a human readable version.
   *
   * @param kind  The original typescript kind identifier.
   * @returns A human readable version of the given typescript kind identifier.
   */
  private static getKindString(kind: ReflectionKind): string {
    let str = ReflectionKind[kind];
    str = str.replace(/(.)([A-Z])/g, (m, a, b) => a + ' ' + b.toLowerCase());
    return str;
  }

  /**
   * Return the singular name of a internal typescript kind identifier.
   *
   * @param kind The original internal typescript kind identifier.
   * @returns The singular name of the given internal typescript kind identifier
   */
  static getKindSingular(kind: ReflectionKind): string {
    if (CubejsGroupPlugin.SINGULARS[kind]) {
      return CubejsGroupPlugin.SINGULARS[kind];
    } else {
      return CubejsGroupPlugin.getKindString(kind);
    }
  }

  /**
   * Return the plural name of a internal typescript kind identifier.
   *
   * @param kind The original internal typescript kind identifier.
   * @returns The plural name of the given internal typescript kind identifier
   */
  static getKindPlural(kind: ReflectionKind): string {
    if (CubejsGroupPlugin.PLURALS[kind]) {
      return CubejsGroupPlugin.PLURALS[kind];
    } else {
      return this.getKindString(kind) + 's';
    }
  }

  /**
   * Callback used to sort reflections by weight defined by ´CubejsGroupPlugin.WEIGHTS´ and name.
   *
   * @param a The left reflection to sort.
   * @param b The right reflection to sort.
   * @returns The sorting weight.
   */
  static sortCallback(a: Reflection, b: Reflection): number {
    const aWeight = CubejsGroupPlugin.orderByName.get(a.name) || CubejsGroupPlugin.WEIGHTS.indexOf(a.kind);
    const bWeight = CubejsGroupPlugin.orderByName.get(b.name) || CubejsGroupPlugin.WEIGHTS.indexOf(b.kind);

    if (aWeight === bWeight) {
      if (a.flags.isStatic && !b.flags.isStatic) {
        return 1;
      }
      if (!a.flags.isStatic && b.flags.isStatic) {
        return -1;
      }
      if (a.name === b.name) {
        return 0;
      }
      return a.name > b.name ? 1 : -1;
    } else {
      return aWeight - bWeight;
    }
  }
}
