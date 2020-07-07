import { ReflectionKind, Reflection, ContainerReflection, DeclarationReflection } from 'typedoc';
import { Component } from 'typedoc/dist/lib/utils';
import { Converter, Context } from 'typedoc/dist/lib/converter';
import { SourceDirectory, ReflectionGroup, Comment } from 'typedoc/dist/lib/models';
import { ConverterComponent } from 'typedoc/dist/lib/converter/components';

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
        comment.tags = (comment.tags || []).filter((tag) => tag.tagName !== 'order');
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

  /**
   * Create a grouped representation of the given list of reflections.
   *
   * Reflections are grouped by kind and sorted by weight and name.
   *
   * @param reflections  The reflections that should be grouped.
   * @returns An array containing all children of the given reflection grouped by their kind.
   */
  static getReflectionGroups(reflections: Reflection[]): ReflectionGroup[] {
    const groups: ReflectionGroup[] = [];
    reflections.forEach((child) => {
      for (let i = 0; i < groups.length; i++) {
        const group = groups[i];
        if (group.kind !== child.kind) {
          continue;
        }

        group.children.push(child);
        return;
      }

      const group = new ReflectionGroup(CubejsGroupPlugin.getKindPlural(child.kind), child.kind);
      group.children.push(child);
      groups.push(group);
    });

    groups.forEach((group) => {
      let someExported = false,
        allInherited = true,
        allPrivate = true,
        allProtected = true,
        allExternal = true;
      group.children.forEach((child) => {
        someExported = child.flags.isExported || someExported;
        allPrivate = child.flags.isPrivate && allPrivate;
        allProtected = (child.flags.isPrivate || child.flags.isProtected) && allProtected;
        allExternal = child.flags.isExternal && allExternal;

        if (child instanceof DeclarationReflection) {
          allInherited = !!child.inheritedFrom && allInherited;
        } else {
          allInherited = false;
        }
      });

      group.someChildrenAreExported = someExported;
      group.allChildrenAreInherited = allInherited;
      group.allChildrenArePrivate = allPrivate;
      group.allChildrenAreProtectedOrPrivate = allProtected;
      group.allChildrenAreExternal = allExternal;
    });

    return groups;
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
