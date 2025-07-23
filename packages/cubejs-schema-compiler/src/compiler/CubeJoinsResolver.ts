import { CompilerInterface } from './PrepareCompiler';
import { CubeDefinitionExtended, CubeSymbols, JoinDefinition } from './CubeSymbols';
import type { ErrorReporter } from './ErrorReporter';

export class CubeJoinsResolver extends CubeSymbols implements CompilerInterface {
  // key: cubeName with joins defined
  private cubeJoins: Record<string, JoinDefinition[]>;

  // key: cubeName with joins defined
  // 1st level value: join alias
  // 2nd level value: join definition
  public cubeJoinAliases: Record<string, Record<string, JoinDefinition>>;

  // key: cubeName with joins defined
  // 1st level value: target cube name
  // 2nd level value: join definition
  private cubeJoinTargets: Record<string, Record<string, JoinDefinition>>;

  public constructor(evaluateViews = false) {
    super(evaluateViews);
    this.cubeJoins = {};
    this.cubeJoinAliases = {};
    this.cubeJoinTargets = {};
  }

  public compile(cubes: CubeDefinitionExtended[], errorReporter: ErrorReporter) {
    super.compile(cubes, errorReporter);

    this.cubeList.forEach(cube => {
      if (!cube.joins?.length) {
        return;
      }

      this.cubeJoins[cube.name] = this.cubeJoins[cube.name] || [];
      this.cubeJoinAliases[cube.name] = this.cubeJoinAliases[cube.name] || {};
      this.cubeJoinTargets[cube.name] = this.cubeJoinTargets[cube.name] || {};

      const er = errorReporter.inContext(`${cube.name} cube`);

      cube.joins.forEach(join => {
        this.cubeJoins[cube.name].push(join);
        this.cubeJoinTargets[cube.name][join.name] = join;

        if (join.alias) {
          if (this.cubeJoinAliases[cube.name][join.alias]) {
            er.error(
              `Join alias "${join.alias}" is already defined in cube "${cube.name}".`,
              cube.fileName
            );

            return;
          }
          this.cubeJoinAliases[cube.name][join.alias] = join;
        }
      });
    });
  }

  public resolveSymbol(cubeName: string | null, name: string) {
    if (this.isCurrentCube(name) && !cubeName) {
      return null;
    }

    if (cubeName && this.cubeJoinAliases[cubeName]?.[name]) {
      // TODO: Write a full implementation
      // Some kind of proxy like in symbols

      return super.resolveSymbol(cubeName, this.cubeJoinAliases[cubeName]?.[name].name);
    }

    return super.resolveSymbol(cubeName, name);
  }
}
