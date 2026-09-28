import { CompilerInterface } from './PrepareCompiler';

export class ViewCompilationGate implements CompilerInterface {
  private shouldCompile: boolean = false;

  public compile(cubes: any[]) {
    // When developing Data Access Policies feature, we've come across a
    // limitation that Cube members can't be referenced in access policies defined on Views,
    // because views aren't (yet) compiled at the time of access policy evaluation.
    // To work around this limitation and additional compilation pass is necessary,
    // however it comes with a significant performance penalty.
    // This gate check whether the data model contains views with access policies,
    // and only then allows the additional compilation pass.
    //
    // Check out the DataSchemaCompiler.ts to see how this gate is used.
    if (this.viewsHaveAccessPolicies(cubes)) {
      this.shouldCompile = true;
    }
  }

  private viewsHaveAccessPolicies(cubes: any[]) {
    return cubes.some(c => c.isView && c.accessPolicy);
  }

  public shouldCompileViews() {
    return this.shouldCompile;
  }
}
