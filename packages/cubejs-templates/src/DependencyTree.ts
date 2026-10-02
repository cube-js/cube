import R from 'ramda';

export type Package = {
  name: string;
  version: string;
  installsTo: Record<string, string> | null;
  receives: Record<string, string>;
};

export type DependencyNode = {
  package: Package;
  children: DependencyNode[];
  // Instance returned by the template package's own module, assigned by AppContainer
  packageInstance?: any;
};

const indexByName = (packages: Package[]) => R.indexBy(R.prop('name'), packages);

export class DependencyTree {
  protected rootNode: DependencyNode | null = null;

  protected resolved: any[] = [];

  public constructor(private manifest: Record<string, unknown>, private templatePackages: string[]) {
    this.build(this.getRootNode());

    const diff = R.difference(templatePackages, this.resolved);
    if (diff.length) {
      throw new Error(`The following packages could not be resolved: ${diff.join(', ')}`);
    }
  }

  protected packages() {
    return <Package[]> this.manifest.packages;
  }

  public getRootNode(): DependencyNode {
    if (this.rootNode) {
      return this.rootNode;
    }

    const rootPackages = this.packages().filter((pkg) => pkg.installsTo == null);
    const root = rootPackages.find((pkg) => this.templatePackages.includes(pkg.name));

    if (!root) {
      throw new Error('root package not found');
    }

    this.resolved.push(root.name);

    this.rootNode = {
      package: root,
      children: [],
    };

    return this.rootNode;
  }

  protected packagesInstalledTo(name: string): Record<string, Package> {
    return indexByName(this.packages().filter((pkg) => (pkg.installsTo || {})[name]));
  }

  protected getChildren(pkg: Package): Package[] {
    const children: Package[] = [];

    Object.keys(pkg.receives || {}).forEach((receive) => {
      const currentPackages = this.packagesInstalledTo(receive);

      if (Object.keys(currentPackages || {}).length) {
        this.templatePackages.forEach((name) => {
          if (currentPackages[name]) {
            children.push(currentPackages[name]);
          }
        });
      }
    });

    return children;
  }

  protected build(node: DependencyNode) {
    if (!node) {
      return;
    }

    (this.getChildren(node.package) || []).forEach((child) => {
      const childNode: DependencyNode = {
        package: child,
        children: [],
      };
      node.children.push(childNode);
      this.resolved.push(child.name);
      this.build(childNode);
    });
  }
}
