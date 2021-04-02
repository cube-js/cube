import R from 'ramda';

export type Package = {
  name: string;
  version: string;
  installsTo: string[] | null;
  receives: Record<string, string>;
};

const indexByName = (packages: Package[]) => R.indexBy(R.prop('name'), packages);

export class DependencyTree {
  protected rootNode: any = null;

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

  public getRootNode() {
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

  protected packagesInstalledTo(name): Record<string, unknown> {
    return indexByName(this.packages().filter((pkg) => (pkg.installsTo || {})[name]));
  }

  protected getChildren(pkg): any[] {
    const children: any[] = [];

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

  protected build(node) {
    if (!node) {
      return;
    }

    (this.getChildren(node.package) || []).forEach((child) => {
      const childNode = {
        package: child,
        children: [],
      };
      node.children.push(childNode);
      this.resolved.push(child.name);
      this.build(childNode);
    });
  }
}
