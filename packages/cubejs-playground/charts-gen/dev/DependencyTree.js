const R = require('ramda');

const indexByName = (packages) => R.indexBy(R.prop('name'), packages);

class DependencyTree {
  constructor(manifest, templatePackages) {
    this.manifest = manifest;
    this.templatePackages = templatePackages;
    this.rootNode = null;
    this.resolved = [];

    this.build(this.getRootNode());

    const diff = R.difference(templatePackages, this.resolved);
    if (diff.length) {
      throw new Error(`The following packages could not be resolved: ${diff.join(', ')}`);
    }
  }

  packages() {
    return this.manifest.packages;
  }

  getRootNode() {
    if (this.rootNode) {
      return this.rootNode;
    }

    const rootPackages = this.packages().filter((pkg) => pkg.installsTo == null);
    const root = rootPackages.find((pkg) => this.templatePackages.includes(pkg.name));

    this.resolved.push(root.name);

    this.rootNode = {
      package: root,
      children: [],
    };

    return this.rootNode;
  }

  packagesInstalledTo(name) {
    return indexByName(this.packages().filter((pkg) => (pkg.installsTo || {})[name]));
  }

  getChildren(pkg) {
    const children = [];

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

  build(node) {
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

module.exports = DependencyTree;
