<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js JDBC Database Driver

JDBC driver.

## Support

This package is **community supported** and should be used at your own risk.

While the Cube Dev team is happy to review and accept future community contributions, we don't have active plans for
further development. This includes bug fixes unless they affect different parts of Cube.js. **We're looking for
maintainers for this package.** If you'd like to become a maintainer, please contact us in Cube.js Slack.

## Java installation

### macOS

```sh
brew install openjdk
# At the moment of writing, openjdk 22.0.1 is the latest and proven to work on Intel/M1 Mac's
# Follow the brew suggested advice at the end of installation:
# For the system Java wrappers to find this JDK, symlink it with
sudo ln -sfn /usr/local/opt/openjdk/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk.jdk

# Ensure that newly installed jdk is visible
/usr/libexec/java_home -V
# You should see installed jdk among others, something like this:
Matching Java Virtual Machines (3):
    22.0.1 (x86_64) "Homebrew" - "OpenJDK 22.0.1" /usr/local/Cellar/openjdk/22.0.1/libexec/openjdk.jdk/Contents/Home
    1.8.0_40 (x86_64) "Oracle Corporation" - "Java SE 8" /Library/Java/JavaVirtualMachines/jdk1.8.0_40.jdk/Contents/Home

# Set JAVA_HOME environment variable before running yarn in the Cube repo
export JAVA_HOME=`/usr/libexec/java_home -v 22.0.1`
```

**Note:** It's important to set `JAVA_HOME` prior to running `yarn/npm install` in Cube repo as Java Bridge npm package
uses is to locate JAVA and caches it internally. In case you already run package installation you have to rebuild
all native packages or just delete `node_modules` and run `yarn` again.

### Debian, Ubuntu, etc.

```sh
sudo apt install openjdk-8-jdk
```

### Fedora, Oracle Linux, Red Hat Enterprise Linux, etc.

```sh
su -c "yum install java-1.8.0-openjdk"
```

### Windows

If you have Chocolatey packet manager:

```
choco install openjdk
```

## License

Cube.js JDBC Database Driver is [Apache 2.0 licensed](./LICENSE).
