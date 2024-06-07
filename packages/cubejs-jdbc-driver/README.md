<p align="center"><a href="https://cube.dev"><img src="https://i.imgur.com/zYHXm4o.png" alt="Cube.js" width="300px"></a></p>

[Website](https://cube.dev) • [Docs](https://cube.dev/docs) • [Blog](https://cube.dev/blog) • [Slack](https://slack.cube.dev) • [Twitter](https://twitter.com/the_cube_dev)

[![npm version](https://badge.fury.io/js/%40cubejs-backend%2Fserver.svg)](https://badge.fury.io/js/%40cubejs-backend%2Fserver)
[![GitHub Actions](https://github.com/cube-js/cube.js/workflows/Build/badge.svg)](https://github.com/cube-js/cube.js/actions?query=workflow%3ABuild+branch%3Amaster)

# Cube.js JDBC Database Driver

JDBC driver.

## Support

This package is **community supported** and should be used at your own risk. 

While the Cube Dev team is happy to review and accept future community contributions, we don't have active plans for further development. This includes bug fixes unless they affect different parts of Cube.js. **We're looking for maintainers for this package.** If you'd like to become a maintainer, please contact us in Cube.js Slack. 

## Java installation

### macOS

```sh
brew install openjdk@8
sudo ln -sfn /usr/local/opt/openjdk@8/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-8.jdk
```

If this doesn't work, please run commands from `$ brew info openjdk@8`.

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

Or download it from 

## License

Cube.js JDBC Database Driver is [Apache 2.0 licensed](./LICENSE).
