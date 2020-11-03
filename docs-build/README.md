# Cube.js Docs

This repository contains Gatsby.js powered Cube.js Docs:
[cube.dev/docs](https://cube.dev/docs)

Docs are markdown files located in the main Cube.js repository in the `docs/`
folder: https://github.com/cube-js/cube.js/tree/master/docs

The build process pulls the Cube.js repo and generate docs.

## Deployment

You need to have [AWS Command Line Interface](https://aws.amazon.com/cli/) installed and configured to deploy both staging and production.

Generate your AWS credentials and use the following command to configure the
CLI.

```
$ aws configure
```

Read more on [Configuring AWS CLI here](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html)

### Staging

The staging URL is [http://cubejs-docs-staging.s3-website-us-east-1.amazonaws.com/docs/](http://cubejs-docs-staging.s3-website-us-east-1.amazonaws.com/docs/)

To deploy staging run the following command inside the repository's root folder:

```bash
$ ./deploy-staging.sh
```

### Production

To deploy production run the following command inside the repository's root folder:

```bash
$ ./deploy-production.sh
```
