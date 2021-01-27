# Cube.js Docs

This repository contains the [Gatsby][link-gatsby]-powered Cube.js
Documentation: [cube.dev/docs][link-docs-live]

Docs are Markdown files located in the main Cube.js repository in
[`docs/content`][link-docs-content]. The build process uses the Gatsby CLI to
scan the `docs/content/` folder and generate a static HTML site.

[link-gatsby]: https://www.gatsbyjs.com/
[link-docs-live]: https://cube.dev/docs
[link-docs-content]: https://github.com/cube-js/cube.js/tree/master/docs/content

## Development

To start the project in development mode, run the following:

```bash
yarn dev
```

To build a production-ready version of the site, run the following:

```bash
source .env.production
yarn build --prefix-paths
```

## Formatting

Run the following to format a Markdown file:

```bash
yarn prettier content/<NAME_OF_FILE> --write
```

If the file includes any alerts (`[[info | Note]]`), then wrap the alert with
`<!-- prettier-ignore-start -->` and `<!-- prettier-ignore-end -->` to prevent
Prettier from messing with them.


## Indexing

The search functionality is powered by [DocSearch by Algolia][link-docsearch].
The configuration file can be [found here][link-docsearch-config].

[link-docsearch]: https://docsearch.algolia.com/
[link-docsearch-config]:
  https://github.com/algolia/docsearch-configs/blob/master/configs/cubejs.json

## Deployment

### Staging

[Netlify][link-netlify] is used for staging and pull request previews. The
staging URL is [cubejs-docs-staging.netlify.app][link-docs-staging].

[link-netlify]: https://www.netlify.com/
[link-docs-staging]: https://cubejs-docs-staging.netlify.app

PRs automatically generate [Deploy Previews] with unique URLs that can be found
in the status checks for a PR.

### Production

Deployment is handled via a [GitHub action][link-gh-docs-workflow].

[link-gh-docs-workflow]: /.github/workflows/docs.yml
