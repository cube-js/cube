# Development

## Prerequisites:

- `rustup`

## Build and run

```bash
cd packages/cubejs-backend-native
# default one, fallback build
yarn run native:build-debug
# or with python
yarn native:build-debug-python
yarn link
```

In a Cube project with Cube SQL enabled, run:

```bash
yarn link "@cubejs-backend/native"
yarn dev
```

## Known Issues

### `SIGKILL`

Occasionally during development on macOS ARM devices, the generated `index.node`
can be corrupted. To fix this, remove the file and rebuild:

```bash
rm -rf index.node && yarn native:build && yarn test:unit
```
