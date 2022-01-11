# Development

## Prerequisites:

- `rustup`

# Build and run

```bash
cd packages/cubejs-backend-native
yarn run native:build
yarn link
```

In a Cube project with Cube SQL enabled, run:

```bash
yarn link "@cubejs-backend/native"
yarn dev
```
