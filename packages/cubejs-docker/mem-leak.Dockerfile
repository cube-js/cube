#FROM cubejs/cube:latest
FROM cubejs/cube:testing-drivers

# Install jemalloc library required by the native module
#RUN apt-get update && apt-get install -y libjemalloc2 && rm -rf /var/lib/apt/lists/*

COPY packages/cubejs-server-core/dist/src/core/CompilerApi.js /cube/node_modules/@cubejs-backend/server-core/dist/src/core/CompilerApi.js
COPY packages/cubejs-server-core/dist/src/core/server.js /cube/node_modules/@cubejs-backend/server-core/dist/src/core/server.js
COPY packages/cubejs-schema-compiler/dist/src/compiler/DataSchemaCompiler.js /cube/node_modules/@cubejs-backend/schema-compiler/dist/src/compiler/DataSchemaCompiler.js

# Replace default native allocator with jemalloc version
COPY packages/cubejs-backend-native/index-jemalloc.node /cube/node_modules/@cubejs-backend/native/native/index-jemalloc.node
#COPY packages/cubejs-backend-native/index-default.node /cube/node_modules/@cubejs-backend/native/index.node

# Override CMD to run node with --expose-gc flag for memory leak debugging
CMD ["node", "--expose-gc", "/cube/node_modules/.bin/cubejs", "server"]
