#!/bin/bash

flatc --ts ../../../rust/cubestore/cubestore/src/codegen/http_message.fbs
echo "import { flatbuffers } from 'flatbuffers';" > HttpMessage.ts
cat http_message_generated.ts >> HttpMessage.ts
rm http_message_generated.ts
