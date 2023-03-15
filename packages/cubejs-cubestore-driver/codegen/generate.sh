#!/bin/bash

flatc --ts ../../../rust/cubestore/cubestore/src/codegen/http_message.fbs --ts-flat-files
mv http_message.ts index.ts
