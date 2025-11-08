#!/bin/bash

flatc --ts ../../../rust/cubeshared/src/codegen/http_message.fbs --ts-flat-files
mv http_message.ts index.ts
