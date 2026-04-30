#!/bin/bash

flatc --ts ../../../rust/cube/cubeshared/src/codegen/http_message.fbs --gen-all
mv http_message.ts index.ts
