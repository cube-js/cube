#!/bin/bash

flatc --ts ../../../rust/cubeshared/src/codegen/http_message.fbs --gen-all
mv http_message.ts index.ts
