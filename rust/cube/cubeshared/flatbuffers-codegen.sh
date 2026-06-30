#!/bin/sh

cd ./src/codegen || exit 1
flatc --rust http_message.fbs
