#!/bin/bash

echo 'Install script for Ubuntu'

RUN apt-get update && \
    apt-get install -y llvm libssl-dev
