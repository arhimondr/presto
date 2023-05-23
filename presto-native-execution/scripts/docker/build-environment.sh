#!/bin/bash

set -ex

SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
PRESTO_NATIVE_EXECUTION_ROOT_DIR=$(readlink -e "${SCRIPT_DIR}/../..")

(
    cd ${PRESTO_NATIVE_EXECUTION_ROOT_DIR} && \
    docker build --network=host --tag "prestissimo/environment" -f "${PRESTO_NATIVE_EXECUTION_ROOT_DIR}/scripts/docker/environment/Dockerfile" .
)