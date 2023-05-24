#!/bin/bash

set -ex

SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
PRESTO_NATIVE_EXECUTION_ROOT_DIR=$(readlink -e "${SCRIPT_DIR}/../..")
IMAGE_DIR="${SCRIPT_DIR}/image"

cp "${PRESTO_NATIVE_EXECUTION_ROOT_DIR}/_build/release/presto_cpp/main/presto_server" "${IMAGE_DIR}/presto_server"

(
    cd ${IMAGE_DIR} && \
    docker build --network=host --tag "prestissimo/image" -f "${IMAGE_DIR}/Dockerfile" .
) || true

rm "${IMAGE_DIR}/presto_server"
