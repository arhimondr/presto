#!/bin/bash

set -ex

SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
SOURCE_DIR=$(readlink -e "${SCRIPT_DIR}/../../..")

docker run \
    --rm \
    -v /etc/passwd:/etc/passwd \
    -v /home/$USER:/home/$USER \
    -u $(id -u ${USER}) \
    --name=prestissimo-environment \
    --mount type=bind,source=${SOURCE_DIR},target=/src \
    --workdir=/src/presto-native-execution \
    prestissimo/environment:latest \
    bash -c 'make release PRESTO_ENABLE_PARQUET=ON PRESTO_ENABLE_S3=ON PRESTO_ENABLE_TESTING=ON'
