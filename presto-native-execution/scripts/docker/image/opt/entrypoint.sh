#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eExv -o functrace

DISCOVERY_URI="${DISCOVERY_URI:-"http://127.0.0.1:8080"}"
HTTP_SERVER_PORT="${HTTP_SERVER_PORT:-"8081"}"
NODE_MEMORY_GB="${NODE_MEMORY_GB:-"32"}"

while getopts ':-:' optchar; do
  case "$optchar" in
    -)
      case "$OPTARG" in
        discovery-uri=*) DISCOVERY_URI="${OPTARG#*=}" ;;
        http-server-port=*) HTTP_SERVER_PORT="${OPTARG#*=}" ;;
        node-memory-gb=*) NODE_MEMORY_GB="${OPTARG#*=}" ;;
        *)
          presto_args+=($optchar)
          ;;
      esac
      ;;
    *)
      presto_args+=($optchar)
      ;;
  esac
done

if [[ ! -f "/opt/presto/etc/config.properties" ]]; then
  cat > "/opt/presto/etc/config.properties" << EOF
presto.version=testversion
discovery.uri=${DISCOVERY_URI}
http-server.http.port=${HTTP_SERVER_PORT}
shutdown-onset-sec=1
register-test-functions=true
EOF
fi

if [[ ! -f "/opt/presto/etc/node.properties" ]]; then
  cat > "/opt/presto/etc/node.properties" << EOF
node.environment=testing
node.location=testing-location
node.id=e4901aae-a1c9-4ff7-97a9-5687835ad54c
node.ip=127.0.0.1
node.memory_gb=${NODE_MEMORY_GB}
EOF
fi

if [[ ! -f "/opt/presto/etc/catalog/hive.properties" ]]; then
  cat > "/opt/presto/etc/catalog/hive.properties" << EOF
connector.name=hive
cache.enabled=true
EOF
fi

if [[ ! -f "/opt/presto/etc/catalog/tpchstandard.properties" ]]; then
  cat > "/opt/presto/etc/catalog/tpchstandard.properties" << EOF
connector.name=tpch
EOF
fi

cd "/opt/presto/etc"
exec "/opt/presto/presto_server" --logtostderr=1 --v=1 "${presto_args[@]}"
