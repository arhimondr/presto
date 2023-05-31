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

FROM quay.io/centos/centos:stream8
USER root:root
SHELL ["/bin/bash", "-c"]

RUN echo "fastestmirror=1" >> /etc/dnf/dnf.conf

RUN dnf -y install openssh-clients git wget unzip make uuid hostname vim python3 perf && \
    python3 -m pip install six

RUN git clone https://github.com/brendangregg/FlameGraph /opt/flamegraph

WORKDIR /opt/dependency

# Install Velox dependencies
RUN wget -q 'https://www.antlr.org/download/antlr-4.9.3-complete.jar' -O '/usr/local/lib/antlr-4.9.3-complete.jar' && \
  printf '#!/usr/bin/bash\n\njava -jar /usr/local/lib/antlr-4.9.3-complete.jar "$@"' > '/usr/local/bin/antlr' && \
  chmod +x "/usr/local/bin/antlr";

RUN --mount=type=bind,rw,source="velox/scripts",target="/opt/velox/scripts" \
  set -exu && \
  sed -i 's/\(.*folly.*\)/# \1/g' "/opt/velox/scripts/setup-circleci.sh" && \
  bash "/opt/velox/scripts/setup-circleci.sh"

ENV CC=/opt/rh/gcc-toolset-9/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-9/root/bin/g++
RUN --mount=type=bind,rw,source="velox/scripts",target="/opt/velox/scripts" \
  set -exu && \
  bash "/opt/velox/scripts/setup-adapters.sh"

# Install Prestissimo dependencies
RUN --mount=type=bind,rw,source="scripts",target="/opt/scripts" \
  --mount=type=bind,rw,source="velox/scripts",target="/opt/velox/scripts" \
  set -exu && \
  bash "/opt/scripts/setup-centos.sh"

RUN rm -rf /opt/dependency/* && dnf clean all

#####################################
# END OF INSTALL DEPENDENCIES STAGE #
#####################################
