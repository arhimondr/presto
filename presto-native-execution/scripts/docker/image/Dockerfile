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

FROM prestissimo/environment:latest

ARG PRESTO_UID=186

SHELL ["/bin/bash", "-c"]

ENV PRESTO_HOME="/opt/presto/"

WORKDIR /opt/presto/

COPY --chown=${PRESTO_UID} opt/ /opt/
COPY --chown=${PRESTO_UID} presto_server /opt/presto/presto_server

RUN set -exu && \
    #---( INFO create a system account (-r) and same name group (-U) with UID and home path at /opt (-d) named presto
    useradd -r -U --uid=${PRESTO_UID} -d /opt presto && \
    chgrp ${PRESTO_UID} /etc/passwd && \
    chown -R ${PRESTO_UID} /opt && \
    chmod -R ug+rw /etc/passwd /opt && \
    chmod ug+x /opt/*.sh /opt/presto/presto_server

USER ${PRESTO_UID}

RUN mkdir /opt/presto/catalog /opt/presto/etc

ENTRYPOINT [ "/opt/entrypoint.sh" ]
