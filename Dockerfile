###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

#FROM ubuntu:latest
FROM wmoim/dim_eccodes_baseimage:2.31.0

ENV DEBIAN_FRONTEND="noninteractive" \
    TZ="Etc/UTC" \
    ECCODES_DIR=/opt/eccodes \
    PATH="${PATH}:/opt/eccodes/bin" \
    BUFR_ORIGINATING_CENTRE=65535 \
    BUFR_ORIGINATING_SUBCENTRE=65535

RUN apt-get update -y \
    && apt-get install -y vim emacs nedit nano git wget

# install csv2bufr templates
RUN mkdir /opt/csv2bufr &&  \
    cd /opt/csv2bufr && \
    wget https://github.com/wmo-im/csv2bufr-templates/archive/refs/tags/v0.1.tar.gz && \
    tar -zxf v0.1.tar.gz --strip-components=1 csv2bufr-templates-0.1/templates

WORKDIR /tmp

COPY . /tmp/csv2bufr

RUN cd /tmp/csv2bufr && python3 setup.py install


