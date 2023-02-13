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

FROM bitnami/minideb:latest

ARG BUILD_PACKAGES="build-essential cmake gfortran libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev libffi-dev zlib1g-dev" \
    ECCODES_VER=2.28.0 \
    PYTHON_VER=3.11.2

ENV DEBIAN_FRONTEND="noninteractive" \
    TZ="Etc/UTC" \
    ECCODES_DIR=/opt/eccodes \
    PATH="${PATH}:/opt/eccodes/bin"

WORKDIR /tmp/eccodes

RUN echo "Acquire::Check-Valid-Until \"false\";\nAcquire::Check-Date \"false\";" | cat > /etc/apt/apt.conf.d/10no--check-valid-until \
    && apt-get update -y \
    && apt-get install -y ${BUILD_PACKAGES} curl vim \
    && curl https://www.python.org/ftp/python/${PYTHON_VER}/Python-${PYTHON_VER}.tgz --output Python-${PYTHON_VER}.tar.gz \
    && tar xzf Python-${PYTHON_VER}.tar.gz && cd ./Python-${PYTHON_VER} && ./configure --enable-optimisations && make altinstall && cd .. \
    && curl https://confluence.ecmwf.int/download/attachments/45757960/eccodes-${ECCODES_VER}-Source.tar.gz --output eccodes-${ECCODES_VER}-Source.tar.gz \
    && tar xzf eccodes-${ECCODES_VER}-Source.tar.gz \
    && mkdir build && cd build && cmake -DCMAKE_INSTALL_PREFIX=${ECCODES_DIR} -DENABLE_AEC=OFF ../eccodes-${ECCODES_VER}-Source && make && ctest && make install # \
    && cd / && rm -rf /tmp/eccodes /tmp/csv2bufr \
    && apt-get remove --purge -y ${BUILD_PACKAGES} \
    && apt autoremove -y  \
    && apt-get -q clean \
    && rm -rf /var/lib/apt/lists/*

COPY . /tmp/csv2bufr

RUN cd /tmp/csv2bufr && python3.11 setup.py install
    

WORKDIR /

